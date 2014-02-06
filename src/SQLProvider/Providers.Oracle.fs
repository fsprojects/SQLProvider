namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Generic
open System.Data
open System.Reflection
open FSharp.Data.Sql
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common

type internal OracleProvider(resolutionPath, owner) =
    let assemblyNames = 
        [
            "Oracle.ManagedDataAccess.dll"
            "Oracle.DataAccess.dll"
        ]

    let assembly =  
        assemblyNames 
        |> List.pick (fun asm ->
            try 
                let loadedAsm =              
                    Assembly.LoadFrom(
                        if String.IsNullOrEmpty resolutionPath then asm
                        else System.IO.Path.Combine(resolutionPath,asm)
                        ) 
                if loadedAsm <> null
                then Some loadedAsm
                else None
            with e ->
                None)
   
    let connectionType =  (assembly.GetTypes() |> Array.find(fun t -> t.Name = "OracleConnection"))
    let commandType =     (assembly.GetTypes() |> Array.find(fun t -> t.Name = "OracleCommand"))
    let paramterType =    (assembly.GetTypes() |> Array.find(fun t -> t.Name = "OracleParameter"))
    let oracleDbType = (assembly.GetTypes() |> Array.find(fun t -> t.Name = "OracleDbType"))
    let getSchemaMethod = (connectionType.GetMethod("GetSchema",[|typeof<string>; typeof<string[]>|]))

    let mutable clrToEnum : (string -> DbType option)  = fun _ -> failwith "!"
    let mutable sqlToEnum : (string -> DbType option)  = fun _ -> failwith "!"
    let mutable sqlToClr :  (string -> Type option)       = fun _ -> failwith "!"

    let primaryKeyCache = new Dictionary<string, PrimaryKey>()
    let relationshipCache = new Dictionary<string, Relationship list * Relationship list>()
    let columnCache = new Dictionary<string, Column list>()
    let mutable tableCache : Table list = []

    let createTypeMappings (dt:DataTable) =        
        let clr =             
            [for r in dt.Rows -> 
                string r.["TypeName"],  unbox<int> r.["ProviderDbType"], string r.["DataType"]]

        // create map from sql name to clr type, and type to SqlDbType enum
        let sqlToClr', sqlToEnum', clrToEnum' =
            clr
            |> List.choose( fun (tn,providerType,dt) ->
                if String.IsNullOrWhiteSpace dt then None else
                let ty = Type.GetType dt 
                if ty = null 
                then None
                else
                    match Enum.GetName(oracleDbType, providerType) with
                    | "Raw" | "LongRaw" -> Some DbType.Binary
                    | "RefCursor" | "BFile" | "Blob" | "NClob" -> Some DbType.Object
                    | "Byte" -> Some DbType.Byte
                    | "Varchar2" | "NVarchar2" | "Long" | "XmlType" -> Some DbType.String
                    | "Char" | "NChar" -> Some DbType.StringFixedLength
                    | "Date" -> Some DbType.Date
                    | "TimeStamp" | "TimeStampLTZ" | "TimeStampTZ" -> Some DbType.DateTime
                    | "Decimal" -> Some DbType.Decimal
                    | "Double" -> Some DbType.Double
                    | "Int16" -> Some DbType.Int16
                    | "Int32" -> Some DbType.Int32
                    | "Int64" | "IntervalYM" -> Some DbType.Int64
                    | "IntervalDS" -> Some DbType.Time
                    | "Single" -> Some DbType.Single
                    | _ -> None
                    |> Option.map (fun ev -> ((tn,ty),(tn,ev),(ty.FullName,ev))))
            |> fun x ->  
                let fst (x,_,_) = x
                let snd (_,y,_) = y
                let trd (_,_,z) = z
                (Map.ofList (List.map fst x), 
                 Map.ofList (List.map snd x),
                 Map.ofList (List.map trd x))

        // set lookup functions         
        sqlToClr <-  (fun name -> Map.tryFind name sqlToClr')
        sqlToEnum <- (fun name -> Map.tryFind name sqlToEnum' )
        clrToEnum <- (fun name -> Map.tryFind name clrToEnum' )

    let quoteWhiteSpace (str:String) = 
        (if str.Contains(" ") then sprintf "\"%s\"" str else str)
    
    let getSchema name (args:string[]) conn = 
        getSchemaMethod.Invoke(conn,[|name; args|]) :?> DataTable

    let tableFullName (table:Table) = 
        table.Schema + "." + (quoteWhiteSpace table.Name)

    interface ISqlProvider with
        member __.CreateConnection(connectionString) = 
            Activator.CreateInstance(connectionType,[|box connectionString|]) :?> IDbConnection
        member __.CreateCommand(connection,commandText) =  Activator.CreateInstance(commandType,[|box commandText;box connection|]) :?> IDbCommand
        member __.CreateCommandParameter(name,value,dbType) =
            let value = if value = null then (box System.DBNull.Value) else value
            let p = Activator.CreateInstance(paramterType,[|box name;value|]) :?> IDbDataParameter
            if dbType.IsSome then p.DbType <- dbType.Value 
            upcast p
        member __.CreateTypeMappings(con) = 
            getSchema "DataTypes" [||] con
            |> createTypeMappings

            let indexColumns = 
                getSchema "IndexColumns" [|owner|] con
                |> DataTable.map (fun row -> row.[1] :?> string, row.[4] :?> string)
                |> Map.ofList

            getSchema "PrimaryKeys" [|owner|] con
            |> DataTable.cache primaryKeyCache (fun row ->
                let indexName = unbox row.[15]
                let tableName = unbox row.[2]
                match Map.tryFind indexName indexColumns with
                | Some(column) -> 
                    let pk = { Name = unbox row.[1]; Table = tableName; Column = column; IndexName = indexName }
                    Some(tableName, pk)
                | None -> None) |> ignore

        member __.ClrToEnum = clrToEnum
        member __.SqlToEnum = sqlToEnum
        member __.SqlToClr = sqlToClr
        member __.GetTables(con) =
               match tableCache with
               | [] ->
                    let tables = 
                        getSchema "Tables" [|owner|] con
                        |> DataTable.map (fun row -> 
                                              let name = unbox row.[1]
                                              { Schema = unbox row.[0]; Name = name; Type = unbox row.[2] })
                    tableCache <- tables
                    tables
                | a -> a

        member __.GetPrimaryKey(table) = 
            match primaryKeyCache.TryGetValue table.Name with 
            | true, v -> Some v.Column
            | _ -> None
        member __.GetColumns(con,table) = 
            match columnCache.TryGetValue table.FullName  with
            | true, cols -> cols
            | false, _ ->
                let cols = 
                    getSchema "Columns" [|owner; table.Name|] con
                    |> DataTable.choose (fun row -> 
                            let typ = unbox row.[4]
                            let nullable = unbox row.[8] = "Y"
                            let colName =  unbox row.[2]
                            match sqlToClr typ, sqlToEnum typ with
                            | Some(clrTyp), Some(dbType) -> 
                                    { Name = colName; 
                                      ClrType = clrTyp
                                      DbType = dbType 
                                      IsPrimarKey = primaryKeyCache.Values |> Seq.exists (fun x -> x.Table = table.Name && x.Column = colName)
                                      IsNullable = nullable } |> Some
                            | _, _ -> None)
                columnCache.Add(table.FullName, cols)
                cols
        member __.GetRelationships(con,table) =
                match relationshipCache.TryGetValue(table.FullName) with
                | true, rels -> rels
                | false, _ ->
                    let foreignKeyCols = 
                        getSchema "ForeignKeyColumns" [|owner;table.Name|] con
                        |> DataTable.map (fun row -> (row.[1] :?> string, row.[3] :?> string)) 
                        |> Map.ofList
                    let rels = 
                        getSchema "ForeignKeys" [|owner;table.Name|] con
                        |> DataTable.choose (fun row -> 
                            let name = unbox row.[4]
                            let pkName = unbox row.[0]
                            match primaryKeyCache.TryGetValue(table.Name) with
                            | true, pk ->
                                match foreignKeyCols.TryFind name with
                                | Some(fk) ->
                                     { Name = name 
                                       PrimaryTable = Table.CreateFullName(owner,unbox row.[2]) 
                                       PrimaryKey = pk.Column
                                       ForeignTable = Table.CreateFullName(owner,unbox row.[5])
                                       ForeignKey = fk } |> Some
                                | None -> None
                            | false, _ -> None
                        )
                    let children = 
                        rels |> List.map (fun x -> 
                            { Name = x.Name 
                              PrimaryTable = x.ForeignTable
                              PrimaryKey = x.ForeignKey
                              ForeignTable = x.PrimaryTable
                              ForeignKey = x.PrimaryKey }
                        )

                    relationshipCache.Add(table.FullName, (children ,  rels))
                    (children ,  rels)
        
        member __.GetSprocs(con) =
            let objToString (v:obj) : string = 
                if Convert.IsDBNull(v) then null else unbox v
     
            let objToDecimal (v:obj) : decimal = 
                if Convert.IsDBNull(v) then 0M else unbox v

            getSchema "ProcedureParameters" [|owner|] con
            |> DataTable.groupBy (fun row -> 
                    let owner = unbox row.["OWNER"]
                    let (procName, packageName) = (unbox row.["OBJECT_NAME"], objToString row.["PACKAGE_NAME"])
                    let dataType = unbox row.["DATA_TYPE"]
                    let name = 
                            if String.IsNullOrEmpty(packageName)
                            then procName else (owner + "." + packageName + "." + procName)
                    match sqlToEnum dataType, sqlToClr dataType with
                    | Some(dbType), Some(clrType) ->
                        let direction = 
                            match objToString row.["IN_OUT"] with
                            | "IN" -> Direction.In
                            | "OUT" -> Direction.Out
                            | a -> failwith "Direction not supported %s" a
                        let paramName, paramDetails = objToString row.["ARGUMENT_NAME"], (dbType, clrType, direction, int (row.["POSITION"] :?> decimal), int (objToDecimal row.["DATA_LENGTH"]))
                        (name, Some (paramName, (paramDetails)))
                    | _,_ -> (name, None))
            |> Seq.choose (fun (name, parameters) -> 
                if parameters |> Seq.forall (fun x -> x.IsSome)
                then 
                    let inParams, outParams = 
                        parameters
                        |> Seq.map (function
                                     | Some (pName, (dbType, clrType, direction, position, length)) -> 
                                            { Name = pName; ClrType = clrType; DbType = dbType;  Direction = direction; MaxLength = None; Ordinal = position }
                                     | None -> failwith "How the hell did we get here??")
                        |> Seq.toList
                        |> List.partition (fun p -> p.Direction = Direction.In)
                    let retCols = 
                        outParams 
                        |> List.mapi (fun i p -> { Name = (if (String.IsNullOrEmpty p.Name) then "Column_" + (string i) else p.Name); ClrType = p.ClrType; DbType = p.DbType; IsPrimarKey = false; IsNullable = true })
                    Some { FullName = name; Params = inParams; ReturnColumns = retCols }
                else None
            ) |> Seq.toList

        member this.GetIndividualsQueryText(table,amount) = 
            sprintf "select * from ( select * from %s order by 1 desc) where ROWNUM <= %i" (tableFullName table) amount 

        member this.GetIndividualQueryText(table,column) = 
            let tName = tableFullName table
            sprintf "SELECT * FROM %s WHERE %s.%s = :id" tName tName (quoteWhiteSpace column)

        member this.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns) =
            let sb = System.Text.StringBuilder()
            let parameters = ResizeArray<_>()
            let (~~) (t:string) = sb.Append t |> ignore
            
            let getTable x =
                match sqlQuery.Aliases.TryFind x with
                | Some(a) -> a
                | None -> baseTable

            let singleEntity = sqlQuery.Aliases.Count = 0
            // now we can build the sql query that has been simplified by the above expression converter
            // working on the basis that we will alias everything to make my life eaiser
            // first build  the select statment, this is easy ...
            let columns = 
                String.Join(",",
                    [|for KeyValue(k,v) in projectionColumns do
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in columnCache.[tableFullName (getTable k)] |> List.map(fun c -> c.Name) do 
                                if singleEntity then yield sprintf "%s.%s as \"%s\"" k col col
                                else yield sprintf "%s.%s as \"%s.%s\"" k col k col
                        else
                            for col in v do 
                                if singleEntity then yield sprintf "%s.%s as \"%s\"" k (quoteWhiteSpace col) col
                                yield sprintf "%s.%s as \"%s.%s\"" k (quoteWhiteSpace col) k col|]) // F# makes this so easy :)
        
            // next up is the filter expressions
            // NOTE: really need to assign the parameters their correct db types
            let param = ref 0
            let nextParam() =
                incr param
                sprintf ":param%i" !param

            let createParam (value:obj) =
                let paramName = nextParam()
                (this:>ISqlProvider).CreateCommandParameter(paramName,value,None)

            let rec filterBuilder = function 
                | [] -> ()
                | (cond::conds) ->
                    let build op preds (rest:Condition list option) =
                        ~~ "("
                        preds |> List.iteri( fun i (alias,col,operator,data) ->
                                let extractData data = 
                                     match data with
                                     | Some(x) when (box x :? string array) -> 
                                         // in and not in operators pass an array
                                         let strings = box x :?> string array
                                         strings |> Array.map createParam
                                     | Some(x) -> [|createParam (box x)|]
                                     | None ->    [|createParam null|]

                                let prefix = if i>0 then (sprintf " %s " op) else ""
                                let paras = extractData data
                                ~~(sprintf "%s%s" prefix <|
                                    match operator with
                                    | FSharp.Data.Sql.IsNull -> (sprintf "%s.%s IS NULL") alias (quoteWhiteSpace col) 
                                    | FSharp.Data.Sql.NotNull -> (sprintf "%s.%s IS NOT NULL") alias (quoteWhiteSpace col) 
                                    | FSharp.Data.Sql.In ->                                     
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "%s.%s IN (%s)") alias (quoteWhiteSpace col) text
                                    | FSharp.Data.Sql.NotIn ->                                    
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "%s.%s NOT IN (%s)") alias (quoteWhiteSpace col) text 
                                    | _ -> 
                                        parameters.Add paras.[0]
                                        (sprintf "%s.%s %s %s") alias (quoteWhiteSpace col) 
                                         (operator.ToString()) paras.[0].ParameterName)
                        )
                        // there's probably a nicer way to do this
                        let rec aux = function
                            | x::[] when preds.Length > 0 ->
                                ~~ (sprintf " %s " op)
                                filterBuilder [x]
                            | x::[] -> filterBuilder [x]
                            | x::xs when preds.Length > 0 ->
                                ~~ (sprintf " %s " op)
                                filterBuilder [x]
                                ~~ (sprintf " %s " op)
                                aux xs 
                            | x::xs ->
                                filterBuilder [x]
                                ~~ (sprintf " %s " op)
                                aux xs
                            | [] -> ()
                    
                        Option.iter aux rest
                        ~~ ")"
                
                    match cond with
                    | Or(preds,rest) -> build "OR" preds rest
                    | And(preds,rest) ->  build "AND" preds rest 
                
                    filterBuilder conds
                
            // next up is the FROM statement which includes joins .. 
            let fromBuilder() = 
                sqlQuery.Links
                |> Map.iter(fun fromAlias (destList) ->
                    destList
                    |> List.iter(fun (alias,data) ->
                        let joinType = if data.OuterJoin then "LEFT OUTER JOIN " else "INNER JOIN "
                        let destTable = getTable alias
                        ~~  (sprintf "%s %s %s on %s.%s = %s.%s " 
                               joinType (tableFullName destTable) alias 
                               (if data.RelDirection = RelationshipDirection.Parents then fromAlias else alias)
                               data.ForeignKey  
                               (if data.RelDirection = RelationshipDirection.Parents then alias else fromAlias) 
                               data.PrimaryKey)))

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) -> 
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "%s.%s%s" alias (quoteWhiteSpace column) (if not desc then " DESC NULLS LAST" else " ASC NULLS FIRST")))

            // SELECT
            if sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s " columns)
            elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
            else  ~~(sprintf "SELECT %s " columns)
            // FROM
            ~~(sprintf "FROM %s %s " (tableFullName baseTable) baseAlias)         
            fromBuilder()
            // WHERE
            if sqlQuery.Filters.Length > 0 then
                // each filter is effectively the entire contents of each where clause in the linq query,
                // of which there can be many. Simply turn them all into one big AND expression as that is the
                // only logical way to deal with them. 
                let f = [And([],Some sqlQuery.Filters)]
                ~~"WHERE " 
                filterBuilder f
        
            if sqlQuery.Ordering.Length > 0 then
                ~~"ORDER BY "
                orderByBuilder()

            //I think on oracle this will potentially impact the ordering as the row num is generated before any 
            //filters or ordering is applied hance why this produces a nested query. something like 
            //select * from (select ....) where ROWNUM <= 5.
            if sqlQuery.Take.IsSome 
            then 
                let sql = sprintf "select * from (%s) where ROWNUM <= %i" (sb.ToString()) sqlQuery.Take.Value
                (sql, parameters)
            else
                let sql = sb.ToString()
                (sql,parameters)