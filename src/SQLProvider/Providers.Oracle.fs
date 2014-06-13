namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Generic
open System.Data
open System.Reflection
open FSharp.Data.Sql
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common

module internal OracleHelpers = 
    let mutable resolutionPath = String.Empty
    let mutable owner = String.Empty

    let assemblyNames = 
        [
            "Oracle.ManagedDataAccess.dll"
            "Oracle.DataAccess.dll"
        ]

    let assembly =
        (fun () ->   
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
                None))
   
    let connectionType() =  (assembly().GetTypes() |> Array.find(fun t -> t.Name = "OracleConnection"))
    let commandType() =     (assembly().GetTypes() |> Array.find(fun t -> t.Name = "OracleCommand"))
    let paramterType() =    (assembly().GetTypes() |> Array.find(fun t -> t.Name = "OracleParameter"))
    let oracleDbType() = (assembly().GetTypes() |> Array.find(fun t -> t.Name = "OracleDbType"))
    let getSchemaMethod() = (connectionType().GetMethod("GetSchema",[|typeof<string>; typeof<string[]>|]))

    let mutable clrToEnum : (string -> DbType option)  = fun _ -> failwith "!"
    let mutable sqlToEnum : (string -> DbType option)  = fun _ -> failwith "!"
    let mutable sqlToClr :  (string -> Type option)       = fun _ -> failwith "!"

    let getSchema name (args:string[]) conn = 
        getSchemaMethod().Invoke(conn,[|name; args|]) :?> DataTable

    let createTypeMappings con =
        let dt = getSchema "DataTypes" [||] con       
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
                    match Enum.GetName(oracleDbType(), providerType) with
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
    
    let tableFullName (table:Table) = 
        table.Schema + "." + (quoteWhiteSpace table.Name)

    let dbUnbox<'a> (v:obj) : 'a = 
        if Convert.IsDBNull(v) then Unchecked.defaultof<'a> else unbox v
    
    let createConnection connectionString = 
        Activator.CreateInstance(connectionType(),[|box connectionString|]) :?> IDbConnection

    let connect (con:IDbConnection) f =
        if con.State <> ConnectionState.Open then con.Open()
        let result = f con
        con.Close(); result
    
    let getTables con = 
        getSchema "Tables" [|owner|] con
        |> DataTable.map (fun row -> 
                              let name = dbUnbox row.[1]
                              { Schema = dbUnbox row.[0]; Name = name; Type = dbUnbox row.[2] })

    let getColumns (primaryKeys:IDictionary<_,_>) table con = 
        getSchema "Columns" [|owner; table|] con
        |> DataTable.choose (fun row -> 
                let typ = dbUnbox row.[4]
                let nullable = (dbUnbox row.[8]) = "Y"
                let colName =  (dbUnbox row.[2])
                match sqlToClr typ, sqlToEnum typ with
                | Some(clrTyp), Some(dbType) -> 
                        { Name = colName; 
                          ClrType = clrTyp
                          DbType = dbType 
                          IsPrimarKey = primaryKeys.Values |> Seq.exists (fun x -> x.Table = table && x.Column = colName)
                          IsNullable = nullable } |> Some
                | _, _ -> None)

    let getRelationships (primaryKeys:IDictionary<_,_>) table con =
        let foreignKeyCols = 
            getSchema "ForeignKeyColumns" [|owner;table|] con
            |> DataTable.map (fun row -> (dbUnbox row.[1], dbUnbox row.[3])) 
            |> Map.ofList
        let rels = 
            getSchema "ForeignKeys" [|owner;table|] con
            |> DataTable.choose (fun row -> 
                let name = dbUnbox row.[4]
                let pkName = dbUnbox row.[0]
                match primaryKeys.TryGetValue(table) with
                | true, pk ->
                    match foreignKeyCols.TryFind name with
                    | Some(fk) ->
                         { Name = name 
                           PrimaryTable = Table.CreateFullName(owner,dbUnbox row.[2]) 
                           PrimaryKey = pk.Column
                           ForeignTable = Table.CreateFullName(owner,dbUnbox row.[5])
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
        (children, rels)

    let getSprocs con =
        getSchema "ProcedureParameters" [|owner|] con
        |> DataTable.groupBy (fun row -> 
                let owner = dbUnbox row.["OWNER"]
                let (procName, packageName) = (dbUnbox row.["OBJECT_NAME"], dbUnbox row.["PACKAGE_NAME"])
                let dataType = dbUnbox row.["DATA_TYPE"]
                let name = 
                        if String.IsNullOrEmpty(packageName)
                        then (procName) 
                        else (packageName + " " + procName)
                let dbName =
                    if String.IsNullOrEmpty(packageName)
                    then (owner + "." + procName) 
                    else (owner + "." + packageName + "." + procName)
                let argumentName = dbUnbox row.["ARGUMENT_NAME"]
                match sqlToEnum dataType, sqlToClr dataType with
                | Some(dbType), Some(clrType) ->
                    let direction = 
                        match dbUnbox row.["IN_OUT"] with
                        | "IN" -> ParameterDirection.Input
                        | "OUT" when String.IsNullOrEmpty(argumentName) -> ParameterDirection.ReturnValue
                        | "OUT" -> ParameterDirection.Output
                        | a -> failwith "Direction not supported %s" a
                    let paramName, paramDetails = argumentName, (dbType, clrType, direction, dbUnbox<decimal> row.["POSITION"], dbUnbox<decimal> row.["DATA_LENGTH"])
                    ((name, dbName), Some (paramName, paramDetails))
                | _,_ -> ((name, dbName), None))
        |> Seq.choose (fun ((name, dbName), parameters) -> 
            if parameters |> Seq.forall (fun x -> x.IsSome)
            then 
                let sparams = 
                    parameters
                    |> Seq.map (function
                                 | Some (pName, (dbType, clrType, direction, position, length)) -> 
                                        { Name = pName; ClrType = clrType; DbType = dbType;  Direction = direction; MaxLength = None; Ordinal = int(position) }
                                 | None -> failwith "How the hell did we get here??")
                    |> Seq.toList
                    |> List.sortBy (fun x -> x.Ordinal)
                    
                let retCols = 
                    sparams
                    |> List.filter (fun x -> x.Direction <> ParameterDirection.Input)
                    |> List.mapi (fun i p -> { Name = (if (String.IsNullOrEmpty p.Name) then "Column_" + (string i) else p.Name); ClrType = p.ClrType; DbType = p.DbType; IsPrimarKey = false; IsNullable = true })
                Some { FullName = name; DbName = dbName; Params = sparams; ReturnColumns = retCols }
            else None) 
        |> Seq.toList


type internal OracleProvider(resolutionPath, owner) =
    
    let primaryKeyCache = new Dictionary<string, PrimaryKey>()
    let relationshipCache = new Dictionary<string, Relationship list * Relationship list>()
    let columnCache = new Dictionary<string, Column list>()
    let mutable tableCache : Table list = []

    do
        OracleHelpers.owner <- owner
        OracleHelpers.resolutionPath <- resolutionPath
    
    interface ISqlProvider with
        member __.CreateConnection(connectionString) = OracleHelpers.createConnection connectionString
        member __.CreateCommand(connection,commandText) =  Activator.CreateInstance(OracleHelpers.commandType(),[|box commandText;box connection|]) :?> IDbCommand
        member __.CreateCommandParameter(name, value, dbType, direction, maxlength) =
            let value = if value = null then (box System.DBNull.Value) else value
            let p = Activator.CreateInstance(OracleHelpers.paramterType(),[|box name;value|]) :?> IDbDataParameter
            if dbType.IsSome then p.DbType <- dbType.Value
            if direction.IsSome then p.Direction <- direction.Value 
            match maxlength with
            | Some(length) -> p.Size <- length
            | None ->
                match dbType with
                | Some(DbType.String) -> p.Size <- 32767
                | _ -> ()
            upcast p
        member __.CreateTypeMappings(con) = 
            OracleHelpers.connect con (fun con -> 
                OracleHelpers.createTypeMappings con

                let indexColumns = 
                    OracleHelpers.getSchema "IndexColumns" [|owner|] con
                    |> DataTable.map (fun row -> OracleHelpers.dbUnbox row.[1], OracleHelpers.dbUnbox row.[4])
                    |> Map.ofList

                OracleHelpers.getSchema "PrimaryKeys" [|owner|] con
                |> DataTable.cache primaryKeyCache (fun row ->
                    let indexName = OracleHelpers.dbUnbox row.[15]
                    let tableName = OracleHelpers.dbUnbox row.[2]
                    match Map.tryFind indexName indexColumns with
                    | Some(column) -> 
                        let pk = { Name = unbox row.[1]; Table = tableName; Column = column; IndexName = indexName }
                        Some(tableName, pk)
                    | None -> None) |> ignore
            )

        member __.ClrToEnum = OracleHelpers.clrToEnum
        member __.SqlToEnum = OracleHelpers.sqlToEnum
        member __.SqlToClr = OracleHelpers.sqlToClr
        member __.GetTables(con) =
               match tableCache with
               | [] ->
                    let tables = OracleHelpers.connect con OracleHelpers.getTables
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
                let cols = OracleHelpers.connect con (OracleHelpers.getColumns primaryKeyCache table.Name)
                columnCache.Add(table.FullName, cols)
                cols
        member __.GetRelationships(con,table) =
                match relationshipCache.TryGetValue(table.FullName) with
                | true, rels -> rels
                | false, _ ->
                    let rels = OracleHelpers.connect con (OracleHelpers.getRelationships primaryKeyCache table.Name)
                    relationshipCache.Add(table.FullName, rels)
                    rels
        
        member __.GetSprocs(con) = OracleHelpers.connect con OracleHelpers.getSprocs

        member this.GetIndividualsQueryText(table,amount) = 
            sprintf "select * from ( select * from %s order by 1 desc) where ROWNUM <= %i" (OracleHelpers.tableFullName table) amount 

        member this.GetIndividualQueryText(table,column) = 
            let tName = OracleHelpers.tableFullName table
            sprintf "SELECT * FROM %s WHERE %s.%s = :id" tName tName (OracleHelpers.quoteWhiteSpace column)

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
                            for col in columnCache.[baseTable.FullName] |> List.map(fun c -> c.Name) do 
                                if singleEntity then yield sprintf "%s.%s as \"%s\"" k col col
                                else yield sprintf "%s.%s as \"%s.%s\"" k col k col
                        else
                            for col in v do 
                                if singleEntity then yield sprintf "%s.%s as \"%s\"" k (OracleHelpers.quoteWhiteSpace col) col
                                yield sprintf "%s.%s as \"%s.%s\"" k (OracleHelpers.quoteWhiteSpace col) k col|]) // F# makes this so easy :)
        
            // next up is the filter expressions
            // NOTE: really need to assign the parameters their correct db types
            let param = ref 0
            let nextParam() =
                incr param
                sprintf ":param%i" !param

            let createParam (value:obj) =
                let paramName = nextParam()
                (this:>ISqlProvider).CreateCommandParameter(paramName,value,None,None, None)

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
                                    | FSharp.Data.Sql.IsNull -> (sprintf "%s.%s IS NULL") alias (OracleHelpers.quoteWhiteSpace col) 
                                    | FSharp.Data.Sql.NotNull -> (sprintf "%s.%s IS NOT NULL") alias (OracleHelpers.quoteWhiteSpace col) 
                                    | FSharp.Data.Sql.In ->                                     
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "%s.%s IN (%s)") alias (OracleHelpers.quoteWhiteSpace col) text
                                    | FSharp.Data.Sql.NotIn ->                                    
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "%s.%s NOT IN (%s)") alias (OracleHelpers.quoteWhiteSpace col) text 
                                    | _ -> 
                                        parameters.Add paras.[0]
                                        (sprintf "%s.%s %s %s") alias (OracleHelpers.quoteWhiteSpace col) 
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
                |> List.iter(fun (fromAlias, data, destAlias)  ->
                    let joinType = if data.OuterJoin then "LEFT OUTER JOIN " else "INNER JOIN "
                    let destTable = getTable destAlias
                    ~~  (sprintf "%s %s %s on %s.%s = %s.%s " 
                            joinType (OracleHelpers.tableFullName destTable) destAlias 
                            (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias)
                            data.ForeignKey  
                            (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias) 
                            data.PrimaryKey))

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) -> 
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "%s.%s%s" alias (OracleHelpers.quoteWhiteSpace column) (if not desc then " DESC NULLS LAST" else " ASC NULLS FIRST")))

            // SELECT
            if sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s " columns)
            elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
            else  ~~(sprintf "SELECT %s " columns)
            // FROM
            ~~(sprintf "FROM %s %s " (OracleHelpers.tableFullName baseTable) baseAlias)         
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

        member this.ProcessUpdates(con, entities) =
            let sb = Text.StringBuilder()
            let provider = this :> ISqlProvider
            let (~~) (t:string) = sb.Append t |> ignore

            // ensure columns have been loaded
            entities |> List.map(fun e -> e.Table) 
                     |> Seq.distinct 
                     |> Seq.iter(fun t -> provider.GetColumns(con,t) |> ignore )

            con.Open()

            let createInsertCommand (entity:SqlEntity) =     
                let pk = primaryKeyCache.[entity.Table.Name] 
                let columnNames, values = 
                    (([],0),entity.ColumnValues)
                    ||> Seq.fold(fun (out,i) kvp ->                         
                        let name = sprintf ":param%i" i
                        let p = provider.CreateCommandParameter(name,kvp.Value, None, None, None)
                        (kvp.Key,p)::out,i+1)
                    |> fun (x,_)-> x 
                    |> List.rev
                    |> List.toArray 
                    |> Array.unzip
                
                sb.Clear() |> ignore
                ~~(sprintf "INSERT INTO %s (%s) VALUES (%s)" 
                    (OracleHelpers.tableFullName entity.Table)
                    (String.Join(",",columnNames))
                    (String.Join(",",values |> Array.map(fun p -> p.ParameterName))))
                let cmd = provider.CreateCommand(con, sb.ToString())
                values |> Array.iter (cmd.Parameters.Add >> ignore)
                cmd

            let createUpdateCommand (entity:SqlEntity) changedColumns =
                let pk = primaryKeyCache.[entity.Table.Name] 
                sb.Clear() |> ignore

                if changedColumns |> List.exists ((=)pk.Column) then failwith "Error - you cannot change the primary key of an entity."

                let pkValue = 
                    match entity.GetColumnOption<obj> pk.Column with
                    | Some v -> v
                    | None -> failwith "Error - you cannot update an entity that does not have a primary key."
                
                let columns, parameters = 
                    (([],0),changedColumns)
                    ||> List.fold(fun (out,i) col ->                                                         
                        let name = sprintf ":param%i" i
                        let p = 
                            match entity.GetColumnOption<obj> col with
                            | Some v -> provider.CreateCommandParameter(name,v, None, None, None)
                            | None -> provider.CreateCommandParameter(name, DBNull.Value, None, None, None)
                        (col,p)::out,i+1)
                    |> fun (x,_)-> x 
                    |> List.rev
                    |> List.toArray
                    |> Array.unzip
                
                let pkParam = provider.CreateCommandParameter(":pk", pkValue, None, None, None)

                ~~(sprintf "UPDATE %s SET (%s) = (%s) WHERE %s = :pk" 
                    (OracleHelpers.tableFullName entity.Table)
                    (String.Join(",", columns))
                    (String.Join(",", parameters |> Array.map (fun p -> p.ParameterName)))
                    pk.Column)
                
                let cmd = provider.CreateCommand(con, sb.ToString())
                parameters |> Array.iter (cmd.Parameters.Add >> ignore)
                cmd.Parameters.Add pkParam |> ignore
                cmd
            
            let createDeleteCommand (entity:SqlEntity) =
                let pk = primaryKeyCache.[entity.Table.Name] 
                sb.Clear() |> ignore
                let pkValue = 
                    match entity.GetColumnOption<obj> pk.Column with
                    | Some v -> v
                    | None -> failwith "Error - you cannot delete an entity that does not have a primary key."
                ~~(sprintf "DELETE FROM %s WHERE %s = :id" (OracleHelpers.tableFullName entity.Table) pk.Column )
                let cmd = provider.CreateCommand(con, sb.ToString())
                cmd.CommandType <- CommandType.Text
                cmd.Parameters.Add(provider.CreateCommandParameter("id", pkValue, OracleHelpers.clrToEnum (pkValue.GetType().Name), None, None)) |> ignore
                cmd

            use scope = new Transactions.TransactionScope()
            try                
                if con.State <> ConnectionState.Open then con.Open()         
                // initially supporting update/create/delete of single entities, no hierarchies yet
                entities
                |> List.iter(fun e -> 
                    match e.State with
                    | Created -> 
                        let cmd = createInsertCommand e
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        let id = cmd.ExecuteScalar()
                        match e.GetColumnOption primaryKeyCache.[e.Table.Name].Column with
                        | Some v -> () // if the primary key exists, do nothing
                                       // this is because non-identity columns will have been set 
                                       // manually and in that case scope_identity would bring back 0 "" or whatever
                        | None ->  e.SetColumnSilent(primaryKeyCache.[e.Table.Name].Column, id)
                        e.State <- Unchanged
                    | Modified fields -> 
                        let cmd = createUpdateCommand e fields
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        cmd.ExecuteNonQuery() |> ignore
                        e.State <- Unchanged
                    | Deleted -> 
                        let cmd = createDeleteCommand e
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        cmd.ExecuteNonQuery() |> ignore
                        // remove the pk to prevent this attempting to be used again
                        e.SetColumnOptionSilent(primaryKeyCache.[e.Table.Name].Column, None)
                    | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!")
                scope.Complete()
            finally
                con.Close()