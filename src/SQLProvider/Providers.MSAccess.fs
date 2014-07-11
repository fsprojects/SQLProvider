namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Generic
open System.Data
open System.Data.OleDb
open System.IO

open FSharp.Data.Sql
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common

type internal MSAccessProvider() =
    let pkLookup =     new Dictionary<string,string>()
    let tableLookup =  new Dictionary<string,Table>()
    let relationshipLookup = new Dictionary<string,Relationship list * Relationship list>()
    let columnLookup = new Dictionary<string,Column list>()
     
    let mutable typeMappings = []
    let mutable findClrType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"

    let createTypeMappings (con:OleDbConnection) =
        let dt = con.GetSchema("DataTypes")

        let getDbType(providerType:int) =
            let p = new OleDbParameter()
            p.OleDbType <- (Enum.ToObject(typeof<OleDbType>, providerType) :?> OleDbType)
            p.DbType

        let getClrType (input:string) = Type.GetType(input).ToString()

        let mappings =             
            [
                for r in dt.Rows do
                    let clrType = getClrType (string r.["NativeDataType"])
                    let oleDbType = string r.["TypeName"]
                    let providerType = unbox<int> r.["ProviderDbType"]
                    let dbType = getDbType providerType
                    yield { ProviderTypeName = oleDbType; ClrType = clrType; DbType = dbType; ProviderType = providerType; UseReaderResults = false }
            ]

        let clrMappings =
            mappings
            |> List.map (fun m -> m.ClrType, m)
            |> Map.ofList

        let dbMappings = 
            mappings
            |> List.map (fun m -> m.ProviderTypeName, m)
            |> Map.ofList
            
        typeMappings <- mappings
        findClrType <- clrMappings.TryFind
        findDbType <- dbMappings.TryFind 
    
    interface ISqlProvider with
        member __.CreateConnection(connectionString) = upcast new OleDbConnection(connectionString)
        member __.CreateCommand(connection,commandText) = upcast new OleDbCommand(commandText,connection:?>OleDbConnection)
        member __.CreateCommandParameter(name,value,dbType, direction, length) = 
            let p = OleDbParameter(name,value)            
            if dbType.IsSome then p.DbType <- dbType.Value.DbType
            if direction.IsSome then p.Direction <- direction.Value
            if length.IsSome then p.Size <- length.Value
            upcast p
        member __.CreateTypeMappings(con) = createTypeMappings (con:?>OleDbConnection)     
        member __.GetTables(con) =
            if con.State <> ConnectionState.Open then con.Open()
            let con = con:?>OleDbConnection
            let tables = 
                con.GetSchema("Tables").AsEnumerable()
                |> Seq.filter (fun row -> ["TABLE";"VIEW";"LINK"] |> List.exists (fun typ -> typ = row.["TABLE_TYPE"].ToString())) // = "TABLE" || row.["TABLE_TYPE"].ToString() = "VIEW" || row.["TABLE_TYPE"].ToString() = "LINK")  //The text file specification 'A Link Specification' does not exist. You cannot import, export, or link using the specification.                                                                                                                       
                |> Seq.map (fun row -> let table ={ Schema = Path.GetFileNameWithoutExtension(con.DataSource); Name = row.["TABLE_NAME"].ToString() ; Type=row.["TABLE_TYPE"].ToString() }
                                       if tableLookup.ContainsKey table.FullName = false then tableLookup.Add(table.FullName,table)
                                       table)
                |> List.ofSeq
            tables

        member __.GetPrimaryKey(table) = 
            match pkLookup.TryGetValue table.FullName with
            | true, v -> Some v
            | _ -> None
        member __.GetColumns(con,table) = 
            match columnLookup.TryGetValue table.FullName with
            | (true,data) -> data
            | _ -> 
               if con.State <> ConnectionState.Open then con.Open()
               let pks = 
                    (con:?>OleDbConnection).GetSchema("Indexes",[|null;null;null;null;table.Name|]).AsEnumerable()
                    |> Seq.filter (fun idx ->  bool.Parse(idx.["PRIMARY_KEY"].ToString()))
                    |> Seq.map (fun idx -> idx.["COLUMN_NAME"].ToString())
                    |> Seq.toList

               let columns = 
                    (con:?>OleDbConnection).GetSchema("Columns",[|null;null;table.Name;null|]).AsEnumerable()
                    |> Seq.map (fun row -> let dt = row.["DATA_TYPE"].ToString()
                                           match findDbType dt with
                                           |Some(m) ->
                                                 let col = 
                                                    {Column.Name = row.["COLUMN_NAME"].ToString();
                                                     TypeMapping = m
                                                     IsPrimarKey = pks |> List.exists (fun idx -> idx = row.["COLUMN_NAME"].ToString())
                                                     IsNullable = bool.Parse(row.["IS_NULLABLE"].ToString()) }
                                                 col
                                           |_ -> failwith "failed to map datatypes") |> List.ofSeq
              
              // only add to PK lookup if it's a single pk - no support for composite keys yet
               match pks with
               | pk::[] -> pkLookup.Add(table.FullName, pk) 
               | _ -> ()

               columnLookup.Add(table.FullName,columns)
               columns
        member __.GetRelationships(con,table) =
            if con.State <> ConnectionState.Open then con.Open() 
            let rels = 
                (con:?>OleDbConnection).GetOleDbSchemaTable(OleDbSchemaGuid.Foreign_Keys,[|null|]).AsEnumerable()
            let children = rels |> Seq.filter (fun r -> r.["PK_TABLE_NAME"].ToString() = table.Name)
                                |> Seq.map    (fun r -> let pktableName = table.FullName
                                                        let fktableName = sprintf "[%s].[%s]" table.Schema  (r.["FK_TABLE_NAME"].ToString())
                                                        let name = sprintf "FK_%s_%s" (r.["FK_TABLE_NAME"].ToString()) (r.["PK_TABLE_NAME"].ToString()) 
                                                        {Name=name;PrimaryTable = pktableName;PrimaryKey=r.["PK_COLUMN_NAME"].ToString();ForeignTable=fktableName;ForeignKey=r.["FK_COLUMN_NAME"].ToString()})    
                                |> List.ofSeq
            let parents  = rels |> Seq.filter (fun r -> r.["FK_TABLE_NAME"].ToString() = table.Name)
                                |> Seq.map    (fun r -> let pktableName = sprintf "[%s].[%s]" table.Schema  (r.["PK_TABLE_NAME"].ToString())
                                                        let fktableName = table.FullName
                                                        let name = sprintf "FK_%s_%s" (r.["FK_TABLE_NAME"].ToString()) (r.["PK_TABLE_NAME"].ToString()) 
                                                        {Name=name;PrimaryTable = pktableName;PrimaryKey=r.["PK_COLUMN_NAME"].ToString();ForeignTable=fktableName;ForeignKey=r.["FK_COLUMN_NAME"].ToString()})    
                                |> List.ofSeq
            relationshipLookup.Add(table.FullName,(children,parents))
            (children,parents)
        member __.GetSprocs(con) = 
            []

        member this.GetIndividualsQueryText(table,amount) = sprintf "SELECT TOP %i * FROM [%s]" amount table.Name
                                                            
        member this.GetIndividualQueryText(table,column) = sprintf "SELECT * FROM [%s] WHERE [%s] = @id" table.Name column
        
        member this.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns) = 
            let sb = System.Text.StringBuilder()
            let parameters = ResizeArray<_>()
            let (~~) (t:string) = sb.Append t |> ignore
    
            let getTable x =
                match sqlQuery.Aliases.TryFind x with
                | Some(a) -> a
                | None -> baseTable

            let singleEntity = sqlQuery.Aliases.Count = 0
            
            // first build  the select statement, this is easy ...
            let columns = 
                String.Join(",",
                    [|for KeyValue(k,v) in projectionColumns do
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in columnLookup.[(getTable k).FullName] |> List.map(fun c -> c.Name) do
                                if singleEntity then yield sprintf "[%s].[%s] as [%s]" k col col
                                else yield sprintf "[%s].[%s] as [%s_%s]" k col k col
                        else
                            for col in v do 
                                if singleEntity then yield sprintf "[%s].[%s] as [%s]" k col col
                                else yield sprintf "[%s].[%s] as [%s_%s]" k col k col|]) // F# makes this so easy :)
        
            // next up is the filter expressions
            // make this nicer later.. just try and get the damn thing to work properly (well, at all) for now :D
            // NOTE: really need to assign the parameters their correct SQL types
            let param = ref 0
            let nextParam() =
                incr param
                sprintf "@param%i" !param

            let createParam (value:obj) =
                let paramName = nextParam()
                OleDbParameter(paramName,value):> IDataParameter

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
                                     | None ->    [|createParam DBNull.Value|]

                                let prefix = if i>0 then (sprintf " %s " op) else ""
                                let paras = extractData data
                                ~~(sprintf "%s%s" prefix <|
                                    match operator with
                                    | FSharp.Data.Sql.IsNull -> (sprintf "[%s].[%s] IS NULL") alias col 
                                    | FSharp.Data.Sql.NotNull -> (sprintf "[%s].[%s] IS NOT NULL") alias col 
                                    | FSharp.Data.Sql.In ->                                     
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "[%s].[%s] IN (%s)") alias col text
                                    | FSharp.Data.Sql.NotIn ->                                    
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "[%s].[%s] NOT IN (%s)") alias col text 
                                    | _ -> 
                                        parameters.Add paras.[0]
                                        (sprintf "[%s].[%s]%s %s") alias col 
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
            let fromBuilder(numLinks:int) = 
                sqlQuery.Links
                |> List.iter(fun (fromAlias, data, destAlias)  ->
                    let joinType = if data.OuterJoin then "LEFT JOIN " else "INNER JOIN "
                    let destTable = getTable destAlias
                    ~~  (sprintf "%s [%s] as %s on [%s].[%s] = [%s].[%s]"
                        joinType destTable.Name destAlias 
                        (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias)
                        data.ForeignKey  
                        (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias) 
                        data.PrimaryKey)
                    if (numLinks > 0)  then ~~ ")")//append close paren after each JOIN, if necessary
                        

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) -> 
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "[%s].[%s] %s" alias column (if not desc then "DESC" else "")))

            // SELECT
            if sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s%s " (if sqlQuery.Take.IsSome then sprintf "TOP %i " sqlQuery.Take.Value else "")   columns)
            elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
            else  ~~(sprintf "SELECT %s%s " (if sqlQuery.Take.IsSome then sprintf "TOP %i " sqlQuery.Take.Value else "")  columns)
            // FROM
            //add in 'numLinks' open parens, after FROM, closing each after each JOIN statement
            let numLinks = sqlQuery.Links.Length
            ~~(sprintf "FROM %s%s as %s " (new String('(',numLinks)) baseTable.Name baseAlias)
            fromBuilder(numLinks)
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

            let sql = sb.ToString()
            (sql,parameters)
    
        member this.ProcessUpdates(con, entities) =
                failwith "The Access type provider does not currently support CRUD operations."