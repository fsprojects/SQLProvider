namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Data
open System.Data.OleDb
open System.IO

open FSharp.Data.Sql
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common

type internal MSAccessProvider() =
    let pkLookup = new ConcurrentDictionary<string,string list>()
    let tableLookup = new ConcurrentDictionary<string,Table>()
    let relationshipLookup = new ConcurrentDictionary<string,Relationship list * Relationship list>()
    let columnLookup = new ConcurrentDictionary<string,ColumnLookup>()

    let mutable typeMappings = []
    let mutable findClrType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbTypeByEnum : (int -> TypeMapping option)  = fun _ -> failwith "!"

    let createTypeMappings (con:OleDbConnection) =
        if con.State <> ConnectionState.Open then con.Open()
        let dt = con.GetSchema("DataTypes")

        let getDbType(providerType:int) =
            let p = new OleDbParameter()
            p.OleDbType <- (Enum.ToObject(typeof<OleDbType>, providerType) :?> OleDbType)
            p.DbType

        let getClrType (input:string) = Type.GetType(input).ToString()
        let mappings =
            [
                for r in dt.Rows do
                    let clrType = getClrType (string r.["DataType"])
                    let oleDbType = string r.["NativeDataType"]
                    let providerType = unbox<int> r.["ProviderDbType"]
                    let dbType = getDbType providerType
                    yield { ProviderTypeName = Some oleDbType; ClrType = clrType; DbType = dbType; ProviderType = Some providerType; }
            ]

        let clrMappings =
            mappings
            |> List.map (fun m -> m.ClrType, m)
            |> Map.ofList

        let dbMappings =
            mappings
            |> List.map (fun m -> m.ProviderTypeName.Value, m)
            |> Map.ofList

        let enumMappings =
            mappings
            |> List.map (fun m -> m.ProviderType.Value, m)
            |> Map.ofList

        typeMappings <- mappings
        findClrType <- clrMappings.TryFind
        findDbType <- dbMappings.TryFind
        findDbTypeByEnum <- enumMappings.TryFind

    let createInsertCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore

        let cmd = new OleDbCommand()
        cmd.Connection <- con :?> OleDbConnection

        let columnNames, values =
            (([],0),entity.ColumnValues)
            ||> Seq.fold(fun (out,i) (k,v) ->
                let name = sprintf "@param%i" i
                let p = OleDbParameter(name,v)
                (k,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray
            |> Array.unzip

        sb.Clear() |> ignore
        ~~(sprintf "INSERT INTO [%s] (%s) VALUES (%s)"//; SELECT @@IDENTITY;"
            entity.Table.Name
            (String.Join(",", columnNames))
            (String.Join(",", values |> Array.map(fun p -> p.ParameterName))))
        cmd.Parameters.AddRange(values)
        cmd.CommandText <- sb.ToString()
        cmd

    let createUpdateCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) (changedColumns: string list) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = new OleDbCommand()
        cmd.Connection <- con :?> OleDbConnection
        let pk =
            if not(pkLookup.ContainsKey entity.Table.FullName) then
                failwith("Can't update entity: Table doesn't have a primary key: " + entity.Table.FullName)
            pkLookup.[entity.Table.FullName]
        sb.Clear() |> ignore

        match pk with
        | [x] when changedColumns |> List.exists ((=)x)
            -> failwith "Error - you cannot change the primary key of an entity."
        | _ -> ()

        let pkValues =
            match entity.GetPkColumnOption<obj> pk with
            | [] -> failwith ("Error - you cannot update an entity that does not have a primary key. (" + entity.Table.FullName + ")")
            | v -> v

        let data =
            (([],0),changedColumns)
            ||> List.fold(fun (out,i) col ->
                let name = sprintf "@param%i" i
                let p =
                    match entity.GetColumnOption<obj> col with
                    | Some v -> OleDbParameter(name,v)
                    | None -> OleDbParameter(name,DBNull.Value)
                (col,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray

        match pk with
        | [] -> ()
        | ks -> 
            ~~(sprintf "UPDATE [%s] SET %s WHERE "
                (entity.Table.Name.Replace("\"", ""))
                (String.Join(",", data |> Array.map(fun (c,p) -> sprintf "%s = %s" c p.ParameterName ) )))
            ~~(String.Join(" AND ", ks |> List.mapi(fun i k -> (sprintf "%s = @pk%i" k i))))

        cmd.Parameters.AddRange(data |> Array.map snd)
        pkValues |> List.iteri(fun i pkValue ->
            let pkParam = OleDbParameter(("@pk"+i.ToString()), pkValue)
            cmd.Parameters.Add pkParam |> ignore)

        cmd.CommandText <- sb.ToString()
        cmd

    let createDeleteCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = new OleDbCommand()
        cmd.Connection <- con :?> OleDbConnection
        sb.Clear() |> ignore
        let haspk = pkLookup.ContainsKey(entity.Table.FullName)
        let pk = if haspk then pkLookup.[entity.Table.FullName] else []
        sb.Clear() |> ignore
        let pkValues =
            match entity.GetPkColumnOption<obj> pk with
            | [] -> failwith ("Error - you cannot delete an entity that does not have a primary key. (" + entity.Table.FullName + ")")
            | v -> v

        pkValues |> List.iteri(fun i pkValue ->
            cmd.Parameters.AddWithValue(("@id"+i.ToString()),pkValue) |> ignore)

        match pk with
        | [] -> ()
        | ks -> 
            ~~(sprintf "DELETE FROM [%s] WHERE " (entity.Table.Name.Replace("\"", "")))
            ~~(String.Join(" AND ", ks |> List.mapi(fun i k -> (sprintf "%s = @id%i" k i))))

        cmd.CommandText <- sb.ToString()
        cmd

    interface ISqlProvider with
        member __.GetTableDescription(con,t) = t
        member __.GetColumnDescription(con,t,c) = c + t
        member __.CreateConnection(connectionString) = 
            // Access connections shouldn't ever be closed as that leads to Unspecified Error.
            let con = new OleDbConnection(connectionString)
            upcast con

        member __.CreateCommand(connection,commandText) = upcast new OleDbCommand(commandText,connection:?>OleDbConnection)

        member __.CreateCommandParameter(param, value) =
            let p = OleDbParameter(param.Name,value)
            p.DbType <- param.TypeMapping.DbType
            p.Direction <- param.Direction
            Option.iter (fun l -> p.Size <- l) param.Length
            upcast p

        member __.ExecuteSprocCommand(_,_,_,_) =  raise(NotImplementedException())
        member __.CreateTypeMappings(con) = createTypeMappings (con:?>OleDbConnection)

        member __.GetTables(con,_) =
            if con.State <> ConnectionState.Open then con.Open()
            let con = con:?>OleDbConnection
            let tables =
                con.GetSchema("Tables").AsEnumerable()
                |> Seq.filter (fun row -> ["TABLE";"VIEW";"LINK"] |> List.exists (fun typ -> typ = row.["TABLE_TYPE"].ToString())) // = "TABLE" || row.["TABLE_TYPE"].ToString() = "VIEW" || row.["TABLE_TYPE"].ToString() = "LINK")  //The text file specification 'A Link Specification' does not exist. You cannot import, export, or link using the specification.
                |> Seq.map (fun row -> let table ={ Schema = Path.GetFileNameWithoutExtension(con.DataSource); Name = row.["TABLE_NAME"].ToString() ; Type=row.["TABLE_TYPE"].ToString() }
                                       tableLookup.GetOrAdd(table.FullName,table)
                                       )
                |> List.ofSeq
            tables

        member __.GetPrimaryKey(table) =
            match pkLookup.TryGetValue table.FullName with
            | true, [v] -> Some v
            | _ -> None

        member __.GetColumns(con,table) =
            match columnLookup.TryGetValue table.FullName with
            | (true,data) when data.Count > 0 -> data
            | _ ->
                if con.State <> ConnectionState.Open then con.Open()
                let pks =
                    (con:?>OleDbConnection).GetSchema("Indexes",[|null;null;null;null;table.Name.Replace("\"", "")|]).AsEnumerable()
                    |> Seq.filter (fun idx ->  bool.Parse(idx.["PRIMARY_KEY"].ToString()))
                    |> Seq.map (fun idx -> idx.["COLUMN_NAME"].ToString())
                    |> Seq.toList

                let columns =
                    (con:?>OleDbConnection).GetSchema("Columns",[|null;null;table.Name.Replace("\"", "");null|]).AsEnumerable()
                    |> Seq.map (fun row ->
                        match row.["DATA_TYPE"].ToString() |> findDbType with
                        |Some(m) ->
                            let col =
                                { Column.Name = row.["COLUMN_NAME"].ToString();
                                  TypeMapping = m
                                  IsPrimaryKey = pks |> List.exists (fun idx -> idx = row.["COLUMN_NAME"].ToString())
                                  IsNullable = bool.Parse(row.["IS_NULLABLE"].ToString()) }
                            (col.Name,col)
                        |_ -> failwith "failed to map datatypes")
                    |> Map.ofSeq

                // only add to PK lookup if it's a single pk - no support for composite keys yet
                match pks with
                | [] -> ()
                | c -> 
                    pkLookup.AddOrUpdate(table.FullName, (c |> List.sort), fun key old -> 
                                match pks.Length with 0 -> old | _ -> (c |> List.sort)) |> ignore
                columnLookup.AddOrUpdate(table.FullName, columns, fun x old -> match columns.Count with 0 -> old | x -> columns)


        member __.GetRelationships(con,table) =
          relationshipLookup.GetOrAdd(table.FullName, fun name ->
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
            (children,parents))

        member __.GetSprocs(_) = []
        member __.GetIndividualsQueryText(table,amount) = sprintf "SELECT TOP %i * FROM [%s]" amount table.Name
        member __.GetIndividualQueryText(table,column) = sprintf "SELECT * FROM [%s] WHERE [%s] = @id" table.Name column

        member __.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns) =
            let sb = System.Text.StringBuilder()
            let parameters = ResizeArray<_>()
            let (~~) (t:string) = sb.Append t |> ignore

            let getTable x =
                match sqlQuery.Aliases.TryFind x with
                | Some(a) -> a
                | None -> baseTable

            let singleEntity = sqlQuery.Aliases.Count = 0

            // first build  the select statement, this is easy ...
            let selectcolumns =
                if projectionColumns |> Seq.isEmpty then "1" else
                String.Join(",",
                    [|for KeyValue(k,v) in projectionColumns do
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in columnLookup.[(getTable k).FullName] |> Seq.map (fun c -> c.Key) do
                                if singleEntity then yield sprintf "[%s].[%s] as [%s]" k col col
                                else yield sprintf "[%s].[%s] as [%s_%s]" k col k col
                        else
                            for col in v do
                                if singleEntity then yield sprintf "[%s].[%s] as [%s]" k col col
                                else yield sprintf "[%s].[%s] as [%s_%s]" k col k col|]) // F# makes this so easy :)

            // Create sumBy, minBy, maxBy, ... field columns
            let columns =
                let extracolumns =
                    let fieldNotation(al:alias,col:string) =
                        match String.IsNullOrEmpty(al) with
                        | true -> sprintf "[%s]" col
                        | false -> sprintf "[%s].[%s]" al col
                    let fieldNotationAlias(al:alias,col:string) =
                        match String.IsNullOrEmpty(al) with
                        | true -> sprintf "[%s]" col
                        | false -> sprintf "[%s_%s]" al col

                    match sqlQuery.Grouping with
                    | [] -> FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation fieldNotationAlias sqlQuery.AggregateOp
                    | g  -> 
                        let keys = g |> List.map(fst) |> List.concat |> List.map(fieldNotation)
                        let aggs = g |> List.map(snd) |> List.concat
                        let res2 = FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation fieldNotationAlias aggs |> List.toSeq
                        [String.Join(", ", keys) + (match aggs with [] -> "" | _ -> ", ") + String.Join(", ", res2)] 
                match extracolumns with
                | [] when String.IsNullOrEmpty(selectcolumns) -> "*"
                | [] -> selectcolumns
                | h::t -> h

            // next up is the filter expressions
            // make this nicer later.. just try and get the damn thing to work properly (well, at all) for now :D
            // NOTE: really need to assign the parameters their correct SQL types
            let param = ref 0
            let nextParam() =
                incr param
                sprintf "@param%i" !param

            let createParam (value:obj) =
                let paramName = nextParam()
                let valu = match value with
                           | :? DateTime as dt -> dt.ToOADate() |> box
                           | _           -> value
                OleDbParameter(paramName,valu):> IDbDataParameter
            let rec filterBuilder = function
                | [] -> ()
                | (cond::conds) ->
                    let build op preds (rest:Condition list option) =
                        ~~ "("
                        preds |> List.iteri( fun i (alias,col,operator,data) ->
                                let extractData data =
                                     match data with
                                     | Some(x) when (box x :? System.Linq.IQueryable) -> [||]
                                     | Some(x) when (box x :? obj array) ->
                                         // in and not in operators pass an array
                                         let strings = box x :?> obj array
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
                                    | FSharp.Data.Sql.NestedIn when data.IsSome ->
                                        let innersql, innerpars = data.Value |> box :?> string * IDbDataParameter[]
                                        Array.iter parameters.Add innerpars
                                        (sprintf "[%s].[%s] IN (%s)") alias col innersql
                                    | FSharp.Data.Sql.NotIn ->
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "[%s].[%s] NOT IN (%s)") alias col text
                                    | FSharp.Data.Sql.NestedNotIn when data.IsSome ->
                                        let innersql, innerpars = data.Value |> box :?> string * IDbDataParameter[]
                                        Array.iter parameters.Add innerpars
                                        (sprintf "[%s].[%s] NOT IN (%s)") alias col innersql
                                    | _ ->
                                        let aliasformat = if alias<>"" then (sprintf "[%s].[%s]%s %s") alias col else (sprintf "%s %s %s") col
                                        match data with 
                                        | Some d when (box d :? alias * string) ->
                                            let alias2, col2 = box d :?> (alias * string)
                                            let alias2f = if alias2<>"" then (sprintf "[%s].[%s]") alias2 col2 else col2
                                            aliasformat (operator.ToString()) alias2f
                                        | _ ->
                                            parameters.Add paras.[0]
                                            aliasformat (operator.ToString()) paras.[0].ParameterName
                        ))
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
                    | ConstantTrue -> ~~ " (1=1) "
                    | ConstantFalse -> ~~ " (1=0) "

                    filterBuilder conds

            // next up is the FROM statement which includes joins ..
            let fromBuilder(numLinks:int) =
                sqlQuery.Links
                |> List.iter(fun (fromAlias, data, destAlias)  ->
                    let joinType = if data.OuterJoin then "LEFT JOIN " else "INNER JOIN "
                    let destTable = getTable destAlias
                    ~~  (sprintf "%s [%s] as [%s] on "
                            joinType destTable.Name destAlias)
                    ~~  (String.Join(" AND ", (List.zip data.ForeignKey data.PrimaryKey) |> List.map(fun (foreignKey,primaryKey) ->
                        sprintf "[%s].[%s] = [%s].[%s]"
                            (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias)
                            foreignKey
                            (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias)
                            primaryKey)))
                    if (numLinks > 0)  then ~~ ")")//append close paren after each JOIN, if necessary

            let groupByBuilder() =
                sqlQuery.Grouping |> List.map(fst) |> List.concat
                |> List.iteri(fun i (alias,column) ->
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "[%s].[%s]" alias column))

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

            ~~(sprintf "FROM %s[%s] as [%s] " (new String('(',numLinks)) (baseTable.Name.Replace("\"","")) baseAlias)

            fromBuilder(numLinks)
            // WHERE
            if sqlQuery.Filters.Length > 0 then
                // each filter is effectively the entire contents of each where clause in the linq query,
                // of which there can be many. Simply turn them all into one big AND expression as that is the
                // only logical way to deal with them.
                let f = [And([],Some sqlQuery.Filters)]
                ~~"WHERE "
                filterBuilder f

            // GROUP BY
            if sqlQuery.Grouping.Length > 0 then
                ~~" GROUP BY "
                groupByBuilder()

            if sqlQuery.HavingFilters.Length > 0 then
                let keys = sqlQuery.Grouping |> List.map(fst) |> List.concat

                let f = [And([],Some (sqlQuery.HavingFilters |> CommonTasks.parseHaving keys))]
                ~~" HAVING "
                filterBuilder f

            // ORDER BY
            if sqlQuery.Ordering.Length > 0 then
                ~~"ORDER BY "
                orderByBuilder()

            match sqlQuery.Union with
            | Some(true, suquery) -> ~~(sprintf " UNION ALL %s " suquery)
            | Some(false, suquery) -> ~~(sprintf " UNION %s " suquery)
            | None -> ()

            let sql = sb.ToString()
            (sql,parameters)

        member this.ProcessUpdates(con, entities, transactionOptions) =
            let sb = Text.StringBuilder()

            entities.Keys |> Seq.iter (fun e -> printfn "entity - %A" e.ColumnValues)
            CommonTasks.``ensure columns have been loaded`` (this :> ISqlProvider) con entities

            if entities.Count = 0 then 
                ()
            else

            if con.State = ConnectionState.Closed then con.Open()

            try
                // close the connection first otherwise it won't get enlisted into the transaction
                // ...but if access connection is ever closed, it will start to give unknown errors!
                // if con.State = ConnectionState.Open then con.Close()
                use trnsx = con.BeginTransaction()
                try
                    // initially supporting update/create/delete of single entities, no hierarchies yet
                    entities.Keys
                    |> Seq.iter(fun e ->
                        match e._State with
                        | Created ->
                            let cmd = createInsertCommand con sb e
                            cmd.Transaction <- trnsx :?> OleDbTransaction
                            Common.QueryEvents.PublishSqlQuery cmd.CommandText
                            let id = cmd.ExecuteScalar()
                            CommonTasks.checkKey pkLookup id e
                            e._State <- Unchanged
                        | Modified fields ->
                            let cmd = createUpdateCommand con sb e fields
                            cmd.Transaction <- trnsx :?> OleDbTransaction
                            Common.QueryEvents.PublishSqlQuery cmd.CommandText
                            cmd.ExecuteNonQuery() |> ignore
                            e._State <- Unchanged
                        | Delete ->
                            let cmd = createDeleteCommand con sb e
                            cmd.Transaction <- trnsx :?> OleDbTransaction
                            Common.QueryEvents.PublishSqlQuery cmd.CommandText
                            cmd.ExecuteNonQuery() |> ignore
                            // remove the pk to prevent this attempting to be used again
                            e.SetPkColumnOptionSilent(pkLookup.[e.Table.FullName], None)
                            e._State <- Deleted
                        | Deleted | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!")
                    trnsx.Commit()

                with _ ->
                    trnsx.Rollback()
            finally
                ()
                //con.Close()

        member this.ProcessUpdatesAsync(con, entities, transactionOptions) =
            let sb = Text.StringBuilder()

            entities.Keys |> Seq.iter (fun e -> printfn "entity - %A" e.ColumnValues)
            CommonTasks.``ensure columns have been loaded`` (this :> ISqlProvider) con entities

            if entities.Count = 0 then 
                async { () }
            else

            try
                // close the connection first otherwise it won't get enlisted into the transaction
                // ...but if access connection is ever closed, it will start to give unknown errors!
                // if con.State = ConnectionState.Open then con.Close()
                async {
                    if con.State = ConnectionState.Closed then
                        do! con.OpenAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
                    use trnsx = con.BeginTransaction()
                    try
                        // initially supporting update/create/delete of single entities, no hierarchies yet
                        let handleEntity (e: SqlEntity) =
                            match e._State with
                            | Created ->
                                async {
                                    let cmd = createInsertCommand con sb e
                                    cmd.Transaction <- trnsx :?> OleDbTransaction
                                    Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                    let id = cmd.ExecuteScalarAsync()
                                    CommonTasks.checkKey pkLookup id e
                                    e._State <- Unchanged
                                }
                            | Modified fields ->
                                async {
                                    let cmd = createUpdateCommand con sb e fields
                                    cmd.Transaction <- trnsx :?> OleDbTransaction
                                    Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                    cmd.ExecuteNonQuery() |> ignore
                                    e._State <- Unchanged
                                }
                            | Delete ->
                                async {
                                    let cmd = createDeleteCommand con sb e
                                    cmd.Transaction <- trnsx :?> OleDbTransaction
                                    Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                    cmd.ExecuteNonQuery() |> ignore
                                    // remove the pk to prevent this attempting to be used again
                                    e.SetPkColumnOptionSilent(pkLookup.[e.Table.FullName], None)
                                    e._State <- Deleted
                                }
                            | Deleted | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!"

                        do! Utilities.executeOneByOne handleEntity (entities.Keys|>Seq.toList)
                        trnsx.Commit()

                    with _ ->
                        trnsx.Rollback()
                }
            finally
                //con.Close()
                ()
