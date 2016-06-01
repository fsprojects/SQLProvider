namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Data
open System.Data.Odbc
open FSharp.Data.Sql
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common

type internal OdbcProvider() =
    let pkLookup = Dictionary<string,string>()
    let tableLookup = Dictionary<string,Table>()
    let columnLookup = ConcurrentDictionary<string,ColumnLookup>()

    let mutable typeMappings = []
    let mutable findClrType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"

    let mutable cOpen = '`' //char separator in query for table aliases `alias` or [alias]
    let mutable cClose = '`' 

    let createTypeMappings (con:OdbcConnection) =

        if con.Driver.Contains("sql") then
            cOpen <- '['
            cClose <- ']'

        let dt = con.GetSchema("DataTypes")

        let getDbType(providerType:int) =
            let p = new OdbcParameter()
            p.OdbcType <- (Enum.ToObject(typeof<OdbcType>, providerType) :?> OdbcType)
            p.DbType

        let getClrType (input:string) = Type.GetType(input).ToString()

        let mappings =
            [
                for r in dt.Rows do
                    
                    if not(String.IsNullOrEmpty(r.["DataType"].ToString()) || String.IsNullOrEmpty(r.["TypeName"].ToString()) || String.IsNullOrEmpty(r.["ProviderDbType"].ToString())) then
                        let clrType = getClrType (string r.["DataType"])
                        let oleDbType = string r.["TypeName"]
                        let providerType = unbox<int> r.["ProviderDbType"]
                        let dbType = getDbType providerType
                        yield { ProviderTypeName = Some oleDbType; ClrType = clrType; DbType = dbType; ProviderType = Some providerType; }
                    
                    if r.Table <> null && r.Table.Columns.Contains("DataType")  && r.Table.Columns.Contains("TypeName")  && r.Table.Columns.Contains("ProviderDbType") then
                        let clrType = getClrType (r.Table.Columns.["DataType"].DataType.ToString())
                        let oleDbType = r.Table.Columns.["TypeName"].DataType.ToString()
                        let providerType = unbox<int> r.Table.Columns.["ProviderDbType"].Ordinal
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

        typeMappings <- mappings
        findClrType <- clrMappings.TryFind
        findDbType <- dbMappings.TryFind

    let createInsertCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = new OdbcCommand()
        cmd.Connection <- con :?> OdbcConnection
        let columnNames, values =
            (([],0),entity.ColumnValues)
            ||> Seq.fold(fun (out,i) (key,value) ->
                let name = sprintf "@param%i" i
                let p = OdbcParameter(name,value)
                (key,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray
            |> Array.unzip

        sb.Clear() |> ignore
        ~~(sprintf "INSERT INTO %s (%s) VALUES (%s);"
            entity.Table.Name
            (String.Join(",",columnNames))
            (String.Join(",",values |> Array.map(fun _ -> "?"))))
        cmd.Parameters.AddRange(values)
        cmd.CommandText <- sb.ToString()
        cmd

    let lastInsertId (con:IDbConnection) =
        let cmd = new OdbcCommand()
        cmd.Connection <- con :?> OdbcConnection
        cmd.CommandText <- "SELECT @@IDENTITY AS id;"
        cmd

    let createUpdateCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) changedColumns =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = new OdbcCommand()
        cmd.Connection <- con :?> OdbcConnection
        let pk = pkLookup.[entity.Table.FullName]
        sb.Clear() |> ignore

        if changedColumns |> List.exists ((=)pk) then failwith "Error - you cannot change the primary key of an entity."

        let pkValue =
            match entity.GetColumnOption<obj> pk with
            | Some v -> v
            | None -> failwith "Error - you cannot update an entity that does not have a primary key."

        let data =
            (([],0),changedColumns)
            ||> List.fold(fun (out,i) col ->
                let p =
                    match entity.GetColumnOption<obj> col with
                    | Some v -> OdbcParameter(null,v)
                    | None -> OdbcParameter(null,DBNull.Value)
                (col,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray

        let pkParam = OdbcParameter(null, pkValue)

        ~~(sprintf "UPDATE %s SET %s WHERE %s = ?;"
            entity.Table.Name
            (String.Join(",", data |> Array.map(fun (c,_) -> sprintf "%s = %s" c "?" ) ))
            pk)

        cmd.Parameters.AddRange(data |> Array.map snd)
        cmd.Parameters.Add pkParam |> ignore
        cmd.CommandText <- sb.ToString()
        cmd

    let createDeleteCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = new OdbcCommand()
        cmd.Connection <- con :?> OdbcConnection
        sb.Clear() |> ignore
        let pk = pkLookup.[entity.Table.FullName]
        sb.Clear() |> ignore
        let pkValue =
            match entity.GetColumnOption<obj> pk with
            | Some v -> v
            | None -> failwith "Error - you cannot delete an entity that does not have a primary key."
        cmd.Parameters.AddWithValue("@id",pkValue) |> ignore
        ~~(sprintf "DELETE FROM %s WHERE %s = ?" entity.Table.Name pk )
        cmd.CommandText <- sb.ToString()
        cmd

    let checkKey id (e:SqlEntity) =
        if pkLookup.ContainsKey e.Table.FullName then
            match e.GetColumnOption pkLookup.[e.Table.FullName] with
            | Some(_) -> () // if the primary key exists, do nothing
                            // this is because non-identity columns will have been set
                            // manually and in that case scope_identity would bring back 0 "" or whatever
            | None ->  e.SetColumnSilent(pkLookup.[e.Table.FullName], id)

    interface ISqlProvider with
        member __.CreateConnection(connectionString) = upcast new OdbcConnection(connectionString)
        member __.CreateCommand(connection,commandText) = upcast new OdbcCommand(commandText, connection:?>OdbcConnection)

        member __.CreateCommandParameter(param, value) =
            let p = OdbcParameter()
            p.Value <- value
            p.ParameterName <- param.Name
            p.DbType <- param.TypeMapping.DbType
            p.Direction <- param.Direction
            Option.iter (fun l -> p.Size <- l) param.Length
            upcast p

        member __.ExecuteSprocCommand(_,_,_,_) = ReturnValueType.Unit
        member __.CreateTypeMappings(con) = createTypeMappings (con:?>OdbcConnection)

        member __.GetTables(con,_) =
            let con = con :?> OdbcConnection
            if con.State <> ConnectionState.Open then con.Open()
            let dataTables = con.GetSchema("Tables").Rows |> Seq.cast<DataRow> |> Seq.map (fun i -> i.ItemArray)
            [ for dataTable in dataTables do
                let schema = 
                    if String.IsNullOrEmpty(dataTable.[1].ToString()) then
                        "dbo"
                    else string dataTable.[1]

                let table ={ Schema = schema ; Name = string dataTable.[2] ; Type=(string dataTable.[3]).ToLower() }
                if tableLookup.ContainsKey table.FullName = false then tableLookup.Add(table.FullName,table)
                yield table ]

        member __.GetPrimaryKey(table) =
            match pkLookup.TryGetValue table.FullName with
            | true, v -> Some v
            | _ -> None

        member __.GetColumns(con,table) =
            match columnLookup.TryGetValue table.FullName with
            | (true,data) -> data
            | _ ->
                let con = con :?> OdbcConnection
                if con.State <> ConnectionState.Open then con.Open()
                let primaryKey = con.GetSchema("Indexes", [| null; null; table.Name |]).Rows |> Seq.cast<DataRow> |> Seq.map (fun i -> i.ItemArray) |> Array.ofSeq
                let dataTable = con.GetSchema("Columns", [| null; null; table.Name; null|]).Rows |> Seq.cast<DataRow> |> Seq.map (fun i -> i.ItemArray)
                let columns =
                    [ for i in dataTable do
                        let dt = i.[5] :?> string
                        match findDbType dt with
                        | Some(m) ->
                            let name = i.[3] :?> string
                            let col =
                                { Column.Name = name
                                  TypeMapping = m
                                  IsNullable = let b = i.[17] :?> string in if b = "YES" then true else false
                                  IsPrimaryKey = if primaryKey.Length > 0 && primaryKey.[0].[8] = box name then true else false }
                            if col.IsPrimaryKey && pkLookup.ContainsKey table.FullName = false then pkLookup.Add(table.FullName,col.Name)
                            yield (col.Name,col)
                        | _ -> ()]
                    |> Map.ofList
                columnLookup.GetOrAdd(table.FullName,columns)

        member __.GetRelationships(_,_) = ([],[]) // The ODBC type provider does not currently support GetRelationships operations.
        member __.GetSprocs(_) = []

        member __.GetIndividualsQueryText(table,_) =
            sprintf "SELECT * FROM %c%s%c" cOpen table.Name cClose

        member __.GetIndividualQueryText(table,column) =
            sprintf "SELECT * FROM %c%s%c WHERE %c%s%c.%c%s%c = ?" 
                     cOpen table.Name cClose cOpen table.Name cClose cOpen column cClose

        member __.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns) =
            let sb = System.Text.StringBuilder()
            let parameters = ResizeArray<_>()
            let (~~) (t:string) = sb.Append t |> ignore

            let getTable x =
                match sqlQuery.Aliases.TryFind x with
                | Some(a) -> a
                | None -> baseTable

            let singleEntity = sqlQuery.Aliases.Count = 0

            let selectcolumns =
                if projectionColumns |> Seq.isEmpty then "1" else
                String.Join(",",
                    [|for KeyValue(k,v) in projectionColumns do
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in columnLookup.[(getTable k).FullName] |> Seq.map (fun c -> c.Key) do
                                if singleEntity then yield sprintf "%c%s%c" cOpen col cClose
                                else yield sprintf "%c%s%c.%c%s%c as %c%s_%s%c" cOpen k cClose cOpen col cClose cOpen k col cClose
                        else
                            for col in v do
                                if singleEntity then yield sprintf "%c%s%c" cOpen col cClose
                                else yield sprintf "%c%s%c.%c%s%c as %c%s_%s%c" cOpen k cClose cOpen col cClose cOpen k col cClose |]) // F# makes this so easy :)

            // Create sumBy, minBy, maxBy, ... field columns
            let columns =
                let extracolumns =
                    let fieldNotation(al:alias,col:string) =
                        match String.IsNullOrEmpty(al) with
                        | true -> sprintf "%c%s%c" cOpen col cClose
                        | false -> sprintf "%c%s%c.%c%s%c" cOpen al cClose cOpen col cClose
                    let fieldNotationAlias(al:alias,col:string) =
                        match String.IsNullOrEmpty(al) with
                        | true -> sprintf "%c%s%c" cOpen col cClose
                        | false -> sprintf "%c%s_%s%c" cOpen al col cClose
                    FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation fieldNotationAlias sqlQuery.AggregateOp
                // Currently we support only aggregate or select. selectcolumns + String.Join(",", extracolumns) when groupBy is ready
                match extracolumns with
                | [] -> selectcolumns
                | h::t -> h

            // make this nicer later.. just try and get the damn thing to work properly (well, at all) for now :D
            // NOTE: really need to assign the parameters their correct sql types

            let createParam (value:obj) =
                let paramName = "?"
                OdbcParameter(paramName,value):> IDbDataParameter

            let rec filterBuilder = function
                | [] -> ()
                | (cond::conds) ->
                    let build op preds (rest:Condition list option) =
                        ~~ "("
                        preds |> List.iteri( fun i (alias,col,operator,data) ->
                                let extractData data =
                                     match data with
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
                                    | FSharp.Data.Sql.IsNull -> (sprintf "%c%s%c.%c%s%c IS NULL") cOpen alias cClose cOpen col cClose
                                    | FSharp.Data.Sql.NotNull -> (sprintf "%c%s%c.%c%s%c IS NOT NULL") cOpen alias cClose cOpen col cClose
                                    | FSharp.Data.Sql.In ->
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "%c%s%c.%c%s%c IN (%s)") cOpen alias cClose cOpen col cClose text
                                    | FSharp.Data.Sql.NotIn ->
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "%c%s%c.%c%s%c NOT IN (%s)") cOpen alias cClose cOpen col cClose text
                                    | _ ->
                                        parameters.Add paras.[0]
                                        (sprintf "%c%s%c.%s %s %s") cOpen alias cClose col
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
                    ~~  (sprintf "%s %c%s%c as %c%s%c on %c%s%c.%c%s%c = %c%s%c.%c%s%c "
                            joinType cOpen destTable.Name cClose cOpen destAlias cClose cOpen
                            (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias)
                            cClose cOpen data.ForeignKey cClose cOpen
                            (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias)
                            cClose cOpen data.PrimaryKey cClose))

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) ->
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "%c%s%c.%c%s%c %s" cOpen alias cClose cOpen column cClose (if not desc then "DESC" else "")))

            // Certain ODBC drivers (excel) don't like special characters in aliases, so we need to strip them
            // or else it will fail
            let stripSpecialCharacters (s:string) =
                String(s.ToCharArray() |> Array.filter(fun c -> Char.IsLetterOrDigit c || c = ' ' || c = '_'))

            // SELECT
            if sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s " columns)
            elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
            else  ~~(sprintf "SELECT %s " columns)
            // FROM
            ~~(sprintf "FROM %c%s%c as %c%s%c " cOpen baseTable.Name cClose cOpen (stripSpecialCharacters baseAlias) cClose)
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

            let sql = sb.ToString()
            (sql,parameters)

        member this.ProcessUpdates(con, entities) =
            let sb = Text.StringBuilder()

            // ensure columns have been loaded
            entities |> Seq.map(fun e -> e.Key.Table)
                     |> Seq.distinct
                     |> Seq.iter(fun t -> (this :> ISqlProvider).GetColumns(con,t) |> ignore )

            if entities.Count = 0 then 
                ()
            else

            if con.State <> ConnectionState.Open then con.Open()

            use scope = Utilities.ensureTransaction()
            try
                // close the connection first otherwise it won't get enlisted into the transaction
                if con.State = ConnectionState.Open then con.Close()
                con.Open()
                // initially supporting update/create/delete of single entities, no hierarchies yet
                entities.Keys
                |> Seq.iter(fun e ->
                    match e._State with
                    | Created ->
                        let cmd = createInsertCommand con sb e
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        cmd.ExecuteNonQuery() |> ignore
                        let id = (lastInsertId con).ExecuteScalar()
                        checkKey id e
                        e._State <- Unchanged
                    | Modified fields ->
                        let cmd = createUpdateCommand con sb e fields
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        cmd.ExecuteNonQuery() |> ignore
                        e._State <- Unchanged
                    | Delete ->
                        let cmd = createDeleteCommand con sb e
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        cmd.ExecuteNonQuery() |> ignore
                        // remove the pk to prevent this attempting to be used again
                        e.SetColumnOptionSilent(pkLookup.[e.Table.FullName], None)
                        e._State <- Deleted
                    | Deleted | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!")
                scope.Complete()

            finally
                con.Close()

        member this.ProcessUpdatesAsync(con, entities) =
            let sb = Text.StringBuilder()

            // ensure columns have been loaded
            entities |> Seq.map(fun e -> e.Key.Table)
                     |> Seq.distinct
                     |> Seq.iter(fun t -> (this :> ISqlProvider).GetColumns(con,t) |> ignore )

            if entities.Count = 0 then 
                async { () }
            else

            async {
                use scope = Utilities.ensureTransaction()
                try
                    // close the connection first otherwise it won't get enlisted into the transaction
                    if con.State = ConnectionState.Open then con.Close()

                    do! con.OpenAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
                    // initially supporting update/create/delete of single entities, no hierarchies yet
                    let handleEntity (e: SqlEntity) =
                        match e._State with
                        | Created ->
                            async {
                                let cmd = createInsertCommand con sb e
                                Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                let id = (lastInsertId con).ExecuteScalar()
                                checkKey id e
                                e._State <- Unchanged
                            }
                        | Modified fields ->
                            async {
                                let cmd = createUpdateCommand con sb e fields
                                Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                e._State <- Unchanged
                            }
                        | Delete ->
                            async {
                                let cmd = createDeleteCommand con sb e
                                Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                // remove the pk to prevent this attempting to be used again
                                e.SetColumnOptionSilent(pkLookup.[e.Table.FullName], None)
                            }
                        | Deleted | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!"
                    do! Utilities.executeOneByOne handleEntity (entities.Keys|>Seq.toList)
                    scope.Complete()

                finally
                    con.Close()
            }
