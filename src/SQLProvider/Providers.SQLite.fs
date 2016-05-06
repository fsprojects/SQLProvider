namespace FSharp.Data.Sql.Providers

open System
open System.IO
open System.Collections.Concurrent
open System.Collections.Generic
open System.Data
open FSharp.Data.Sql
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common

type internal SQLiteProvider(resolutionPath, referencedAssemblies, runtimeAssembly) as this =
    // note we intentionally do not hang onto a connection object at any time,
    // as the type provider will dicate the connection lifecycles
    let pkLookup = Dictionary<string,string>()
    let tableLookup = Dictionary<string,Table>()
    let columnLookup = ConcurrentDictionary<string,ColumnLookup>()
    let relationshipLookup = Dictionary<string,Relationship list * Relationship list>()
    let isMono = Type.GetType ("Mono.Runtime") <> null

    // Dynamically load the SQLite assembly so we don't have a dependency on it in the project
    let assemblyNames =
        [
           (if isMono then "Mono" else "System") + ".Data.SQLite.dll"
           (if isMono then "Mono" else "System") + ".Data.Sqlite.dll"
        ]

    let assembly =
        lazy Reflection.tryLoadAssemblyFrom resolutionPath (Array.append [|runtimeAssembly|] referencedAssemblies) assemblyNames

    let findType f =
        match assembly.Value with
        | Choice1Of2(assembly) -> assembly.GetTypes() |> Array.find f
        | Choice2Of2(paths) ->
           failwithf "Unable to resolve assemblies. One of %s must exist in the paths: %s %s"
                (String.Join(", ", assemblyNames |> List.toArray))
                Environment.NewLine
                (String.Join(Environment.NewLine, paths))

    let connectionType =  (findType (fun t -> t.Name = if isMono then "SqliteConnection" else "SQLiteConnection"))
    let commandType =     (findType (fun t -> t.Name = if isMono then "SqliteCommand" else "SQLiteCommand"))
    let paramterType =    (findType (fun t -> t.Name = if isMono then "SqliteParameter" else "SQLiteParameter"))
    let getSchemaMethod = (connectionType.GetMethod("GetSchema",[|typeof<string>|]))


    let getSchema name conn =
        getSchemaMethod.Invoke(conn,[|name|]) :?> DataTable

    let mutable typeMappings = []
    let mutable findClrType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"

    let createTypeMappings con =
        let dt = getSchema "DataTypes" con

        let mappings =
            [
                for r in dt.Rows do
                    let clrType = string r.["DataType"]
                    let sqlliteType = string r.["TypeName"]
                    let providerType = unbox<int> r.["ProviderDbType"]
                    let dbType = Enum.ToObject(typeof<DbType>, providerType) :?> DbType
                    yield { ProviderTypeName = Some sqlliteType; ClrType = clrType; DbType = dbType; ProviderType = Some providerType; }
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

        let cmd = (this :> ISqlProvider).CreateCommand(con,"")
        cmd.Connection <- con

        let columnNames, values =
            (([],0),entity.ColumnValues)
            ||> Seq.fold(fun (out,i) (k,v) ->
                let name = sprintf "@param%i" i
                let p = (this :> ISqlProvider).CreateCommandParameter(QueryParameter.Create(name,i),v)
                (k,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray
            |> Array.unzip

        sb.Clear() |> ignore
        ~~(sprintf "INSERT INTO %s (%s) VALUES (%s); SELECT last_insert_rowid();"
            entity.Table.FullName
            (String.Join(",",columnNames))
            (String.Join(",",values |> Array.map(fun p -> p.ParameterName))))

        values |> Array.iter (cmd.Parameters.Add >> ignore)
        cmd.CommandText <- sb.ToString()
        cmd

    let createUpdateCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) changedColumns =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = (this :> ISqlProvider).CreateCommand(con,"")
        cmd.Connection <- con
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
                let name = sprintf "@param%i" i
                let p =
                    match entity.GetColumnOption<obj> col with
                    | Some v -> (this :> ISqlProvider).CreateCommandParameter(QueryParameter.Create(name,i),v)
                    | None -> (this :> ISqlProvider).CreateCommandParameter(QueryParameter.Create(name,i),DBNull.Value)
                (col,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray

        let pkParam = (this :> ISqlProvider).CreateCommandParameter(QueryParameter.Create("@pk",0),pkValue)

        ~~(sprintf "UPDATE %s SET %s WHERE %s = @pk;"
            entity.Table.FullName
            (String.Join(",", data |> Array.map(fun (c,p) -> sprintf "%s = %s" c p.ParameterName ) ))
            pk)

        data |> Array.map snd |> Array.iter (cmd.Parameters.Add >> ignore)
        cmd.Parameters.Add pkParam |> ignore
        cmd.CommandText <- sb.ToString()
        cmd

    let createDeleteCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = (this :> ISqlProvider).CreateCommand(con,"")
        cmd.Connection <- con
        sb.Clear() |> ignore
        let pk = pkLookup.[entity.Table.FullName]
        sb.Clear() |> ignore
        let pkValue =
            match entity.GetColumnOption<obj> pk with
            | Some v -> v
            | None -> failwith "Error - you cannot delete an entity that does not have a primary key."
        let p = (this :> ISqlProvider).CreateCommandParameter(QueryParameter.Create("@id",0),pkValue)
        cmd.Parameters.Add(p) |> ignore
        ~~(sprintf "DELETE FROM %s WHERE %s = @id" entity.Table.FullName pk )
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
        member __.CreateConnection(connectionString) =
            //Forces relative paths to be relative to the Runtime assembly
            let basePath =
                if String.IsNullOrEmpty(resolutionPath) || resolutionPath = Path.DirectorySeparatorChar.ToString()
                then runtimeAssembly
                else resolutionPath
            let connectionString = connectionString.Replace(@"." + Path.DirectorySeparatorChar.ToString(), basePath + Path.DirectorySeparatorChar.ToString())
            Activator.CreateInstance(connectionType,[|box connectionString|]) :?> IDbConnection

        member __.CreateCommand(connection,commandText) = Activator.CreateInstance(commandType,[|box commandText;box connection|]) :?> IDbCommand

        member __.CreateCommandParameter(param,value) =
            let p = Activator.CreateInstance(paramterType,[|box param.Name;box value|]) :?> IDbDataParameter
            p.DbType <- param.TypeMapping.DbType
            p.Direction <- param.Direction
            Option.iter (fun l -> p.Size <- l) param.Length
            p

        member __.ExecuteSprocCommand(_,_,_,_) =  raise(NotImplementedException())

        member __.CreateTypeMappings(con) =
            if con.State <> ConnectionState.Open then con.Open()
            createTypeMappings con
            con.Close()

        member __.GetTables(con,_) =
            if con.State <> ConnectionState.Open then con.Open()
            let ret =
                [ for row in (getSchemaMethod.Invoke(con,[|"Tables"|]) :?> DataTable).Rows do
                    let ty = string row.["TABLE_TYPE"]
                    if ty <> "SYSTEM_TABLE" then
                        let table = { Schema = string row.["TABLE_CATALOG"] ; Name = string row.["TABLE_NAME"]; Type=ty }
                        if tableLookup.ContainsKey table.FullName = false then tableLookup.Add(table.FullName,table)
                        yield table ]
            con.Close()
            ret

        member __.GetPrimaryKey(table) =
            match pkLookup.TryGetValue table.FullName with
            | true, v -> Some v
            | _ -> None

        member __.GetColumns(con,table) =
            match columnLookup.TryGetValue table.FullName with
            | (true,data) -> data
            | _ ->
                if con.State <> ConnectionState.Open then con.Open()
                let query = sprintf "pragma table_info(%s)" table.Name
                use com = (this:>ISqlProvider).CreateCommand(con,query)
                use reader = com.ExecuteReader()
                let columns =
                    [ while reader.Read() do
                        let dt = reader.GetString(2).ToLower()
                        let dt = if dt.Contains("(") then dt.Substring(0,dt.IndexOf("(")) else dt
                        match findDbType dt with
                        | Some(m) ->
                            let col =
                                { Column.Name = reader.GetString(1);
                                  TypeMapping = m
                                  IsNullable = not <| reader.GetBoolean(3);
                                  IsPrimaryKey = if reader.GetBoolean(5) then true else false }
                            if col.IsPrimaryKey && pkLookup.ContainsKey table.FullName = false then pkLookup.Add(table.FullName,col.Name)
                            yield (col.Name,col)
                        | _ -> ()]
                    |> Map.ofList
                con.Close()
                columnLookup.GetOrAdd(table.FullName,columns)

        member __.GetRelationships(con,table) =
            match relationshipLookup.TryGetValue(table.FullName) with
            | true,v -> v
            | _ ->
                // SQLite doesn't have great metadata capabilities.
                // while we can use PRGAMA FOREIGN_KEY_LIST, it will only show us
                // relationships in one direction, the only way to get all the relationships
                // is to retrieve all the relationships in the entire database.  This is not ideal for
                // huge schemas, but SQLite is not generally used for that purpose so we should be ok.
                // At least we can perform all the work for all the tables once here
                // and cache the results for successive calls.....
                if con.State <> ConnectionState.Open then con.Open()
                let relData = (getSchemaMethod.Invoke(con,[|"ForeignKeys"|]) :?> DataTable)
                for row in relData.Rows do
                    let pTable =
                        { Schema = string row.["FKEY_TO_CATALOG"]     //I've not seen a schema column populated in SQLite so I'm using catalog instead
                          Name = string row.["FKEY_TO_TABLE"]
                          Type = ""}
                    let fTable =
                        { Schema = string row.["TABLE_CATALOG"]
                          Name = string row.["TABLE_NAME"]
                          Type = ""}

                    if not <| relationshipLookup.ContainsKey pTable.FullName then relationshipLookup.Add(pTable.FullName,([],[]))
                    if not <| relationshipLookup.ContainsKey fTable.FullName then relationshipLookup.Add(fTable.FullName,([],[]))

                    let rel = { Name = string row.["CONSTRAINT_NAME"]; PrimaryTable= pTable.FullName; PrimaryKey=string row.["FKEY_TO_COLUMN"]
                                ForeignTable=fTable.FullName; ForeignKey=string row.["FKEY_FROM_COLUMN"] }

                    let (c,p) = relationshipLookup.[pTable.FullName]
                    relationshipLookup.[pTable.FullName] <- (rel::c,p)
                    let (c,p) = relationshipLookup.[fTable.FullName]
                    relationshipLookup.[fTable.FullName] <- (c,rel::p)
                con.Close()
                match relationshipLookup.TryGetValue table.FullName with
                | true,v -> v
                | _ -> [],[]

        member __.GetSprocs(_) = [] // SQLite does not support stored procedures.
        member __.GetIndividualsQueryText(table,amount) = sprintf "SELECT * FROM %s LIMIT %i;" table.FullName amount
        member __.GetIndividualQueryText(table,column) = sprintf "SELECT * FROM [%s].[%s] WHERE [%s].[%s].[%s] = @id" table.Schema table.Name table.Schema table.Name column

        member this.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns) =
            let sb = System.Text.StringBuilder()
            let parameters = ResizeArray<_>()
            let (~~) (t:string) = sb.Append t |> ignore

            // all tables should be aliased.
            // the LINQ infrastructure will cause this will happen by default if the query includes more than one table
            // if it does not, then we first need to create an alias for the single table
            let getTable x =
                match sqlQuery.Aliases.TryFind x with
                | Some(a) -> a
                | None -> baseTable

            let singleEntity = sqlQuery.Aliases.Count = 0

            // first build  the select statement, this is easy ...
            let columns =
                if projectionColumns |> Seq.isEmpty then "1" else
                String.Join(",",
                    [|for KeyValue(k,v) in projectionColumns do
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in columnLookup.[(getTable k).FullName] |> Seq.map (fun c -> c.Key) do
                                if singleEntity then yield sprintf "[%s].[%s] as '%s'" k col col
                                else yield sprintf "[%s].[%s] as '[%s].[%s]'" k col k col
                        else
                            for col in v do
                                if singleEntity then yield sprintf "[%s].[%s] as '%s'" k col col
                                else yield sprintf "[%s].[%s] as '[%s].[%s]'" k col k col|]) // F# makes this so easy :)

            // next up is the filter expressions
            // NOTE: really need to assign the parameters their correct db types
            let param = ref 0
            let nextParam() =
                incr param
                sprintf "@param%i" !param

            let createParam (value:obj) =
                let paramName = nextParam()
                (this:>ISqlProvider).CreateCommandParameter(QueryParameter.Create(paramName, !param), value)

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
            let fromBuilder() =
                sqlQuery.Links
                |> List.iter(fun (fromAlias, data, destAlias)  ->
                    let joinType = if data.OuterJoin then "LEFT OUTER JOIN " else "INNER JOIN "
                    let destTable = getTable destAlias
                    ~~  (sprintf "%s [%s].[%s] as [%s] on [%s].[%s] = [%s].[%s] "
                            joinType destTable.Schema destTable.Name destAlias
                            (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias)
                            data.ForeignKey
                            (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias)
                            data.PrimaryKey))

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) ->
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "[%s].[%s]%s" alias column (if not desc then "DESC" else "")))

            // SELECT
            if sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s " columns)
            elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
            else  ~~(sprintf "SELECT %s " columns)
            // FROM
            ~~(sprintf "FROM %s as %s " baseTable.FullName baseAlias)
            fromBuilder()
            // WHERE
            if sqlQuery.Filters.Length > 0 then
                // each filter is effectively the entire contents of each where clause in the LINQ  query,
                // of which there can be many. Simply turn them all into one big AND expression as that is the
                // only logical way to deal with them.
                let f = [And([],Some sqlQuery.Filters)]
                ~~"WHERE "
                filterBuilder f

            if sqlQuery.Ordering.Length > 0 then
                ~~"ORDER BY "
                orderByBuilder()

            if sqlQuery.Take.IsSome then
                ~~(sprintf " LIMIT %i;" sqlQuery.Take.Value)

            let sql = sb.ToString()
            (sql,parameters)

        member this.ProcessUpdates(con, entities) =
            let sb = Text.StringBuilder()

            // ensure columns have been loaded
            entities |> List.map(fun e -> e.Table)
                     |> Seq.distinct
                     |> Seq.iter(fun t -> (this :> ISqlProvider).GetColumns(con,t) |> ignore )

            con.Open()

            use scope = Utilities.ensureTransaction()
            try
                // close the connection first otherwise it won't get enlisted into the transaction
                if con.State = ConnectionState.Open then con.Close()
                con.Open()
                // initially supporting update/create/delete of single entities, no hierarchies yet
                entities
                |> List.iter(fun e ->
                    match e._State with
                    | Created ->
                        use cmd = createInsertCommand con sb e
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        let id = cmd.ExecuteScalar()
                        checkKey id e
                        e._State <- Unchanged
                    | Modified fields ->
                        use cmd = createUpdateCommand con sb e fields
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        cmd.ExecuteNonQuery() |> ignore
                        e._State <- Unchanged
                    | Deleted ->
                        use cmd = createDeleteCommand con sb e
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        cmd.ExecuteNonQuery() |> ignore
                        // remove the pk to prevent this attempting to be used again
                        e.SetColumnOptionSilent(pkLookup.[e.Table.FullName], None)
                    | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!")
                scope.Complete()
            finally
                con.Close()

        member this.ProcessUpdatesAsync(con, entities) =
            let sb = Text.StringBuilder()

            // ensure columns have been loaded
            entities |> List.map(fun e -> e.Table)
                     |> Seq.distinct
                     |> Seq.iter(fun t -> (this :> ISqlProvider).GetColumns(con,t) |> ignore )

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
                                use cmd = createInsertCommand con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                let! id = cmd.ExecuteScalarAsync() |> Async.AwaitTask
                                checkKey id e
                                e._State <- Unchanged
                            }
                        | Modified fields ->
                            async {
                                use cmd = createUpdateCommand con sb e fields :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                e._State <- Unchanged
                            }
                        | Deleted ->
                            async {
                                use cmd = createDeleteCommand con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                // remove the pk to prevent this attempting to be used again
                                e.SetColumnOptionSilent(pkLookup.[e.Table.FullName], None)
                            }
                        | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!"

                    do! Utilities.executeOneByOne handleEntity entities
                    scope.Complete()
                finally
                    con.Close()
            }
