namespace FSharp.Data.Sql.Runtime

open System
open System.Collections.Generic
open System.Data
open System.Data.Common
open System.Linq
open FSharp.Data.Sql
open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Schema
open System.Collections.Concurrent

module internal ProviderBuilder =
    open FSharp.Data.Sql.Providers

    let createProvider vendor resolutionPath referencedAssemblies runtimeAssembly owner tableNames contextSchemaPath odbcquote sqliteLibrary =
        match vendor with
        | DatabaseProviderTypes.MSSQLSERVER -> MSSqlServerProvider(contextSchemaPath, tableNames) :> ISqlProvider
        | DatabaseProviderTypes.SQLITE -> SQLiteProvider(resolutionPath, contextSchemaPath, referencedAssemblies, runtimeAssembly, sqliteLibrary) :> ISqlProvider
        | DatabaseProviderTypes.POSTGRESQL -> PostgresqlProvider(resolutionPath, contextSchemaPath, owner, referencedAssemblies) :> ISqlProvider
        | DatabaseProviderTypes.MYSQL -> MySqlProvider(resolutionPath, contextSchemaPath, owner, referencedAssemblies) :> ISqlProvider
        | DatabaseProviderTypes.ORACLE -> OracleProvider(resolutionPath, contextSchemaPath, owner, referencedAssemblies, tableNames) :> ISqlProvider
#if !NETSTANDARD
        | DatabaseProviderTypes.MSACCESS -> MSAccessProvider(contextSchemaPath) :> ISqlProvider
        | DatabaseProviderTypes.ODBC -> OdbcProvider(contextSchemaPath, odbcquote) :> ISqlProvider
#endif
        | DatabaseProviderTypes.FIREBIRD -> FirebirdProvider(resolutionPath, contextSchemaPath, owner, referencedAssemblies, odbcquote) :> ISqlProvider
        | _ -> failwith ("Unsupported database provider: " + vendor.ToString())

type public SqlDataContext (typeName, connectionString:string, providerType, resolutionPath, referencedAssemblies, runtimeAssembly, owner, caseSensitivity, tableNames, contextSchemaPath, odbcquote, sqliteLibrary, transactionOptions, commandTimeout:Option<int>, sqlOperationsInSelect) =
    let pendingChanges = System.Collections.Concurrent.ConcurrentDictionary<SqlEntity, DateTime>()
    static let providerCache = ConcurrentDictionary<string,ISqlProvider>()
    let myLock2 = new Object();

    let provider =
        providerCache.GetOrAdd(typeName,
            fun typeName -> 
                let prov : ISqlProvider = ProviderBuilder.createProvider providerType resolutionPath referencedAssemblies runtimeAssembly owner tableNames contextSchemaPath odbcquote sqliteLibrary
                if not (prov.GetSchemaCache().IsOffline) then
                    use con = prov.CreateConnection(connectionString)
                    con.Open()
                    // create type mappings and also trigger the table info read so the provider has
                    // the minimum base set of data available
                    prov.CreateTypeMappings(con)
                    prov.GetTables(con,caseSensitivity) |> ignore
                    if (providerType <> DatabaseProviderTypes.MSACCESS && providerType.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close()
                prov)

    let initCallSproc (dc:ISqlDataContext) (def:RunTimeSprocDefinition) (values:obj array) (con:IDbConnection) (com:IDbCommand) =
        
        if (providerType <> DatabaseProviderTypes.SQLITE) then 
            com.CommandType <- CommandType.StoredProcedure

        let columns =
            def.Params
            |> List.map (fun p -> p.Name, Column.FromQueryParameter(p))
            |> Map.ofList

        let entity = new SqlEntity(dc, def.Name.DbName, columns)

        let toEntityArray rowSet =
            [|
                for row in rowSet do
                    let entity = new SqlEntity(dc, def.Name.DbName, columns)
                    entity.SetData(row)
                    yield entity
            |]

        let param = def.Params |> List.toArray

        Common.QueryEvents.PublishSqlQuery dc.ConnectionString (sprintf "EXEC %s(%s)" com.CommandText (String.Join(", ", (values |> Seq.map (sprintf "%A"))))) []
        param, entity, toEntityArray

    interface ISqlDataContext with
        member __.ConnectionString with get() = connectionString
        member __.CommandTimeout with get() = commandTimeout
        member __.CreateConnection() = provider.CreateConnection(connectionString)

        member __.GetPrimaryKeyDefinition(tableName) =
            let schemaCache = provider.GetSchemaCache()
            match schemaCache.IsOffline with
            | false ->
                use con = provider.CreateConnection(connectionString)
                provider.GetTables(con, caseSensitivity)
                |> List.tryFind (fun t -> t.Name = tableName)
                |> Option.bind (fun t -> provider.GetPrimaryKey(t))
            | true ->
                schemaCache.Tables.TryGetValue(tableName)
                |> function
                    | true, t -> provider.GetPrimaryKey(t)
                    | false, _ -> None
            |> (fun x -> defaultArg x "")

        member __.SubmitChangedEntity e = pendingChanges.AddOrUpdate(e, DateTime.UtcNow, fun oldE dt -> DateTime.UtcNow) |> ignore
        member __.ClearPendingChanges() = pendingChanges.Clear()
        member __.GetPendingEntities() = pendingChanges.Keys |> Seq.toList

        member __.SubmitPendingChanges() =
            use con = provider.CreateConnection(connectionString)
            lock myLock2 (fun () ->
                provider.ProcessUpdates(con, pendingChanges, transactionOptions, commandTimeout)
                pendingChanges |> Seq.iter(fun e -> if e.Key._State = Unchanged || e.Key._State = Deleted then pendingChanges.TryRemove(e.Key) |> ignore)
            )

        member __.SubmitPendingChangesAsync() =
            async {
                use con = provider.CreateConnection(connectionString) :?> System.Data.Common.DbConnection
                let maxWait = DateTime.Now.AddSeconds(3.)
                while (pendingChanges |> Seq.exists(fun e -> match e.Key._State with Unchanged | Deleted -> true | _ -> false)) && DateTime.Now < maxWait do
                    do! Async.Sleep 150 // we can't let async lock but this helps.
                do! provider.ProcessUpdatesAsync(con, pendingChanges, transactionOptions, commandTimeout)
                pendingChanges |> Seq.iter(fun e -> if e.Key._State = Unchanged || e.Key._State = Deleted then pendingChanges.TryRemove(e.Key) |> ignore)
            }

        member this.CreateRelated(inst:SqlEntity,_,pe,pk,fe,fk,direction) : IQueryable<SqlEntity> =
            let parseKey k = KeyColumn k
            if direction = RelationshipDirection.Children then
                QueryImplementation.SqlQueryable<_>(this,provider,
                    FilterClause(
                        Condition.And(["__base__",(parseKey fk),ConditionOperator.Equal, Some(inst.GetColumn pk)],None),
                        BaseTable("__base__",Table.FromFullName fe)),ResizeArray<_>()) :> IQueryable<_>
            else
                QueryImplementation.SqlQueryable<_>(this,provider,
                    FilterClause(
                        Condition.And(["__base__",(parseKey pk),ConditionOperator.Equal, Some(box<|inst.GetColumn fk)],None),
                        BaseTable("__base__",Table.FromFullName pe)),ResizeArray<_>()) :> IQueryable<_>

        member this.CreateEntities(table:string) : IQueryable<SqlEntity> =
            QueryImplementation.SqlQueryable.Create(Table.FromFullName table,this,provider)

        member this.CallSproc(def:RunTimeSprocDefinition, retCols:QueryParameter[], values:obj array) =
            use con = provider.CreateConnection(connectionString)
            con.Open()
            use com = provider.CreateCommand(con, def.Name.DbName)
            if commandTimeout.IsSome then
                com.CommandTimeout <- commandTimeout.Value
            let param, entity, toEntityArray = initCallSproc (this) def values con com

            let entities =
                match provider.ExecuteSprocCommand(com, param, retCols, values) with
                | Unit -> () |> box
                | Scalar(name, o) -> entity.SetColumnSilent(name, o); entity |> box
                | SingleResultSet(name, rs) -> entity.SetColumnSilent(name, toEntityArray rs); entity |> box
                | Set(rowSet) ->
                    for row in rowSet do
                        match row with
                        | ScalarResultSet(name, o) -> entity.SetColumnSilent(name, o);
                        | ResultSet(name, rs) ->
                            let data = toEntityArray rs
                            entity.SetColumnSilent(name, data)
                    entity |> box

            if (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close()
            entities

        member this.CallSprocAsync(def:RunTimeSprocDefinition, retCols:QueryParameter[], values:obj array) =
            async {
                use con = provider.CreateConnection(connectionString) :?> System.Data.Common.DbConnection
                do! con.OpenAsync() |> Async.AwaitIAsyncResult |> Async.Ignore

                use com = provider.CreateCommand(con, def.Name.DbName)
                if commandTimeout.IsSome then
                    com.CommandTimeout <- commandTimeout.Value
                let param, entity, toEntityArray = initCallSproc (this) def values con com

                let! res = provider.ExecuteSprocCommandAsync((com:?> System.Data.Common.DbCommand), param, retCols, values)
                let entities =
                    match res with
                    | Unit -> Unchecked.defaultof<SqlEntity>
                    | Scalar(name, o) -> entity.SetColumnSilent(name, o); entity
                    | SingleResultSet(name, rs) -> entity.SetColumnSilent(name, toEntityArray rs); entity
                    | Set(rowSet) ->
                        for row in rowSet do
                            match row with
                            | ScalarResultSet(name, o) -> entity.SetColumnSilent(name, o);
                            | ResultSet(name, rs) ->
                                let data = toEntityArray rs
                                entity.SetColumnSilent(name, data)
                        entity

                if (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close()
                return entities
            }

        member this.GetIndividual(table,id) : SqlEntity =
            use con = provider.CreateConnection(connectionString)
            con.Open()
            let table = Table.FromFullName table
            // this line is to ensure the columns for the table have been retrieved and therefore
            // its primary key exists in the lookup
            let columns = provider.GetColumns(con, table)
            let pk =
                match provider.GetPrimaryKey table with
                | Some v -> columns.[v]
                | None ->
                    // this fail case should not really be possible unless the runtime database is different to the design-time one
                    failwithf "Primary key could not be found on object %s. Individuals only supported on objects with a single primary key." table.FullName
            use com = provider.CreateCommand(con,provider.GetIndividualQueryText(table,pk.Name))
            if commandTimeout.IsSome then
                com.CommandTimeout <- commandTimeout.Value
            //todo: establish pk SQL data type
            com.Parameters.Add (provider.CreateCommandParameter(QueryParameter.Create("@id", 0, pk.TypeMapping),id)) |> ignore
            if con.State <> ConnectionState.Open then con.Open()
            use reader = com.ExecuteReader()
            let entity = (this :> ISqlDataContext).ReadEntities(table.FullName, columns, reader) |> Seq.exactlyOne
            if (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close()
            entity

        member this.ReadEntities(name: string, columns: ColumnLookup, reader: IDataReader) =
            [| while reader.Read() = true do
                 let e = SqlEntity(this, name, columns)
                 for i = 0 to reader.FieldCount - 1 do
                    match reader.GetValue(i) with
                    | null | :? DBNull ->  e.SetColumnSilent(reader.GetName(i),null)
                    | value -> e.SetColumnSilent(reader.GetName(i),value)
                 yield e
            |]

        member this.ReadEntitiesAsync(name: string, columns: ColumnLookup, reader: DbDataReader) =
            let collectItemfunc() : SqlEntity =
                    let e = SqlEntity(this, name, columns)
                    for i = 0 to reader.FieldCount - 1 do
                        match reader.GetValue(i) with
                        | null | :? DBNull ->  e.SetColumnSilent(reader.GetName(i),null)
                        | value -> e.SetColumnSilent(reader.GetName(i),value)
                    e

            let rec readitems acc =
                async {
                    let! moreitems = reader.ReadAsync() |> Async.AwaitTask
                    match moreitems with
                    | true ->
                        return! readitems (collectItemfunc()::acc)
                    | false -> return acc
                }

            async {
                let! items = readitems []
                return items |> List.rev |> List.toArray
            }

        member this.CreateEntity(tableName) =
            use con = provider.CreateConnection(connectionString)
            let columns = provider.GetColumns(con, Table.FromFullName(tableName))
            new SqlEntity(this, tableName, columns)

        member __.SqlOperationsInSelect with get() = sqlOperationsInSelect

        member __.SaveContextSchema(filePath) =
            providerCache
            |> Seq.iter (fun prov -> prov.Value.GetSchemaCache().Save(filePath))