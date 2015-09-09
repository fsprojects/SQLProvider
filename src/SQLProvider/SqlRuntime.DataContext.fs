namespace FSharp.Data.Sql.Runtime

open System
open System.Collections.Generic
open System.Data
open System.Linq

open FSharp.Data.Sql
open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Schema

module internal ProviderBuilder = 
    open FSharp.Data.Sql.Providers

    let createProvider vendor resolutionPath referencedAssemblies runtimeAssembly owner =
        match vendor with                
        | DatabaseProviderTypes.MSSQLSERVER -> MSSqlServerProvider() :> ISqlProvider
        | DatabaseProviderTypes.SQLITE -> SQLiteProvider(resolutionPath, referencedAssemblies, runtimeAssembly) :> ISqlProvider
        | DatabaseProviderTypes.POSTGRESQL -> PostgresqlProvider(resolutionPath, owner, referencedAssemblies) :> ISqlProvider
        | DatabaseProviderTypes.MYSQL -> MySqlProvider(resolutionPath, owner, referencedAssemblies) :> ISqlProvider
        | DatabaseProviderTypes.ORACLE -> OracleProvider(resolutionPath, owner, referencedAssemblies) :> ISqlProvider
        | DatabaseProviderTypes.MSACCESS -> MSAccessProvider() :> ISqlProvider
        | DatabaseProviderTypes.ODBC -> OdbcProvider(resolutionPath) :> ISqlProvider
        | _ -> failwith "Unsupported database provider" 

type public SqlDataContext (typeName,connectionString:string,providerType,resolutionPath, referencedAssemblies, runtimeAssembly, owner, caseSensitivity) =   
    let pendingChanges = HashSet<SqlEntity>()
    static let providerCache = Dictionary<string,ISqlProvider>()
    do
        lock providerCache (fun () ->  
            match providerCache .TryGetValue typeName with
            | true, _ -> ()
            | false,_ -> 
                let prov = ProviderBuilder.createProvider providerType resolutionPath referencedAssemblies runtimeAssembly owner
                use con = prov.CreateConnection(connectionString)
                con.Open()
                // create type mappings and also trigger the table info read so the provider has 
                // the minimum base set of data available
                prov.CreateTypeMappings(con)
                prov.GetTables(con,caseSensitivity) |> ignore
                if (providerType.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close()
                providerCache.Add(typeName,prov))

    interface ISqlDataContext with
        member this.ConnectionString with get() = connectionString
        member this.CreateConnection() = 
            match providerCache.TryGetValue typeName with
            | true,provider -> provider.CreateConnection(connectionString)
            | false, _ -> failwith "fatal error - provider cache was not populated with expected ISqlprovider instance"
        member this.GetPrimaryKeyDefinition(tableName) =
            match providerCache.TryGetValue typeName with
            | true,provider -> 
                use con = provider.CreateConnection(connectionString)
                provider.GetTables(con, caseSensitivity) 
                |> List.tryFind (fun t -> t.Name = tableName)
                |> Option.bind (fun t -> provider.GetPrimaryKey(t))
                |> (fun x -> defaultArg x "")
            | false, _ -> failwith "fatal error - provider cache was not populated with expected ISqlprovider instance"
        member this.SubmitChangedEntity e = pendingChanges.Add e |> ignore
        member this.ClearPendingChanges() = pendingChanges.Clear()
        member this.GetPendingEntities() = pendingChanges |> Seq.toList
        member this.SubmitPendingChanges() = 
            match providerCache.TryGetValue typeName with
            | true,provider -> 
                use con = provider.CreateConnection(connectionString)
                provider.ProcessUpdates(con, Seq.toList pendingChanges)
                pendingChanges.Clear()
            | false, _ -> failwith "fatal error - provider cache was not populated with expected ISqlprovider instance"
            
        member this.CreateRelated(inst:SqlEntity,entity,pe,pk,fe,fk,direction) : IQueryable<SqlEntity> =
            match providerCache.TryGetValue typeName with
            | true,provider -> 
               if direction = RelationshipDirection.Children then
                   QueryImplementation.SqlQueryable<_>(this,provider,
                      FilterClause(
                         Condition.And(["__base__",fk,ConditionOperator.Equal, Some(inst.GetColumn pk)],None), 
                            BaseTable("__base__",Table.FromFullName fe)),ResizeArray<_>()) :> IQueryable<_> 
               else
                   QueryImplementation.SqlQueryable<_>(this,provider,
                      FilterClause(
                         Condition.And(["__base__",pk,ConditionOperator.Equal, Some(box<|inst.GetColumn fk)],None), 
                            BaseTable("__base__",Table.FromFullName pe)),ResizeArray<_>()) :> IQueryable<_> 
             | false, _ -> failwith "fatal error - provider cache was not populated with expected ISqlprovider instance"
        member this.CreateEntities(table:string) : IQueryable<SqlEntity> =  
            match providerCache.TryGetValue typeName with
            | true,provider -> QueryImplementation.SqlQueryable.Create(Table.FromFullName table,this,provider) 
            | false, _ -> failwith "fatal error - provider cache was not populated with expected ISqlprovider instance"
        member this.CallSproc(definition:SprocDefinition, retCols:QueryParameter[], values:obj array) =
            match providerCache.TryGetValue typeName with
            | true,provider -> 
               use con = provider.CreateConnection(connectionString)
               con.Open()
               use com = provider.CreateCommand(con, definition.Name.DbName)
               com.CommandType <- CommandType.StoredProcedure
               
               let entity = new SqlEntity(this, definition.Name.DbName)

               let toEntityArray rowSet = 
                   [|
                       for row in rowSet do
                           let entity = new SqlEntity(this, definition.Name.DbName)
                           entity.SetData(row)
                           yield entity
                   |]

               let entities =
                   match provider.ExecuteSprocCommand(com, definition,retCols, values) with
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
            | false, _ -> failwith "fatal error - provider cache was not populated with expected ISqlprovider instance"
        member this.GetIndividual(table,id) : SqlEntity =
            match providerCache.TryGetValue typeName with
            | true,provider -> 
               use con = provider.CreateConnection(connectionString)
               con.Open()
               let table = Table.FromFullName table
               // this line is to ensure the columns for the table have been retrieved and therefore
               // its primary key exists in the lookup
               lock provider (fun () -> provider.GetColumns (con,table) |> ignore)
               let pk = 
                   match provider.GetPrimaryKey table with
                   | Some v -> v
                   | None -> 
                      // this fail case should not really be possible unless the runtime database is different to the design-time one
                      failwithf "Primary key could not be found on object %s. Individuals only supported on objects with a single primary key." table.FullName         
        
               use com = provider.CreateCommand(con,provider.GetIndividualQueryText(table,pk))
               //todo: establish pk SQL data type
               com.Parameters.Add (provider.CreateCommandParameter(QueryParameter.Create("@id", 0),id)) |> ignore
               if con.State <> ConnectionState.Open then con.Open()
               use reader = com.ExecuteReader()
               let entity = SqlEntity.FromDataReader(this,table.FullName,reader).[0]
               if (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close()
               entity
            | false, _ -> failwith "fatal error - connection cache was not populated with expected connection details"
    
        
