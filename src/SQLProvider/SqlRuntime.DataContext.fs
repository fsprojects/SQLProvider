namespace FSharp.Data.Sql.Runtime

open System.Collections.Generic
open System.Data
open System.Linq

open FSharp.Data.Sql
open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Schema

type public SqlDataContext (typeName,connectionString,providerType,resolutionPath, owner) =   
    let pendingChanges = HashSet<SqlEntity>()
    static let providerCache = Dictionary<string,ISqlProvider>()
    do
        lock providerCache (fun () ->  
            match providerCache .TryGetValue typeName with
            | true, _ -> ()
            | false,_ -> 
                let prov = Utilities.createSqlProvider providerType resolutionPath owner
                use con = prov.CreateConnection(connectionString)
                con.Open()
                // create type mappings and also trigger the table info read so the provider has 
                // the minimum base set of data available
                prov.CreateTypeMappings(con)
                prov.GetTables(con) |> ignore
                #if MSACCESS
                con.Close()
                #endif
                providerCache.Add(typeName,prov))

    interface ISqlDataContext with
        member this.ConnectionString with get() = connectionString
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
        member this.CallSproc(name,parameters,values:obj array) =
            match providerCache.TryGetValue typeName with
            | true,provider -> 
               use con = provider.CreateConnection(connectionString)
               con.Open()
               use com = provider.CreateCommand(con,name)
               com.CommandType <- CommandType.StoredProcedure
               let inputParameters, outputParameters = parameters |> Array.partition (fun (_, _, dir, _) -> dir = ParameterDirection.Input || dir = ParameterDirection.InputOutput)
               
               let outps =
                outputParameters
                |> Array.mapi(fun i (name, dbtype, dir, length) ->
                    let p = provider.CreateCommandParameter(name,null,Some dbtype, Some dir, if length = -1 then None else Some length)
                    com.Parameters.Add p |> ignore; p)

               inputParameters
               |> Array.iteri(fun i (name, dbtype, dir, length) ->
                   let p = provider.CreateCommandParameter(name,values.[i],Some dbtype, Some dir, if length = -1 then None else Some length)
                   com.Parameters.Add p |> ignore)

               use reader = com.ExecuteReader()
               let entities = 
                if outputParameters.Length > 0
                then SqlEntity.FromOutputParameters(this, name, outps)
                else SqlEntity.FromDataReader(this,name,reader)
               #if MSACCESS
               con.Close()
               #endif
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
               com.Parameters.Add (provider.CreateCommandParameter("@id",id,None, None, None)) |> ignore
               if con.State <> ConnectionState.Open then con.Open()
               use reader = com.ExecuteReader()
               let entity = List.head <| SqlEntity.FromDataReader(this,table.FullName,reader)
               #if MSACCESS
               con.Close()
               #endif
               entity
            | false, _ -> failwith "fatal error - connection cache was not populated with expected connection details"
    
        
