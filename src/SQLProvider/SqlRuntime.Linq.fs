namespace FSharp.Data.Sql.Runtime

open System
open System.Collections
open System.Collections.Generic
open System.Data

open FSharp.Data.Sql
open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Common.Utilities
open FSharp.Data.Sql.QueryExpression
open FSharp.Data.Sql.Schema

// this is publically exposed and used in the runtime
type IWithDataContext =
    abstract DataContext : ISqlDataContext

type CastTupleMaker<'T1,'T2,'T3,'T4,'T5,'T6,'T7> = 
    static member makeTuple2(t1:obj, t2:obj) = 
        (t1 :?> 'T1, t2 :?> 'T2) |> box
    static member makeTuple3(t1:obj, t2:obj, t3:obj) = 
        (t1 :?> 'T1, t2 :?> 'T2, t3 :?> 'T3) |> box
    static member makeTuple4(t1:obj, t2:obj, t3:obj, t4:obj) = 
        (t1 :?> 'T1, t2 :?> 'T2, t3 :?> 'T3, t4 :?> 'T4) |> box
    static member makeTuple5(t1:obj, t2:obj, t3:obj, t4:obj, t5:obj) = 
        (t1 :?> 'T1, t2 :?> 'T2, t3 :?> 'T3, t4 :?> 'T4, t5 :?> 'T5) |> box
    static member makeTuple6(t1:obj, t2:obj, t3:obj, t4:obj, t5:obj, t6:obj) = 
        (t1 :?> 'T1, t2 :?> 'T2, t3 :?> 'T3, t4 :?> 'T4, t5 :?> 'T5, t6 :?> 'T6) |> box
    static member makeTuple7(t1:obj, t2:obj, t3:obj, t4:obj, t5:obj, t6:obj, t7:obj) = 
        (t1 :?> 'T1, t2 :?> 'T2, t3 :?> 'T3, t4 :?> 'T4, t5 :?> 'T5, t6 :?> 'T6, t7 :?> 'T7) |> box

module internal QueryImplementation =
    open System.Linq
    open System.Linq.Expressions
    open Patterns

    type IWithSqlService =
        abstract DataContext : ISqlDataContext
        abstract SqlExpression : SqlExp
        abstract TupleIndex : string ResizeArray // indexes where in the anonymous object created by the compiler during a select many that each entity alias appears
        abstract Provider : ISqlProvider
    
    /// Interface for async enumerations as .NET doesn't have it out-of-the-box
    type IAsyncEnumerable<'T> =
        abstract GetAsyncEnumerator : unit -> Async<IEnumerator<'T>>
    type IAsyncEnumerable =
        abstract EvaluateQuery : unit -> Async<unit>

    let (|SourceWithQueryData|_|) = function Constant ((:? IWithSqlService as org), _)    -> Some org | _ -> None
    let (|RelDirection|_|)        = function Constant ((:? RelationshipDirection as s),_) -> Some s   | _ -> None

    let (|OptionalOuterJoin|) e =
        match e with
        | MethodCall(None, (!!), [inner]) -> (true,inner)
        | MethodCall(None,MethodWithName("op_BangBang"), [inner]) -> (true,inner)
        | _ -> (false,e)

    let parseQueryResults (projector:Delegate) (results:SqlEntity[]) =
        let args = projector.GetType().GenericTypeArguments
        seq { 
            if args.Length > 0 && (args.[0].Name.StartsWith("IGrouping") ||
                                      (args.[0].Name.StartsWith("AnonymousObject") && args.[0].GenericTypeArguments.Length > 0 && args.[0].GenericTypeArguments |> Array.exists(fun a -> a.Name.StartsWith("IGrouping")))
                        ) then
                let keyType, keyConstructor, itemEntityType = 
                    if args.[0].Name.StartsWith("IGrouping") then
                        if args.[0].GenericTypeArguments.Length = 0 then None, None, typeof<SqlEntity>
                        else 
                            let kt = args.[0].GenericTypeArguments.[0]
                            let itmType = if args.[0].GenericTypeArguments.Length > 1 then args.[0].GenericTypeArguments.[1] else typeof<SqlEntity>
                            if kt = typeof<obj> then None, None, itmType
                            else
                                let tyo = typedefof<GroupResultItems<_,SqlEntity>>.MakeGenericType(kt, itmType)
                                Some kt, Some (tyo.GetConstructors()), itmType
                    else
                        let baseItmType = typeof<Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<SqlEntity,SqlEntity>>
                        let gen = args.[0].GenericTypeArguments |> Array.find(fun a -> a.Name.StartsWith("IGrouping"))
                        if gen.GenericTypeArguments.Length = 0 then None, None, baseItmType
                        else
                            let kt = gen.GenericTypeArguments.[0]
                            let itmType = if gen.GenericTypeArguments.Length > 1 then gen.GenericTypeArguments.[1] else baseItmType
                            if kt = typeof<obj> then None, None, itmType
                            else
                                let tyo = typedefof<GroupResultItems<_,_>>.MakeGenericType(kt, itmType)
                                Some kt, Some (tyo.GetConstructors()), itmType

                let getval (key:obj) idx = 
                    if keyType.Value.IsGenericType then
                        Utilities.convertTypes key keyType.Value.GenericTypeArguments.[idx]
                    else key
                
                let tup2, tup3, tup4, tup5, tup6, tup7 = 
                    let genArg idx = 
                        if keyType.IsSome && keyType.Value.GenericTypeArguments.Length > idx then
                            keyType.Value.GenericTypeArguments.[idx]
                        else typeof<Object>
                    let tup =
                        typedefof<CastTupleMaker<_,_,_,_,_,_,_>>.MakeGenericType(
                            genArg 0, genArg 1, genArg 2, genArg 3, genArg 4, genArg 5, genArg 6
                        )
                    tup.GetMethod("makeTuple2"), tup.GetMethod("makeTuple3"), tup.GetMethod("makeTuple4"),
                    tup.GetMethod("makeTuple5"), tup.GetMethod("makeTuple6"), tup.GetMethod("makeTuple7")

                // do group-read
                let collected = 
                    results |> Array.map(fun (e:SqlEntity) ->
                        // Alias is '[Sum_Column]'
                        let aggregates = [|"COUNT_"; "MIN_"; "MAX_"; "SUM_"; "AVG_";"STDDEV_";"VAR_"|]
                        let data = 
                            e.ColumnValues |> Seq.toArray |> Array.filter(fun (key, _) -> aggregates |> Array.exists (key.Contains) |> not)
                        let entity =
                            if itemEntityType = typeof<SqlEntity> then box e
                            elif itemEntityType = typeof<Tuple<SqlEntity,SqlEntity>> then
                                    box(Tuple<_,_>(e, e))
                            elif itemEntityType = typeof<Tuple<SqlEntity,SqlEntity,SqlEntity>> then
                                    box(Tuple<_,_,_>(e, e, e))
                            elif itemEntityType = typeof<Tuple<SqlEntity,SqlEntity,SqlEntity>> then
                                    box(Tuple<_,_,_,_>(e, e, e, e))
                            elif itemEntityType = typeof<Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<SqlEntity,SqlEntity>> then
                                    box(Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<_,_>(e, e))
                            elif itemEntityType = typeof<Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<SqlEntity,SqlEntity,SqlEntity>> then
                                    box(Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<_,_,_>(e, e, e))
                            elif itemEntityType = typeof<Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<SqlEntity,SqlEntity,SqlEntity,SqlEntity>> then
                                    box(Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<_,_,_,_>(e, e, e, e))
                            else failwith ("Not supported grouping: " + itemEntityType.Name)
                            
                        match data with
                        | [||] -> 
                            let ty = typedefof<GroupResultItems<_,_>>.MakeGenericType(typeof<int>, itemEntityType)
                            ty.GetConstructors().[1].Invoke [|"";1;entity|]
                        | [|keyname, keyvalue|] -> 
                            match keyType with
                            | Some keyTypev ->
                                let x = 
                                    let b = Utilities.convertTypes keyvalue keyTypev |> unbox
                                    if b.GetType() = typeof<obj> then unbox b else b
                                keyConstructor.Value.[1].Invoke [|keyname; x; entity;|]
                            | None ->
                                let ty = typedefof<GroupResultItems<_,_>>.MakeGenericType(keyvalue.GetType(), itemEntityType)
                                ty.GetConstructors().[1].Invoke [|keyname; keyvalue; entity;|]
                        | [|kn1, kv1; kn2, kv2|] when keyType.IsSome ->
                            let v1, v2 = getval kv1 0, getval kv2 1
                            keyConstructor.Value.[2].Invoke [|(kn1,kn2); tup2.Invoke(null, [|v1;v2|]); entity;|]
                        | [|kn1, kv1; kn2, kv2; kn3, kv3|] when keyType.IsSome ->
                            let v1, v2, v3 = getval kv1 0, getval kv2 1, getval kv3 2
                            keyConstructor.Value.[3].Invoke [|(kn1,kn2,kn3); tup3.Invoke(null, [|v1;v2;v3|]); entity;|]
                        | [|kn1, kv1; kn2, kv2; kn3, kv3; kn4, kv4|] when keyType.IsSome ->
                            let v1, v2, v3, v4 = getval kv1 0, getval kv2 1, getval kv3 2, getval kv4 3
                            keyConstructor.Value.[4].Invoke [|(kn1,kn2,kn3,kn4); tup4.Invoke(null, [|v1;v2;v3;v4|]); entity;|]
                        | [|kn1, kv1; kn2, kv2; kn3, kv3; kn4, kv4; kn5, kv5|] when keyType.IsSome ->
                            let v1, v2, v3, v4, v5 = getval kv1 0, getval kv2 1, getval kv3 2, getval kv4 3, getval kv5 4
                            keyConstructor.Value.[5].Invoke [|(kn1,kn2,kn3,kn4,kn5); tup5.Invoke(null, [|v1;v2;v3;v4;v5|]); entity;|]
                        | [|kn1, kv1; kn2, kv2; kn3, kv3; kn4, kv4; kn5, kv5; kn6, kv6|] when keyType.IsSome ->
                            let v1, v2, v3, v4, v5, v6 = getval kv1 0, getval kv2 1, getval kv3 2, getval kv4 3, getval kv5 4, getval kv6 5
                            keyConstructor.Value.[6].Invoke [|(kn1,kn2,kn3,kn4,kn5,kn6); tup6.Invoke(null, [|v1;v2;v3;v4;v5;v6|]); entity;|]
                        | [|kn1, kv1; kn2, kv2; kn3, kv3; kn4, kv4; kn5, kv5; kn6, kv6; kn7, kv7|] when keyType.IsSome ->
                            let v1, v2, v3, v4, v5, v6, v7 = getval kv1 0, getval kv2 1, getval kv3 2, getval kv4 3, getval kv5 4, getval kv6 5, getval kv7 6
                            keyConstructor.Value.[0].Invoke [|(kn1,kn2,kn3,kn4,kn5,kn6,kn7); tup7.Invoke(null, [|v1;v2;v3;v4;v5;v6;v7|]); entity;|]
                        | lst -> failwith("Complex key columns not supported yet (" + String.Join(",", lst) + ")")
                    )// :?> IGrouping<_, _>)

                for e in collected -> projector.DynamicInvoke(e) 
            else
                for e in results -> projector.DynamicInvoke(e) 
        } |> Seq.cache :> System.Collections.IEnumerable

    let executeQuery (dc:ISqlDataContext) (provider:ISqlProvider) sqlExp ti =
        use con = provider.CreateConnection(dc.ConnectionString)
        let (query,parameters,projector,baseTable) = QueryExpressionTransformer.convertExpression sqlExp ti con provider false (dc.SqlOperationsInSelect=SelectOperations.DatabaseSide)
        Common.QueryEvents.PublishSqlQuery con.ConnectionString query parameters
        // todo: make this lazily evaluated? or optionally so. but have to deal with disposing stuff somehow
        use cmd = provider.CreateCommand(con,query)
        if dc.CommandTimeout.IsSome then
            cmd.CommandTimeout <- dc.CommandTimeout.Value
        for p in parameters do cmd.Parameters.Add p |> ignore
        let columns = provider.GetColumns(con, baseTable)
        if con.State <> ConnectionState.Open then con.Open()
        use reader = cmd.ExecuteReader()
        let results = dc.ReadEntities(baseTable.FullName, columns, reader)
        let results = parseQueryResults projector results
        if (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close() //else get 'COM object that has been separated from its underlying RCW cannot be used.'
        results

    let executeQueryAsync (dc:ISqlDataContext) (provider:ISqlProvider) sqlExp ti =
       async {
           use con = provider.CreateConnection(dc.ConnectionString) :?> System.Data.Common.DbConnection
           let (query,parameters,projector,baseTable) = QueryExpressionTransformer.convertExpression sqlExp ti con provider false (dc.SqlOperationsInSelect=SelectOperations.DatabaseSide)
           Common.QueryEvents.PublishSqlQuery con.ConnectionString  query parameters
           // todo: make this lazily evaluated? or optionally so. but have to deal with disposing stuff somehow
           use cmd = provider.CreateCommand(con,query) :?> System.Data.Common.DbCommand
           if dc.CommandTimeout.IsSome then
               cmd.CommandTimeout <- dc.CommandTimeout.Value
           for p in parameters do cmd.Parameters.Add p |> ignore
           let columns = provider.GetColumns(con, baseTable) // TODO : provider.GetColumnsAsync() ??
           if con.State <> ConnectionState.Open then
                do! con.OpenAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
           if (con.State <> ConnectionState.Open) then // Just ensure, as not all the providers seems to work so great with OpenAsync.
                if (con.State <> ConnectionState.Closed) && (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close()
                con.Open()
           use! reader = cmd.ExecuteReaderAsync() |> Async.AwaitTask
           let! results = dc.ReadEntitiesAsync(baseTable.FullName, columns, reader)
           let results = parseQueryResults projector results
           if (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close() //else get 'COM object that has been separated from its underlying RCW cannot be used.'
           return results
       }

    let executeQueryScalar (dc:ISqlDataContext) (provider:ISqlProvider) sqlExp ti =
       use con = provider.CreateConnection(dc.ConnectionString)
       con.Open()
       let (query,parameters,_,_) = QueryExpressionTransformer.convertExpression sqlExp ti con provider false true
       Common.QueryEvents.PublishSqlQuery con.ConnectionString  query parameters
       use cmd = provider.CreateCommand(con,query)
       if dc.CommandTimeout.IsSome then
           cmd.CommandTimeout <- dc.CommandTimeout.Value
       for p in parameters do cmd.Parameters.Add p |> ignore
       // ignore any generated projection and just expect a single integer back
       if con.State <> ConnectionState.Open then con.Open()
       let result = cmd.ExecuteScalar()
       if (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close() //else get 'COM object that has been separated from its underlying RCW cannot be used.'
       result

    let executeQueryScalarAsync (dc:ISqlDataContext) (provider:ISqlProvider) sqlExp ti =
       async {
           use con = provider.CreateConnection(dc.ConnectionString) :?> System.Data.Common.DbConnection
           do! con.OpenAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
           let (query,parameters,_,_) = QueryExpressionTransformer.convertExpression sqlExp ti con provider false true
           Common.QueryEvents.PublishSqlQuery con.ConnectionString query parameters
           use cmd = provider.CreateCommand(con,query) :?> System.Data.Common.DbCommand
           if dc.CommandTimeout.IsSome then
               cmd.CommandTimeout <- dc.CommandTimeout.Value
           for p in parameters do cmd.Parameters.Add p |> ignore
           // ignore any generated projection and just expect a single integer back
           if con.State <> ConnectionState.Open then
                do! con.OpenAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
           if (con.State <> ConnectionState.Open) then // Just ensure, as not all the providers seems to work so great with OpenAsync.
                if (con.State <> ConnectionState.Closed) && (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close()
                con.Open()
           let! executed = cmd.ExecuteScalarAsync() |> Async.AwaitTask
           if (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close() //else get 'COM object that has been separated from its underlying RCW cannot be used.'
           return executed
       }

    let executeDeleteQueryAsync (dc:ISqlDataContext) (provider:ISqlProvider) sqlExp ti =
       async {
            // Not too complex clauses please...
            // No "AS" command allowed for basetable. Little visitor-pattern to modify base-alias name.
           let rec modifyAlias (sqlx:SqlExp) =
               match sqlx with
               | BaseTable (alias,table) when (alias = "" || alias = table.Name || alias = table.FullName ) -> sqlx //ok
               | BaseTable (_,table) -> BaseTable (table.Name,table)
               | FilterClause(a,rest) -> FilterClause(a,modifyAlias rest)
               | AggregateOp(a,c,rest) -> AggregateOp(a,c,modifyAlias rest)
               | _ -> failwithf "Unsupported delete-clause. Only simple single-table deletion where-clauses supported. You had parameters: %O" sqlx

           let sqlExp = modifyAlias sqlExp
           use con = provider.CreateConnection(dc.ConnectionString) :?> System.Data.Common.DbConnection
           do! con.OpenAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
           let (query,parameters,_,_) = QueryExpressionTransformer.convertExpression sqlExp ti con provider true true
           Common.QueryEvents.PublishSqlQuery con.ConnectionString query parameters
           use cmd = provider.CreateCommand(con,query) :?> System.Data.Common.DbCommand
           if dc.CommandTimeout.IsSome then
               cmd.CommandTimeout <- dc.CommandTimeout.Value
           for p in parameters do cmd.Parameters.Add p |> ignore
           // ignore any generated projection and just expect a single integer back
           if con.State <> ConnectionState.Open then
                do! con.OpenAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
           if (con.State <> ConnectionState.Open) then // Just ensure, as not all the providers seems to work so great with OpenAsync.
                if (con.State <> ConnectionState.Closed) && (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close()
                con.Open()
           let! executed = cmd.ExecuteScalarAsync() |> Async.AwaitTask
           if (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close() //else get 'COM object that has been separated from its underlying RCW cannot be used.'
           return executed
       }

    type SqlQueryable<'T>(dc:ISqlDataContext,provider,sqlQuery,tupleIndex) =
        let asyncModePreEvaluated = System.Collections.Concurrent.ConcurrentStack<_>() 
        static member Create(table,conString,provider) =
            SqlQueryable<'T>(conString,provider,BaseTable("",table),ResizeArray<_>()) :> IQueryable<'T>
        interface IQueryable<'T>
        interface IQueryable with
            member __.Provider = SqlQueryProvider.Provider
            member x.Expression =  Expression.Constant(x,typeof<IQueryable<'T>>) :> Expression
            member __.ElementType = typeof<'T>
        interface seq<'T> with
             member __.GetEnumerator() = 
                match asyncModePreEvaluated.TryPop() with
                | false, _ -> (Seq.cast<'T> (executeQuery dc provider sqlQuery tupleIndex)).GetEnumerator()
                | true, res -> (Seq.cast<'T> res).GetEnumerator()
        interface IEnumerable with
             member x.GetEnumerator() = (x :> seq<'T>).GetEnumerator() :> IEnumerator
        interface IWithDataContext with
             member __.DataContext = dc
        interface IWithSqlService with
             member __.DataContext = dc
             member __.SqlExpression = sqlQuery
             member __.TupleIndex = tupleIndex
             member __.Provider = provider
        interface IAsyncEnumerable with
             member __.EvaluateQuery() = 
                async {
                    let! executeSql = executeQueryAsync dc provider sqlQuery tupleIndex
                    asyncModePreEvaluated.Push executeSql 
                    return ()
                }
        interface IAsyncEnumerable<'T> with
             member __.GetAsyncEnumerator() =
                async {
                    let! executeSql = executeQueryAsync dc provider sqlQuery tupleIndex
                    return (Seq.cast<'T> (executeSql)).GetEnumerator()
                }
    
    and SqlOrderedQueryable<'T>(dc:ISqlDataContext,provider,sqlQuery,tupleIndex) =
        let asyncModePreEvaluated = System.Collections.Concurrent.ConcurrentStack<_>() 
        static member Create(table,conString,provider) =
            SqlOrderedQueryable<'T>(conString,provider,BaseTable("",table),ResizeArray<_>()) :> IQueryable<'T>
        interface IOrderedQueryable<'T>
        interface IQueryable<'T>
        interface IQueryable with
            member __.Provider = SqlQueryProvider.Provider
            member x.Expression =  Expression.Constant(x,typeof<IOrderedQueryable<'T>>) :> Expression
            member __.ElementType = typeof<'T>
        interface seq<'T> with
             member __.GetEnumerator() = 
                match asyncModePreEvaluated.TryPop() with
                | false, _ -> (Seq.cast<'T> (executeQuery dc provider sqlQuery tupleIndex)).GetEnumerator()
                | true, res -> (Seq.cast<'T> res).GetEnumerator()
        interface IEnumerable with
             member x.GetEnumerator() = (x :> seq<'T>).GetEnumerator() :> IEnumerator
        interface IWithDataContext with
            member __.DataContext = dc
        interface IWithSqlService with
             member __.DataContext = dc
             member __.SqlExpression = sqlQuery
             member __.TupleIndex = tupleIndex
             member __.Provider = provider
        interface IAsyncEnumerable with
             member __.EvaluateQuery() = 
                async {
                    let! executeSql = executeQueryAsync dc provider sqlQuery tupleIndex
                    asyncModePreEvaluated.Push executeSql 
                    return ()
                }
        interface IAsyncEnumerable<'T> with
             member __.GetAsyncEnumerator() =
                async {
                    let! executeSql = executeQueryAsync dc provider sqlQuery tupleIndex
                    return (Seq.cast<'T> (executeSql)).GetEnumerator()
                }

    /// Structure to make it easier to return IGrouping from GroupBy
    and SqlGroupingQueryable<'TKey, 'TEntity>(dc:ISqlDataContext,provider,sqlQuery,tupleIndex) =
        let asyncModePreEvaluated = System.Collections.Concurrent.ConcurrentStack<_>() 
        static member Create(table,conString,provider) =
            let res = SqlGroupingQueryable<'TKey, 'TEntity>(conString,provider,BaseTable("",table),ResizeArray<_>())
            res :> IQueryable<IGrouping<'TKey, 'TEntity>>
        interface IQueryable<IGrouping<'TKey, 'TEntity>>
        interface IQueryable with
            member __.Provider = 
                SqlQueryProvider.Provider
            member x.Expression =  
                Expression.Constant(x,typeof<SqlGroupingQueryable<'TKey, 'TEntity>>) :> Expression
            member __.ElementType = 
                typeof<IGrouping<'TKey, 'TEntity>>
        interface seq<IGrouping<'TKey, 'TEntity>> with
             member __.GetEnumerator() = 
                match asyncModePreEvaluated.TryPop() with
                | false, _ -> 
                    executeQuery dc provider sqlQuery tupleIndex
                        |> Seq.cast<IGrouping<'TKey, 'TEntity>>
                        |> fun res -> res.GetEnumerator()
                | true, res -> (Seq.cast<IGrouping<'TKey, 'TEntity>> res).GetEnumerator()
        interface IEnumerable with
             member x.GetEnumerator() = 
                let itms = (x :> seq<IGrouping<'TKey, 'TEntity>>)
                itms.GetEnumerator() :> IEnumerator
        interface IWithDataContext with
             member __.DataContext = dc
        interface IWithSqlService with
             member __.DataContext = dc
             member __.SqlExpression = sqlQuery
             member __.TupleIndex = tupleIndex
             member __.Provider = provider
        interface IAsyncEnumerable with
             member __.EvaluateQuery() = 
                async {
                    let! executeSql = executeQueryAsync dc provider sqlQuery tupleIndex
                    asyncModePreEvaluated.Push executeSql 
                    return ()
                }
        interface IAsyncEnumerable<IGrouping<'TKey, 'TEntity>> with
             member __.GetAsyncEnumerator() =
                async {
                    let! executeSql = executeQueryAsync dc provider sqlQuery tupleIndex
                    let toseq = executeSql |> Seq.cast<IGrouping<'TKey, 'TEntity>>
                    return toseq.GetEnumerator()
                }
    and SqlWhereType = NormalWhere | HavingWhere
    and SqlQueryProvider() =
         static member val Provider =

             let rec parseWhere (meth:Reflection.MethodInfo) (source:IWithSqlService) (qual:Expression) =
             
                let isHaving = source.SqlExpression.hasGroupBy().IsSome
                // if same query contains multiple subqueries, the parameter names in those should be different.
                let mutable nestCount = 0

                let (|KnownTemporaryVariable|_|) (exp:Expression) =
                    if exp.Type.Name <> "Boolean" then None
                    else
                    let rec composeLambdas (traversable:Expression) sqlExprProj =

                        // Detect a LINQ-"helper" and unwrap it
                        // There are two types of nesting-generation: into-keyword and let-keyword.
                        // Let creates anonymous tuple of Item1 Item2 where into hides the previous tree.
                        // We have to merge exp + qual expressions
                        match traversable, sqlExprProj with
                        | :? LambdaExpression as le,
                                Projection(MethodCall(None, MethodWithName("Select"), [a ; OptionalQuote(Lambda([pe], nexp) as lambda1)]),innerProj)
                                when le.Parameters.Count = 1 && nexp.Type = le.Parameters.[0].Type ->

                            let visitor =
                                { new ExpressionVisitor() with
                                    member __.VisitParameter _ = nexp
                                    member __.VisitLambda x =
                                        let visitedBody = base.Visit x.Body
                                        Expression.Lambda(visitedBody, pe) :> Expression
                                    }
                            let visited = visitor.Visit traversable
                            let opt = ExpressionOptimizer.visit visited

                            if pe.Type = typeof<SqlEntity> then

                                Some opt
                            else
                                composeLambdas opt innerProj
                        | _ -> None
                    composeLambdas qual source.SqlExpression

                let (|Condition|_|) exp =
                    // IMPORTANT : for now it is always assumed that the table column being checked on the server side is on the left hand side of the condition expression.
                    match exp with
                    | SqlSpecialOpArrQueryable(ti,op,key,qry)
                    | SqlSpecialNegativeOpArrQueryable(ti,op,key,qry) ->

                        let svc = (qry :?> IWithSqlService)

                        use con = svc.Provider.CreateConnection(svc.DataContext.ConnectionString)
                        let (query,parameters,projector,baseTable) = QueryExpressionTransformer.convertExpression svc.SqlExpression svc.TupleIndex con svc.Provider false true

                        let ``nested param names`` = "@param" + abs(query.GetHashCode()).ToString() + nestCount.ToString() + "nested"
                        nestCount <- nestCount + 1

                        let modified = 
                            parameters
                            |> Seq.filter(fun p -> not(p.ParameterName.StartsWith ``nested param names``))
                            |> Seq.map(fun p ->
                                p.ParameterName <- p.ParameterName.Replace("@param", ``nested param names``)
                                p
                            ) |> Seq.toArray
                        let subquery = 
                            let paramfixed = query.Replace("@param", ``nested param names``)
                            match paramfixed.EndsWith(";") with
                            | false -> paramfixed
                            | true -> paramfixed.Substring(0, paramfixed.Length-1)
                        
                        Some(ti,key,op,Some (box (subquery, modified)))
                    | SqlExistsClause(meth,op,src,qual)
                    | SqlNotExistsClause(meth,op,src,qual) ->

                        match qual with
                        | Lambda([ParamName sourceAlias],_) when sourceAlias <> "" && source.TupleIndex.Any(fun v -> v = sourceAlias) |> not ->
                            source.TupleIndex.Add sourceAlias
                        | _ -> ()

                        let innersrc = (src :?> IWithSqlService)
                        source.TupleIndex |> Seq.filter(fun s -> not(innersrc.TupleIndex.Contains s)) |> Seq.iter(fun s -> innersrc.TupleIndex.Add s)
                        let qry = parseWhere meth innersrc qual :> IQueryable
                        let svc = (qry :?> IWithSqlService)
                        use con = svc.Provider.CreateConnection(svc.DataContext.ConnectionString)

                        let (query,parameters,projector,baseTable) = QueryExpressionTransformer.convertExpression svc.SqlExpression svc.TupleIndex con svc.Provider false true

                        let ``nested param names`` = "@param" + abs(query.GetHashCode()).ToString() + nestCount.ToString() + "nested"
                        nestCount <- nestCount + 1

                        let modified = 
                            parameters 
                            |> Seq.filter(fun p -> not(p.ParameterName.StartsWith ``nested param names``))
                            |> Seq.map(fun p ->
                                p.ParameterName <- p.ParameterName.Replace("@param", ``nested param names``)
                                p
                            ) |> Seq.toArray
                        let subquery = 
                            let paramfixed = query.Replace("@param", ``nested param names``)
                            match paramfixed.EndsWith(";") with
                            | false -> paramfixed
                            | true -> paramfixed.Substring(0, paramfixed.Length-1)
                        
                        Some("",KeyColumn(""),op,Some (box (subquery, modified)))
                    | SimpleCondition ((ti,key,op,c) as x) -> 
                        match c with
                        | None -> Some x
                        | Some t when (t :? (string*SqlColumnType)) ->
                            let ti2, col = t :?> string*SqlColumnType
                            let alias2 = 
                                if ti2.StartsWith "Item" then resolveTuplePropertyName ti2 source.TupleIndex
                                else ti2
                            Some(ti,key,op,Some(box (alias2,col)))
                        | Some _ -> Some x
                    | _ -> None

                let rec filterExpression (exp:Expression)  =
                    let extendFilter conditions nextFilter =
                        match exp with
                        | AndAlso(_) -> And(conditions,nextFilter)
                        | OrElse(_) -> Or(conditions,nextFilter)
                        | _ -> failwith ("Filter problem: " + exp.ToString())
                    match exp with
                    | AndAlsoOrElse(AndAlsoOrElse(_,_) as left, (AndAlsoOrElse(_,_) as right)) ->
                        extendFilter [] (Some ([filterExpression left; filterExpression right]))
                    | AndAlsoOrElse(AndAlsoOrElse(_,_) as left,Condition(c))  ->
                        extendFilter [c] (Some ([filterExpression left]))
                    | AndAlsoOrElse(Condition(c),(AndAlsoOrElse(_,_) as right))  ->
                        extendFilter [c] (Some ([filterExpression right]))
                    | AndAlsoOrElse(Condition(c1) as cc1 ,Condition(c2)) as cc2 ->
                        if cc1 = cc2 then extendFilter [c1] None
                        else extendFilter [c1;c2] None
                    | Condition(cond) ->
                        Condition.And([cond],None)

                    // Support for simple boolean expressions:
                    | AndAlso(Bool(b), x) | AndAlso(x, Bool(b)) when b = true -> filterExpression x
                    | OrElse(Bool(b), x) | OrElse(x, Bool(b)) when b = false -> filterExpression x
                    | Bool(b) when b -> Condition.ConstantTrue
                    | Bool(b) when not(b) -> Condition.ConstantFalse
                    | KnownTemporaryVariable(Lambda(_,Condition(cond))) ->
                        Condition.And([cond],None)
                    | _ -> 
                        failwith ("Unsupported expression. Ensure all server-side objects won't have any .NET-operators/methods that can't be converted to SQL. The In and Not In operators only support the inline array syntax. " + exp.ToString())

                match qual with
                | Lambda([name],ex) ->
                    // name here will either be the alias the user entered in the where clause if no joining / select many has happened before this
                    // otherwise, it will be the compiler-generated alias eg _arg2.  this might be the first method called in which case set the
                    // base entity alias to this name.
                    let ex = ExpressionOptimizer.visit ex
                    let filter = filterExpression ex
                    let sqlExpression =
                        match source.SqlExpression with
                        | BaseTable(alias,entity) when alias = "" ->
                            // special case here as above - this is the first call so replace the top of the tree here with the current base entity alias and the filter
                            FilterClause(filter,BaseTable(name.Name,entity))
                        | current ->
                            if isHaving then HavingClause(filter,current)
                            else FilterClause(filter,current)

                    let ty = typedefof<SqlQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                    ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; sqlExpression; source.TupleIndex; |] :?> IQueryable<_>
                | _ -> failwith "only support lambdas in a where"

             let parseGroupBy (meth:Reflection.MethodInfo) (source:IWithSqlService) sourceAlias destAlias (lambdas: LambdaExpression list) (exp:Expression) (sourceTi:string)=
                let sAlias, sourceEntity =

                    match source.SqlExpression with
                    | BaseTable(alias,sourceEntity)
                    | FilterClause(_, BaseTable(alias,sourceEntity)) ->
                        sourceAlias, sourceEntity
                    | FilterClause(_, SelectMany(a1, a2,CrossJoin(_,_),sqlExp))
                    | FilterClause(_, SelectMany(a1, a2,LinkQuery(_),sqlExp))
                    | SelectMany(a1, a2,CrossJoin(_,_),sqlExp)
                    | SelectMany(a1, a2,LinkQuery(_),sqlExp)  ->
                        //let sourceAlias = if sourceTi <> "" then Utilities.resolveTuplePropertyName sourceTi source.TupleIndex else sourceAlias
                        //if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)

                        match sqlExp with
                        | BaseTable(alias,sourceEntity)
                        | FilterClause(_, BaseTable(alias,sourceEntity)) when alias = a1 -> a1, sourceEntity
                        | BaseTable(alias,sourceEntity)
                        | FilterClause(_, BaseTable(alias,sourceEntity)) when alias = a2 -> a2, sourceEntity
                        | FilterClause(_, SelectMany(a3, a4,CrossJoin(_,_),sqlExp2))
                        | FilterClause(_, SelectMany(a3, a4,LinkQuery(_),sqlExp2))
                        | SelectMany(a3, a4,CrossJoin(_,_),sqlExp2)
                        | SelectMany(a3, a4,LinkQuery(_),sqlExp2)  ->
                            match sqlExp2 with
                            | BaseTable(alias,sourceEntity)
                            | FilterClause(_, BaseTable(alias,sourceEntity)) when alias = a3 -> alias, sourceEntity
                            | BaseTable(alias,sourceEntity)
                            | FilterClause(_, BaseTable(alias,sourceEntity)) when alias = a4 -> alias, sourceEntity
                            | _ -> failwithf "Grouping over multiple tables is not supported yet (%A)." source.SqlExpression
                        | _ -> failwithf "Grouping over multiple tables is not supported yet (%A)." source.SqlExpression
                    | _ -> failwithf "Unexpected groupby entity expression (%A)." source.SqlExpression

                let getAlias ti =
                        match ti, source.SqlExpression with
                        | "", _ when source.SqlExpression.HasAutoTupled() -> sAlias
                        | "", FilterClause(alias,sourceEntity) -> sAlias
                        | "", _ -> ""
                        | _ -> resolveTuplePropertyName ti source.TupleIndex

                let keycols = 
                    match exp with
                    | SqlColumnGet(sourceTi,sourceKey,_) -> [getAlias sourceTi, sourceKey]
                    | TupleSqlColumnsGet itms -> itms |> List.map(fun (ti,key,typ) -> getAlias ti, key)
                    | _ -> []

                let data =  {
                    PrimaryTable = sourceEntity
                    KeyColumns = keycols
                    AggregateColumns = [] // Aggregates will be populated later: [CountOp,alias,"City"]
                    Projection = None //lambda2 ?
                }

                let ty =
                    match lambdas with
                    | [x] -> typedefof<SqlGroupingQueryable<_,_>>.MakeGenericType(lambdas.[0].ReturnType,  meth.GetGenericArguments().[0])
                    | [x1;x2] -> typedefof<SqlGroupingQueryable<_,_>>.MakeGenericType(lambdas.[0].ReturnType, lambdas.[1].ReturnType)
                    | _ -> failwith "Unknown grouping lambdas"

                ty, data, sAlias


             // multiple SelectMany calls in sequence are represented in the same expression tree which must be parsed recursively (and joins too!)
             let rec processSelectManys (toAlias:string) (inExp:Expression) (outExp:SqlExp) (projectionParams : ParameterExpression list) (source:IWithSqlService) =
                match inExp with
                | MethodCall(None, (MethodWithName "SelectMany"), [ createRelated ; OptionalQuote (Lambda([_], inner)); OptionalQuote (Lambda(projectionParams,_)) ]) ->
                    let outExp = processSelectManys projectionParams.[0].Name createRelated outExp projectionParams source
                    processSelectManys projectionParams.[1].Name inner outExp projectionParams source
            //                            | MethodCall(None, (MethodWithName "GroupBy" | MethodWithName "GroupJoin" as meth),
            //                                                    [createRelated
            //                                                     ConvertOrTypeAs(MethodCall(Some(Lambda(_,MethodCall(_,MethodWithName "CreateEntities",[String destEntity]))),(MethodWithName "Invoke"),_))
            //                                                     OptionalQuote (Lambda([ParamName sourceAlias], exp) as lambda1)
            //                                                     OptionalQuote (Lambda(_,_))])
            //                            | MethodCall(None, (MethodWithName "GroupBy" | MethodWithName "GroupJoin" as meth),
            //                                                    [createRelated
            //                                                     ConvertOrTypeAs(MethodCall(_, (MethodWithName "CreateEntities"), [String destEntity] ))
            //                                                     OptionalQuote (Lambda([ParamName sourceAlias], exp) as lambda1)
            //                                                     OptionalQuote (Lambda(_,_))]) 
            //                            | MethodCall(None, (MethodWithName "GroupBy" | MethodWithName "GroupJoin" as meth),
            //                                                    [createRelated
            //                                                     ConvertOrTypeAs(MethodCall(Some(Lambda(_,MethodCall(_,MethodWithName "CreateEntities",[String destEntity]))),(MethodWithName "Invoke"),_))
            //                                                     OptionalQuote (Lambda([ParamName sourceAlias], exp) as lambda1)])
                | MethodCall(None, (MethodWithName "GroupBy" | MethodWithName "GroupJoin" as meth),
                                        [createRelated
                                         ConvertOrTypeAs(MethodCall(_, (MethodWithName "CreateEntities"), [String destEntity] ))
                                         OptionalQuote (Lambda([ParamName sourceAlias], exp) as lambda1)]) ->

                    let lambda = lambda1 :?> LambdaExpression
                    let outExp = processSelectManys projectionParams.[0].Name createRelated outExp projectionParams source
                    let ty, data, sourceEntityName = parseGroupBy meth source sourceAlias destEntity [lambda] exp ""
                    SelectMany(sourceEntityName,destEntity,GroupQuery(data), outExp)
                | MethodCall(None, (MethodWithName "Join"),
                                        [createRelated
                                         OptionalOuterJoin(isOuter, ConvertOrTypeAs(MethodCall(Some(Lambda(_,MethodCall(_,MethodWithName "CreateEntities",[String destEntity]))),(MethodWithName "Invoke"),_)))
                                         OptionalQuote (Lambda([ParamName sourceAlias],SqlColumnGet(sourceTi,sourceKey,_)))
                                         OptionalQuote (Lambda([ParamName destAlias],SqlColumnGet(_,destKey,_)))
                                         OptionalQuote (Lambda(projectionParams,_))])
                | MethodCall(None, (MethodWithName "Join"),
                                        [createRelated
                                         OptionalOuterJoin(isOuter, ConvertOrTypeAs(MethodCall(_, (MethodWithName "CreateEntities"), [String destEntity] )))
                                         OptionalQuote (Lambda([ParamName sourceAlias],SqlColumnGet(sourceTi,sourceKey,_)))
                                         OptionalQuote (Lambda([ParamName destAlias],SqlColumnGet(_,destKey,_)))
                                         OptionalQuote (Lambda(projectionParams,_))]) ->
                    let sourceTi,sourceKey =
                        match sourceKey, source.SqlExpression with
                        | GroupColumn(KeyOp "", KeyColumn "Key"), SelectMany(_,_,GroupQuery g,_) when (g.KeyColumns.Length = 1) ->
                            g.KeyColumns.[0]
                        | _ -> sourceTi, sourceKey
                    // this case happens when the select many also includes one or more joins in the same tree.
                    // in this situation, the first agrument will either be an additional nested join method call,
                    // or finally it will be the call to _CreatedRelated which is handled recursively in the next case
                    let outExp = processSelectManys projectionParams.[0].Name createRelated outExp projectionParams source
                    let sourceAlias = if sourceTi <> "" then Utilities.resolveTuplePropertyName sourceTi source.TupleIndex else sourceAlias
                    if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                    if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                    // we don't actually have the "foreign" table name here in a join as that information is "lost" further up the expression tree.
                    // it's ok though because it can always be resolved later after the whole expression tree has been evaluated
                    let data = { PrimaryKey = [destKey]; PrimaryTable = Table.FromFullName destEntity; ForeignKey = [sourceKey];
                                    ForeignTable = {Schema="";Name="";Type=""};
                                    OuterJoin = isOuter; RelDirection = RelationshipDirection.Parents }
                    SelectMany(sourceAlias,destAlias,LinkQuery(data),outExp)
                | OptionalOuterJoin(outerJoin,MethodCall(Some(_),(MethodWithName "CreateRelated"), [param; _; String PE; String PK; String FE; String FK; RelDirection dir;])) ->
                                
                    let parseKey itm =
                        SqlColumnType.KeyColumn itm
                    let fromAlias =
                        match param with
                        | ParamName x -> x
                        | PropertyGet(_,p) -> Utilities.resolveTuplePropertyName p.Name source.TupleIndex
                        | _ -> failwith "unsupported parameter expression in CreatedRelated method call"
                    let data = { PrimaryKey = [parseKey PK]; PrimaryTable = Table.FromFullName PE; ForeignKey = [parseKey FK]; ForeignTable = Table.FromFullName FE; OuterJoin = outerJoin; RelDirection = dir  }
                    let sqlExpression =
                        match outExp with
                        | BaseTable(alias,entity) when alias = "" ->
                            // special case here as above - this is the first call so replace the top of the tree here with the current base entity alias and the select many
                            SelectMany(fromAlias,toAlias,LinkQuery(data),BaseTable(alias,entity))
                        | _ ->
                            SelectMany(fromAlias,toAlias,LinkQuery(data),outExp)
                    // add new aliases to the tuple index
                    if source.TupleIndex.Any(fun v -> v = fromAlias) |> not then source.TupleIndex.Add(fromAlias)
                    if source.TupleIndex.Any(fun v -> v = toAlias) |> not then  source.TupleIndex.Add(toAlias)
                    sqlExpression
                | MethodCall(None, (MethodWithName "Join"),
                                        [createRelated
                                         OptionalOuterJoin(isOuter, ConvertOrTypeAs(OptionalConvertOrTypeAs(MethodCall(Some(Lambda(_,MethodCall(_,MethodWithName "CreateEntities",[String destEntity]))),(MethodWithName "Invoke"),_))))
                                         OptionalQuote (Lambda([ParamName sourceAlias],TupleSqlColumnsGet(multisource)))
                                         OptionalQuote (Lambda([ParamName destAlias],TupleSqlColumnsGet(multidest)))
                                         OptionalQuote (Lambda(projectionParams,_))])
                | MethodCall(None, (MethodWithName "Join"),
                                        [createRelated
                                         OptionalOuterJoin(isOuter, ConvertOrTypeAs(OptionalConvertOrTypeAs(MethodCall(_, MethodWithName "CreateEntities",[String destEntity])))) 
                                         OptionalQuote (Lambda([ParamName sourceAlias],TupleSqlColumnsGet(multisource)))
                                         OptionalQuote (Lambda([ParamName destAlias],TupleSqlColumnsGet(multidest)))
                                         OptionalQuote (Lambda(projectionParams,_))]) ->
                    let outExp = processSelectManys projectionParams.[0].Name createRelated outExp projectionParams source

                    let destKeys = multidest |> List.map(fun (_,destKey,_) -> destKey)
                    let aliashandlesSource =
                        multisource |> List.map(
                            fun (sourceTi,sourceKey,_) ->
                                let sourceAlias = if sourceTi <> "" then Utilities.resolveTuplePropertyName sourceTi source.TupleIndex else sourceAlias
                                if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                                if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                                sourceAlias, sourceKey
                            )
                    let sourceAlias = match aliashandlesSource with [] -> sourceAlias | (alias,_)::t -> alias
                    let sourceKeys = aliashandlesSource |> List.map snd

                    let data = { PrimaryKey = destKeys; PrimaryTable = Table.FromFullName destEntity; ForeignKey = sourceKeys;
                                    ForeignTable = {Schema="";Name="";Type=""};
                                    OuterJoin = isOuter; RelDirection = RelationshipDirection.Parents }
                    SelectMany(sourceAlias,destAlias,LinkQuery(data),outExp)
                | OptionalOuterJoin(isOuter, OptionalConvertOrTypeAs(MethodCall(Some(Lambda([_],MethodCall(_,MethodWithName "CreateEntities",[String destEntity]))),(MethodWithName "Invoke"),_))) 
                | OptionalOuterJoin(isOuter, OptionalConvertOrTypeAs(MethodCall(_, MethodWithName "CreateEntities",[String destEntity]))) ->
                    let sourceAlias =
                        if source.TupleIndex.Contains projectionParams.[0].Name then projectionParams.[0].Name
                        else
                        match source.SqlExpression with
                        | BaseTable(a, t) -> t.Name
                        | _ -> projectionParams.[0].Name
                    let table = Table.FromFullName destEntity
                    let destAlias = table.Name

                    if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                    if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                    SelectMany(sourceAlias,destAlias,CrossJoin(table.Name,table),outExp)
                | MethodCall(None, (MethodWithName "AsQueryable"), [innerExp]) ->
                    processSelectManys projectionParams.[0].Name innerExp outExp projectionParams source
                | MethodCall(None, (MethodWithName "Select"), [ createRelated; OptionalQuote (Lambda([ v1 ], _) as lambda) ]) as whole ->
                    let outExp = processSelectManys projectionParams.[0].Name createRelated outExp projectionParams source
                    Projection(whole,outExp)
                | _ -> failwith ("Unknown: " + inExp.ToString())

             // Possible Linq method overrides are available here: 
             // https://referencesource.microsoft.com/#System.Core/System/Linq/IQueryable.cs
             // https://msdn.microsoft.com/en-us/library/system.linq.enumerable_methods(v=vs.110).aspx
             { new System.Linq.IQueryProvider with
                member __.CreateQuery(e:Expression) : IQueryable = failwithf "CreateQuery, e = %A" e
                member __.CreateQuery<'T>(e:Expression) : IQueryable<'T> =
                    Common.QueryEvents.PublishExpression e
                    match e with
                    | MethodCall(None, (MethodWithName "Skip" as meth), [SourceWithQueryData source; Int amount]) ->
                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                        ty.GetConstructors().[0].Invoke [| source.DataContext ; source.Provider; Skip(amount,source.SqlExpression) ; source.TupleIndex; |] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "Take" as meth), [SourceWithQueryData source; Int amount]) ->
                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                        ty.GetConstructors().[0].Invoke [| source.DataContext ; source.Provider; Take(amount,source.SqlExpression) ; source.TupleIndex; |] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "OrderBy" | MethodWithName "OrderByDescending" as meth), [SourceWithQueryData source; OptionalQuote (Lambda([ParamName param], OptionalConvertOrTypeAs (SqlColumnGet(entity,key,_)))) ]) ->
                        let alias =
                             match entity with
                             | "" when source.SqlExpression.HasAutoTupled() && source.TupleIndex.Contains param -> param
                             | "" -> ""
                             | _ -> Utilities.resolveTuplePropertyName entity source.TupleIndex
                        let ascending = meth.Name = "OrderBy"
                        let gb = source.SqlExpression.hasGroupBy()
                        let sqlExpression =
                               match source.SqlExpression, gb, key with
                               | BaseTable("",entity),_,_ -> OrderBy("",key,ascending,BaseTable(alias,entity))
                               | _, Some gbv, GroupColumn(KeyOp(""), _) -> 
                                    gbv |> snd |> List.fold(fun exprstate (al,itm) ->
                                        OrderBy(al,itm,ascending,exprstate)) source.SqlExpression
                               | _ ->  OrderBy(alias,key,ascending,source.SqlExpression)
                        let ty = typedefof<SqlOrderedQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                        let x = ty.GetConstructors().[0].Invoke [| source.DataContext ; source.Provider; sqlExpression; source.TupleIndex; |]
                        x :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "ThenBy" | MethodWithName "ThenByDescending" as meth), [SourceWithQueryData source; OptionalQuote (Lambda([ParamName param], OptionalConvertOrTypeAs (SqlColumnGet(entity,key,_)))) ]) ->
                        let alias =
                            match entity with
                            | "" when source.SqlExpression.HasAutoTupled() && source.TupleIndex.Contains param -> param
                            | "" -> ""
                            | _ -> Utilities.resolveTuplePropertyName entity source.TupleIndex
                        let ty = typedefof<SqlOrderedQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                        let ascending = meth.Name = "ThenBy"
                        match source.SqlExpression with
                        | OrderBy(_) ->
                            let gb = source.SqlExpression.hasGroupBy()
                            let sqlExpression =
                               match gb, key with
                               | Some gbv, GroupColumn(KeyOp(""), _) ->
                                        gbv |> snd |> List.fold(fun exprstate (al,itm) ->
                                            OrderBy(al,itm,ascending,exprstate)) source.SqlExpression
                               | _ -> OrderBy(alias,key,ascending,source.SqlExpression)
                            let x = ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; sqlExpression ; source.TupleIndex; |]
                            x :?> IQueryable<_>
                        | _ -> failwith (sprintf "'thenBy' operations must come immediately after a 'sortBy' operation in a query")

                    | MethodCall(None, (MethodWithName "Distinct" as meth), [ SourceWithQueryData source ]) ->
                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                        ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; Distinct(source.SqlExpression) ; source.TupleIndex; |] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "Where" as meth), [ SourceWithQueryData source; OptionalQuote qual ]) ->
                        parseWhere meth source qual
//                    | MethodCall(None, (MethodWithName "GroupBy" | MethodWithName "GroupJoin" as meth),
//                                    [ SourceWithQueryData source;
//                                      OptionalQuote (Lambda([ParamName lambdaparam], exp) as lambda1);
//                                      OptionalQuote (Lambda([ParamName _], _))
//                                      OptionalQuote (Lambda([ParamName _], _))]) 
//                    | MethodCall(None, (MethodWithName "GroupBy" | MethodWithName "GroupJoin" as meth),
//                                    [ SourceWithQueryData source;
//                                      OptionalQuote (Lambda([ParamName lambdaparam], exp) as lambda1);
//                                      OptionalQuote (Lambda([ParamName _], _))])
                    | MethodCall(None, (MethodWithName "GroupBy" as meth),
                                    [ SourceWithQueryData source;
                                      OptionalQuote (Lambda([ParamName lambdaparam], exp) as lambda1);
                                      OptionalQuote (Lambda([ParamName lambdaparam2], exp2) as lambda2) ]) ->
                        let lambda = lambda1 :?> LambdaExpression
                        let lambdae2 = lambda2 :?> LambdaExpression
                        let ty, data, sourceEntityName = parseGroupBy meth source lambdaparam "" [lambda; lambdae2] exp ""
                        let vals = { data with Projection = Some lambda2 }
                        let expr = SelectMany(sourceEntityName,"grp",GroupQuery(vals), source.SqlExpression)

                        ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; expr; source.TupleIndex;|] :?> IQueryable<'T>

                    | MethodCall(None, (MethodWithName "GroupBy" as meth),
                                    [ SourceWithQueryData source;
                                      OptionalQuote (Lambda([ParamName lambdaparam], exp) as lambda1)]) ->
                        let lambda = lambda1 :?> LambdaExpression
                        let ty, data, sourceEntityName = parseGroupBy meth source lambdaparam "" [lambda] exp ""
                        let expr = SelectMany(sourceEntityName,"grp",GroupQuery(data), source.SqlExpression)

                        ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; expr; source.TupleIndex;|] :?> IQueryable<'T>

                    | MethodCall(None, (MethodWithName "Join"),
                                    [ SourceWithQueryData source;
                                      SourceWithQueryData dest
                                      OptionalQuote (Lambda([ParamName sourceAlias],SqlColumnGet(sourceTi,sourceKey,_)))
                                      OptionalQuote (Lambda([ParamName destAlias],SqlColumnGet(_,destKey,_)))
                                      OptionalQuote projection ]) ->
                        let sourceTi,sourceKey =
                            match sourceKey, source.SqlExpression with
                            | GroupColumn(KeyOp "", KeyColumn "Key"), SelectMany(_,_,GroupQuery g,_) when (g.KeyColumns.Length = 1) ->
                                g.KeyColumns.[0]
                            | _ -> sourceTi, sourceKey
                        let destEntity, isOuter =
                            match dest.SqlExpression with
                            | BaseTable(_,destEntity) -> destEntity, false
                            | Projection(MethodCall(None, MethodWithName("Select"),  [_ ; OptionalQuote (Lambda(_,MethodCall(None, (MethodWithName "leftJoin"),_)))]),BaseTable(_,destEntity)) -> destEntity, true
                            | _ -> failwithf "Unexpected join destination entity expression (%A)." dest.SqlExpression
                        let sqlExpression =
                            match source.SqlExpression with
                            | BaseTable(alias,entity) when alias = "" ->
                                // special case here as above - this is the first call so replace the top of the tree here with the current base table alias and the select many
                                let data = { PrimaryKey = [destKey]; PrimaryTable = destEntity; ForeignKey = [sourceKey]; ForeignTable = entity; OuterJoin = isOuter; RelDirection = RelationshipDirection.Parents}
                                if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                                if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                                SelectMany(sourceAlias,destAlias, LinkQuery(data),BaseTable(sourceAlias,entity))
                            | _ ->
                                let sourceAlias = if sourceTi <> "" then Utilities.resolveTuplePropertyName sourceTi source.TupleIndex else sourceAlias
                                if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                                if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                                // we don't actually have the "foreign" table name here in a join as that information is "lost" further up the expression tree.
                                // it's ok though because it can always be resolved later after the whole expression tree has been evaluated
                                let data = { PrimaryKey = [destKey]; PrimaryTable = destEntity; ForeignKey = [sourceKey];
                                             ForeignTable = {Schema="";Name="";Type=""};
                                             OuterJoin = isOuter; RelDirection = RelationshipDirection.Parents }
                                SelectMany(sourceAlias,destAlias,LinkQuery(data),source.SqlExpression)

                        let ty =
                            match projection with
                                | :? LambdaExpression as meth -> typedefof<SqlQueryable<_>>.MakeGenericType(meth.ReturnType)
                                | _ -> failwith "unsupported projection in join"
                        ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; sqlExpression; source.TupleIndex; |] :?> IQueryable<_>
                    | MethodCall(None, (MethodWithName "Join"),
                                    [ SourceWithQueryData source;
                                      SourceWithQueryData dest
                                      OptionalQuote (Lambda([ParamName sourceAlias],TupleSqlColumnsGet(multisource)))
                                      OptionalQuote (Lambda([ParamName destAlias],TupleSqlColumnsGet(multidest)))
                                      OptionalQuote projection ]) ->
                        let destEntity, isOuter =
                            match dest.SqlExpression with
                            | BaseTable(_,destEntity) -> destEntity, false
                            | Projection(MethodCall(None, MethodWithName("Select"), [_ ;OptionalQuote (Lambda(_,MethodCall(None, (MethodWithName "leftJoin"),_)))]),BaseTable(_,destEntity)) -> destEntity, true
                            | _ -> failwithf "Unexpected join destination entity expression (%A)." dest.SqlExpression
                        let destKeys = multidest |> List.map(fun(_,dest,_)->dest)
                        let sourceKeys = multisource |> List.map(fun(_,source,_)->source)
                        let sqlExpression =
                            match source.SqlExpression with
                            | BaseTable(alias,entity) when alias = "" ->
                                // special case here as above - this is the first call so replace the top of the tree here with the current base table alias and the select many
                                let data = { PrimaryKey = destKeys; PrimaryTable = destEntity; ForeignKey = sourceKeys; ForeignTable = entity; OuterJoin = isOuter; RelDirection = RelationshipDirection.Parents}
                                if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                                if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                                SelectMany(sourceAlias,destAlias, LinkQuery(data),BaseTable(sourceAlias,entity))
                            | _ ->
                                let sourceTi = multisource |> List.tryPick(fun(ti,_,_)->match ti with "" -> None | x -> Some x)
                                let sourceAlias = match sourceTi with None -> sourceAlias | Some x -> Utilities.resolveTuplePropertyName x source.TupleIndex
                                if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                                if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                                // we don't actually have the "foreign" table name here in a join as that information is "lost" further up the expression tree.
                                // it's ok though because it can always be resolved later after the whole expression tree has been evaluated
                                let data = { PrimaryKey = destKeys; PrimaryTable = destEntity; ForeignKey = sourceKeys;
                                             ForeignTable = {Schema="";Name="";Type=""};
                                             OuterJoin = isOuter; RelDirection = RelationshipDirection.Parents }
                                SelectMany(sourceAlias,destAlias,LinkQuery(data),source.SqlExpression)

                        let ty =
                            match projection with
                                | :? LambdaExpression as meth -> typedefof<SqlQueryable<_>>.MakeGenericType(meth.ReturnType)
                                | _ -> failwith "unsupported projection in join"
                        ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; sqlExpression; source.TupleIndex; |] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "SelectMany"),
                                    [ SourceWithQueryData source;
                                      OptionalQuote (Lambda([_], inner ));
                                      OptionalQuote (Lambda(projectionParams,_) as projection)  ]) ->
                        let ty =
                            match projection with
                                | :? LambdaExpression as meth -> typedefof<SqlQueryable<_>>.MakeGenericType(meth.ReturnType)
                                | _ -> failwith "unsupported projection in select many"

                        let ex = processSelectManys projectionParams.[1].Name inner source.SqlExpression projectionParams source 
                        ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; ex; source.TupleIndex;|] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "Select"), [ SourceWithQueryData source; OptionalQuote (Lambda([ v1 ], _) as lambda) ]) as whole ->
                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType((lambda :?> LambdaExpression).ReturnType )
                        if v1.Name.StartsWith "_arg" && v1.Type <> typeof<SqlEntity> && not(v1.Type.Name.StartsWith("IGrouping")) then
                            // this is the projection from a join - ignore
                            // causing the ignore here will give us wrong return tyoe to deal with in convertExpression lambda handling
                            ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; source.SqlExpression; source.TupleIndex; |] :?> IQueryable<_>
                        else
                            ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; Projection(whole,source.SqlExpression); source.TupleIndex;|] :?> IQueryable<_>

                    | MethodCall(None,(MethodWithName("Union") | MethodWithName("Concat") | MethodWithName("Intersect") | MethodWithName("Except") as meth), [SourceWithQueryData source; SeqValuesQueryable values]) when (values :? IWithSqlService) -> 

                        let subquery = values :?> IWithSqlService
                        use con = subquery.Provider.CreateConnection(source.DataContext.ConnectionString)
                        let (query,parameters,projector,baseTable) = QueryExpressionTransformer.convertExpression subquery.SqlExpression subquery.TupleIndex con subquery.Provider false (source.DataContext.SqlOperationsInSelect=SelectOperations.DatabaseSide)

                        let ``nested param names`` = "@param" + abs(query.GetHashCode()).ToString() + "nested"

                        let modified = 
                            parameters
                            |> Seq.filter(fun p -> not(p.ParameterName.StartsWith ``nested param names``))
                            |> Seq.map(fun p ->
                                p.ParameterName <- p.ParameterName.Replace("@param", ``nested param names``)
                                p
                            ) |> Seq.toArray
                        let subquery = 
                            let paramfixed = query.Replace("@param", ``nested param names``)
                            match paramfixed.EndsWith(";") with
                            | false -> paramfixed
                            | true -> paramfixed.Substring(0, paramfixed.Length-1)

                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                        let utyp = 
                            match meth.Name with
                            | "Concat" -> UnionType.UnionAll
                            | "Union" -> UnionType.NormalUnion
                            | "Intersect" -> UnionType.Intersect
                            | "Except" -> UnionType.Except
                            | _ -> failwithf "Unsupported union type: %s" meth.Name
                        ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; Union(utyp,subquery,modified,source.SqlExpression) ; source.TupleIndex; |] :?> IQueryable<_>

                    | x -> failwith ("unrecognised method call " + x.ToString())

                member __.Execute(_: Expression) : obj =
                    failwith "Execute not implemented"

                member __.Execute<'T>(e: Expression) : 'T =
                    Common.QueryEvents.PublishExpression e
                    match e with
                    | MethodCall(_, (MethodWithName "First"), [Constant(query, _)]) ->
                        let svc = (query :?> IWithSqlService)
                        executeQuery svc.DataContext svc.Provider (Take(1,(svc.SqlExpression))) svc.TupleIndex
                        |> Seq.cast<'T>
                        |> Seq.head
                    | MethodCall(_, (MethodWithName "FirstOrDefault"), [ConstantOrNullableConstant(Some query)]) ->
                        let svc = (query :?> IWithSqlService)
                        executeQuery svc.DataContext svc.Provider (Take(1, svc.SqlExpression)) svc.TupleIndex
                        |> Seq.cast<'T>
                        |> Seq.tryFind (fun _ -> true)
                        |> Option.fold (fun _ x -> x) Unchecked.defaultof<'T>
                    | MethodCall(_, (MethodWithName "Single"), [Constant(query, _)]) ->
                        match (query :?> seq<_>) |> Seq.toList with
                        | x::[] -> x
                        | [] -> raise <| InvalidOperationException("Encountered zero elements in the input sequence")
                        | _ -> raise <| InvalidOperationException("Encountered more than one element in the input sequence")
                    | MethodCall(_, (MethodWithName "SingleOrDefault"), [ConstantOrNullableConstant(Some query)]) ->
                        match (query :?> seq<_>) |> Seq.toList with
                        | [] -> Unchecked.defaultof<'T>
                        | x::[] -> x
                        | _ -> raise <| InvalidOperationException("Encountered more than one element in the input sequence")
                    | MethodCall(None, (MethodWithName "Count"), [Constant(query, _)]) ->
                        let svc = (query :?> IWithSqlService)
                        let res = executeQueryScalar svc.DataContext svc.Provider (Count(svc.SqlExpression)) svc.TupleIndex 
                        if res = box(DBNull.Value) then Unchecked.defaultof<'T> else
                        (Utilities.convertTypes res typeof<'T>) :?> 'T
                    | MethodCall(None, (MethodWithName "Any" as meth), [ SourceWithQueryData source; OptionalQuote qual ]) ->
                        let limitedSource = 
                            {new IWithSqlService with 
                                member t.DataContext = source.DataContext
                                member t.SqlExpression = Take(1, source.SqlExpression) 
                                member t.Provider = source.Provider
                                member t.TupleIndex = source.TupleIndex }
                        let res = parseWhere meth limitedSource qual
                        res |> Seq.length > 0 |> box :?> 'T
                    | MethodCall(None, (MethodWithName "All" as meth), [ SourceWithQueryData source; OptionalQuote qual ]) ->
                        let negativeCheck = 
                            match qual with
                            | :? LambdaExpression as la -> Expression.Lambda(Expression.Not(la.Body), la.Parameters) :> Expression
                            | _ -> Expression.Not(qual) :> Expression

                        let limitedSource = 
                            {new IWithSqlService with 
                                member t.DataContext = source.DataContext
                                member t.SqlExpression = Take(1, source.SqlExpression) 
                                member t.Provider = source.Provider
                                member t.TupleIndex = source.TupleIndex }
                        
                        let res = parseWhere meth limitedSource negativeCheck
                        res |> Seq.length = 0 |> box :?> 'T
                    | MethodCall(None, (MethodWithName "First" as meth), [ SourceWithQueryData source; OptionalQuote qual ]) ->
                        let limitedSource = 
                            {new IWithSqlService with 
                                member t.DataContext = source.DataContext
                                member t.SqlExpression = Take(1, source.SqlExpression) 
                                member t.Provider = source.Provider
                                member t.TupleIndex = source.TupleIndex }
                        let res = parseWhere meth limitedSource qual
                        res |> Seq.head |> box :?> 'T
                    | MethodCall(None, (MethodWithName "Average" | MethodWithName "Avg" | MethodWithName "Sum" | MethodWithName "Max" | MethodWithName "Min"
                                         | MethodWithName "Avg"  | MethodWithName "StdDev" | MethodWithName "StDev" | MethodWithName "StandardDeviation"
                                         | MethodWithName "Variance" as meth), [SourceWithQueryData source; 
                             OptionalQuote (Lambda([ParamName param], OptionalConvertOrTypeAs(SqlColumnGet(entity, op,_)))) 
                             ]) ->

                        let key = Utilities.getBaseColumnName op

                        let alias =
                             match entity with
                             | "" when source.SqlExpression.HasAutoTupled() && source.TupleIndex.Contains param -> param
                             | "" -> ""
                             | _ -> resolveTuplePropertyName entity source.TupleIndex

                        let sqlExpression =

                               let opName = 
                                    match meth.Name with
                                    | "Sum" -> SumOp(key)
                                    | "Max" -> MaxOp(key)
                                    | "Count" -> CountOp(key)
                                    | "Min" -> MinOp(key)
                                    | "Average" | "Avg" -> AvgOp(key)
                                    | "StdDev" | "StDev" | "StandardDeviation" -> StdDevOp(key)
                                    | "Variance" -> VarianceOp(key)
                                    | _ -> failwithf "Unsupported aggregation `%s` in execution expression `%s`" meth.Name (e.ToString())

                               match source.SqlExpression with
                               | BaseTable("",entity)  -> AggregateOp("",GroupColumn(opName, op),BaseTable(alias,entity))
                               | _ ->  
                                    //let ex = processSelectManys param innerProjection x projectionParams source 
                                    AggregateOp(alias,GroupColumn(opName, op),source.SqlExpression)

                        let res = executeQueryScalar source.DataContext source.Provider sqlExpression source.TupleIndex 
                        if res = box(DBNull.Value) then Unchecked.defaultof<'T> else
                        (Utilities.convertTypes res typeof<'T>) :?> 'T
                    | MethodCall(None, (MethodWithName "Contains"), [SourceWithQueryData source; 
                             OptionalQuote(OptionalFSharpOptionValue(ConstantOrNullableConstant(c))) 
                             ]) ->
                             
                        let sqlExpression =
                            match source.SqlExpression with 
                            | Projection(MethodCall(None, _, [SourceWithQueryData source; OptionalQuote (Lambda([ParamName param], OptionalConvertOrTypeAs(SqlColumnGet(entity,key,_)))) ]),BaseTable(alias,entity2)) ->
                                Count(Take(1,(FilterClause(Condition.And([alias, key, ConditionOperator.Equal, c],None),source.SqlExpression))))
                            | Projection(MethodCall(None, _, [SourceWithQueryData source; OptionalQuote (Lambda([ParamName param], OptionalConvertOrTypeAs(SqlColumnGet(entity,key,_)))) ]), current) ->
                                Count(Take(1,(FilterClause(Condition.And(["", key, ConditionOperator.Equal, c],None),current))))
                            | others ->
                                failwithf "Unsupported execution of contains expression `%s`" (e.ToString())

                        let res = executeQueryScalar source.DataContext source.Provider sqlExpression source.TupleIndex 
                        if res = box(DBNull.Value) then Unchecked.defaultof<'T> else
                        (Utilities.convertTypes res typeof<'T>) :?> 'T
                    | MethodCall(_, (MethodWithName "ElementAt"), [SourceWithQueryData source; Int position ]) ->
                        let skips = position - 1
                        executeQuery source.DataContext source.Provider (Take(1,(Skip(skips,source.SqlExpression)))) source.TupleIndex
                        |> Seq.cast<'T>
                        |> Seq.head
                    | e -> failwithf "Unsupported execution expression `%s`" (e.ToString())  }


    let getAgg<'T when 'T : comparison> (agg:string) (s:Linq.IQueryable<'T>) : 'T =
        match s with
        | :? IWithSqlService as svc ->
            match svc.SqlExpression with
            | Projection(MethodCall(None, _, [SourceWithQueryData source; OptionalQuote (Lambda([ParamName param], OptionalConvertOrTypeAs(SqlColumnGet(entity,op,_)))) ]),_) ->
                    
                let key = Utilities.getBaseColumnName op

                let alias =
                        match entity with
                        | "" when source.SqlExpression.HasAutoTupled() -> param
                        | "" -> ""
                        | _ -> FSharp.Data.Sql.Common.Utilities.resolveTuplePropertyName entity source.TupleIndex
                let sqlExpression =

                    let opName = 
                        match agg with
                        | "Sum" -> SumOp(key)
                        | "Max" -> MaxOp(key)
                        | "Count" -> CountOp(key)
                        | "Min" -> MinOp(key)
                        | "Average" | "Avg" -> AvgOp(key)
                        | "StdDev" | "StDev" | "StandardDeviation" -> StdDevOp(key)
                        | "Variance" -> VarianceOp(key)
                        | _ -> failwithf "Unsupported aggregation `%s` in execution expression `%s`" agg (source.SqlExpression.ToString())

                    match source.SqlExpression with
                    | BaseTable("",entity)  -> AggregateOp("",GroupColumn(opName, op),BaseTable(alias,entity))
                    | x -> AggregateOp(alias,GroupColumn(opName, op),source.SqlExpression)

                let res = executeQueryScalar source.DataContext source.Provider sqlExpression source.TupleIndex 
                if res = box(DBNull.Value) then Unchecked.defaultof<'T> else
                (Utilities.convertTypes res typeof<'T>) |> unbox
            | _ -> failwithf "Not supported %s. You must have last a select clause to a single column to aggregate. %s" agg (svc.SqlExpression.ToString())
        | c -> failwithf "Supported only on SQLProvider dataase IQueryables. Was %s" (c.GetType().FullName)

module Seq =
    /// Execute SQLProvider query to get the sum of elements.
    let sumQuery<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> 'T  = QueryImplementation.getAgg "Sum"
    /// Execute SQLProvider query to get the max of elements.
    let maxQuery<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> 'T  = QueryImplementation.getAgg "Max"
    /// Execute SQLProvider query to get the min of elements.
    let minQuery<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> 'T  = QueryImplementation.getAgg "Min"
    /// Execute SQLProvider query to get the avg of elements.
    let averageQuery<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> 'T  = QueryImplementation.getAgg "Average"
    /// Execute SQLProvider query to get the standard deviation of elements.
    let stdDevQuery<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> 'T  = QueryImplementation.getAgg "StdDev"
    /// Execute SQLProvider query to get the variance of elements.
    let varianceQuery<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> 'T  = QueryImplementation.getAgg "Variance"
