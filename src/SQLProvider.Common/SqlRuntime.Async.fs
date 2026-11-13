namespace FSharp.Data.Sql

open FSharp.Data.Sql.Runtime
open System
open System.Collections.Generic
open QueryImplementation
open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Patterns
open System.Linq
open System.Threading.Tasks

/// Provides asynchronous operations for SQL queries.
/// Use these functions when you need to execute queries asynchronously to avoid blocking threads.
module AsyncOperations =

    /// Executes a queryable asynchronously and returns the results as a sequence.
    /// This is useful for executing large queries without blocking the calling thread.
    /// param s: The queryable to execute
    /// returns: A task that yields a sequence of results
    let executeAsync (s:Linq.IQueryable<'T>) =
        let inline yieldseq (en: IEnumerator<'T>) =
            seq {
                while en.MoveNext() do
                yield en.Current
            }
        task {
            match s with
            | :? IAsyncEnumerable<'T> as coll ->
                let! en = coll.GetAsyncEnumerator()
                return yieldseq en
            | c ->
                let en = c.GetEnumerator()
                let source =
                    match en.GetType() with
                    | null -> null
                    | x -> x.GetField("source", System.Reflection.BindingFlags.NonPublic ||| System.Reflection.BindingFlags.Instance)
                if isNull source then
                    return yieldseq en
                else
                    match source.GetValue en with
                    | :? IAsyncEnumerable as coll ->
                        do! coll.EvaluateQuery()
                        return yieldseq en
                    | c -> return yieldseq en
        }

    let inline private fetchTakeN (n: int) (s:Linq.IQueryable<'T>) =
        match findSqlService s with
        | Some svc, wrapper ->
            match wrapper with
            | None -> executeQueryAsync svc.DataContext svc.Provider (Take(n, svc.SqlExpression)) svc.TupleIndex
            | Some sourceRepl ->
                task {
                    let! res = executeQueryAsync svc.DataContext svc.Provider (Take(n, svc.SqlExpression)) svc.TupleIndex
                    sourceRepl res
                    return s.Take n :> Collections.IEnumerable
                }

        | None, _ ->
            task { return s.Take n :> Collections.IEnumerable }

    let private fetchTakeOne (s:Linq.IQueryable<'T>) =
        fetchTakeN 1 s

    let getTryHeadAsync (s:Linq.IQueryable<'T>) =
        task {
            let! res = fetchTakeOne s
            let enu = res.GetEnumerator()
            return
                if enu.MoveNext() then
                    let firstItem = enu.Current
                    if isNull firstItem then None
                    else
                    Some (firstItem :?> 'T)
                else None
        }

    let getHeadAsync (s:Linq.IQueryable<'T>) : Task<'T> =
        task {
            match! getTryHeadAsync s with
            | Some x -> return x
            | None -> return raise (ArgumentException "The input sequence was empty.")
        }
    let inline private getExactlyOneAnd ([<InlineIfLambda>] onSuccess: 'TSource -> 'TTarget) ([<InlineIfLambda>] onTooMany: 'TSource -> 'TTarget) ([<InlineIfLambda>] onNone: unit -> 'TTarget) (s:Linq.IQueryable<'TSource>) =
        task {
            let! res = fetchTakeN 2 s
            let enu = res.GetEnumerator()
            return
                if enu.MoveNext() then
                    let firstItem = enu.Current
                    if enu.MoveNext() then
                        firstItem :?>'TSource |> onTooMany
                    elif isNull firstItem then
                        onNone()
                    else

                    firstItem :?>'TSource |> onSuccess

                else onNone()
        }

    let getExactlyOneAsync (s:Linq.IQueryable<'T>)=
        getExactlyOneAnd
            id
            (fun _ -> raise (ArgumentException("The seq contains more than one element.")))
            (fun _ -> raise (ArgumentNullException("The seq is empty.")))
            s

    let getTryExactlyOneAsync (s:Linq.IQueryable<'T>) =
        getExactlyOneAnd
            Some
            (fun _ -> None)
            (fun _ -> None)
            s

    let getCountAsync (s:Linq.IQueryable<'T>) =
        task {
            match findSqlService s with
            | Some svc, wrapper ->
                let! res = executeQueryScalarAsync svc.DataContext svc.Provider (Count(svc.SqlExpression)) svc.TupleIndex
                return
                    if res = box DBNull.Value then 0 else

                    let t = (Utilities.convertTypes res typeof<int>)

                    t |> unbox
            | None, _ ->
                return s |> Seq.length
        }

    let getAggAsync<'T when 'T : comparison> (agg:string) (s:Linq.IQueryable<'T>) : Task<'T> =

            match findSqlService s with
            | Some svc, wrapper ->

                match svc.SqlExpression with
                | Projection(MethodCall(None, _, [SourceWithQueryData source; OptionalQuote (Lambda([ParamName param], OptionalConvertOrTypeAs(SqlColumnGet(entity,op,_)))) ]),_) ->

                    let key = Utilities.getBaseColumnName op

                    let alias =
                            match entity with
                            | "" when source.SqlExpression.HasAutoTupled() -> param
                            | "" -> ""
                            | _ ->
                                let al = Utilities.resolveTuplePropertyName entity source.TupleIndex
                                if al.StartsWith "_" then al.TrimStart [|'_'|] else al
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
                    task {
                        let! res = executeQueryScalarAsync source.DataContext source.Provider sqlExpression source.TupleIndex
                        return
                            if res = box DBNull.Value then Unchecked.defaultof<'T> else
                            (Utilities.convertTypes res typeof<'T>) |> unbox
                    }
                | _ -> failwithf "Not supported %s. You must have last a select clause to a single column to aggregate. %s" agg (svc.SqlExpression.ToString())
            | None, _ -> failwithf "Supported only on SQLProvider database IQueryables. Was %s" (s.GetType().FullName)

open AsyncOperations

module Seq =
    /// Execute SQLProvider query and release the OS thread while query is being executed.
    let executeQueryAsync = executeAsync
    /// Execute SQLProvider query to count the elements, and release the OS thread while query is being executed.
    let lengthAsync = getCountAsync
    /// Execute SQLProvider query to take one result and release the OS thread while query is being executed.
    /// Like normal head: Throws exception if no elements exists. See also tryHeadAsync.
    let headAsync = getHeadAsync
    /// Execute SQLProvider query to take one result and release the OS thread while query is being executed.
    /// Returns None if no elements exists.
    let tryHeadAsync = getTryHeadAsync
    /// Execute SQLProvider query to get the sum of elements, and release the OS thread while query is being executed.
    let sumAsync<'T when 'T : comparison> : IQueryable<'T> -> Task<'T>  = getAggAsync "Sum"
    /// Execute SQLProvider query to get the max of elements, and release the OS thread while query is being executed.
    let maxAsync<'T when 'T : comparison> : IQueryable<'T> -> Task<'T>  = getAggAsync "Max"
    /// Execute SQLProvider query to get the min of elements, and release the OS thread while query is being executed.
    let minAsync<'T when 'T : comparison> : IQueryable<'T> -> Task<'T>  = getAggAsync "Min"
    /// Execute SQLProvider query to get the avg of elements, and release the OS thread while query is being executed.
    let averageAsync<'T when 'T : comparison> : IQueryable<'T> -> Task<'T>  = getAggAsync "Average"
    /// Execute SQLProvider query to get the standard deviation of elements, and release the OS thread while query is being executed.
    let stdDevAsync<'T when 'T : comparison> : IQueryable<'T> -> Task<'T>  = getAggAsync "StdDev"
    /// Execute SQLProvider query to get the variance of elements, and release the OS thread while query is being executed.
    let varianceAsync<'T when 'T : comparison> : IQueryable<'T> -> Task<'T>  = getAggAsync "Variance"
    /// <summary>Marks a query to lock the rows it reads until the transaction ends, against lost updates: another
    /// transaction that locks, changes or deletes one of them waits until this one commits or rolls back.</summary>
    /// <param name="query">A SQLProvider query of a table's rows, joined ones too; not distinct, grouped, counted,
    /// aggregated or a union, and not a part of another query (a sub-query, a join's table) or a delete.</param>
    /// <returns>The query, locking its rows when any executor runs it. A queryable that isn't a database query, such as a
    /// mock context's, comes back as it is; a SQLProvider query behind an in-memory step (<c>Seq.map</c>, then
    /// <c>AsQueryable</c>) is refused, as its rows couldn't be locked.</returns>
    /// <remarks>Run it inside a transaction: a <c>TransactionScope</c>, with <c>TransactionScopeAsyncFlowOption.Enabled</c>
    /// in async code. Outside one the lock ends with the query. PostgreSQL, MySQL and Oracle write <c>FOR UPDATE</c>
    /// (PostgreSQL not the rows of a left-outer-joined table; Oracle not with skip, nor with take or head after a
    /// sort), Firebird <c>FOR UPDATE WITH LOCK</c> (one table, no joins), SQL Server the <c>UPDLOCK</c> and <c>ROWLOCK</c>
    /// table hints; SQLite, MS Access, DuckDB and ODBC have no row locks and refuse it. See "Locking rows" in the
    /// documentation.</remarks>
    /// <example><code>
    /// use scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled)
    /// let! order = query { for o in ctx.Main.Orders do where (o.OrderId = id); select o } |> Seq.lockForUpdateHeadAsync
    /// order.Freight &lt;- order.Freight + 1m
    /// do! ctx.SubmitUpdatesAsync()
    /// scope.Complete()
    /// </code></example>
    let lockForUpdate<'T> (query: IQueryable<'T>) : IQueryable<'T> =
        match query with
        | :? IWithSqlService as source ->
            Activator.CreateInstance(
                query.GetType(),
                [| box source.DataContext; box source.Provider; box (ForUpdate source.SqlExpression); box source.TupleIndex |]
            ) :?> IQueryable<'T>
        | _ ->
            match findSqlService query with
            | Some _, _ -> failwith "Seq.lockForUpdate takes the SQLProvider query itself: lock it before Seq.map or another in-memory step."
            | None, _ -> query
    /// <summary>Runs a query with its rows locked until the transaction ends (see <c>lockForUpdate</c>), releasing the OS
    /// thread while the database works.</summary>
    /// <param name="query">The query, as <c>lockForUpdate</c> takes it.</param>
    /// <returns>The rows read, each locked.</returns>
    let lockForUpdateAsync<'T> (query: IQueryable<'T>) = executeAsync (lockForUpdate query)
    /// <summary>Reads the query's first row, locked until the transaction ends (see <c>lockForUpdate</c>), releasing the OS
    /// thread while the database works.</summary>
    /// <param name="query">The query, as <c>lockForUpdate</c> takes it.</param>
    /// <returns>The row, locked. Fails when the query reads no rows; see <c>lockForUpdateTryHeadAsync</c>.</returns>
    /// <remarks>Oracle refuses it after a sort.</remarks>
    let lockForUpdateHeadAsync<'T> (query: IQueryable<'T>) = getHeadAsync (lockForUpdate query)
    /// <summary>Reads the query's first row, if any, locked until the transaction ends (see <c>lockForUpdate</c>),
    /// releasing the OS thread while the database works.</summary>
    /// <param name="query">The query, as <c>lockForUpdate</c> takes it.</param>
    /// <returns>The row, locked, or <c>None</c> when the query reads no rows.</returns>
    /// <remarks>Oracle refuses it after a sort.</remarks>
    let lockForUpdateTryHeadAsync<'T> (query: IQueryable<'T>) = getTryHeadAsync (lockForUpdate query)
    /// WARNING! Execute SQLProvider DELETE FROM query to remove elements from the database.
    let ``delete all items from single table``<'T> : IQueryable<'T> -> Task<int> = function
        | :? IWithSqlService as source ->
            if source.DataContext.IsReadOnly then failwith "Context is readonly" else
            task {
                let! res = executeDeleteQueryAsync source.DataContext source.Provider source.SqlExpression source.TupleIndex
                return
                    if res = box DBNull.Value then Unchecked.defaultof<int> else
                    (Utilities.convertTypes res typeof<int>) |> unbox
            }
        | x -> failwithf "Only SQLProvider queryables accepted. Only simple single-table deletion where-clauses supported. Unsupported type %O" x
    /// Execute SQLProvider query to get the only element of the sequence.
    /// Throws `ArgumentNullException` if the seq is empty.
    /// Throws `ArgumentException` if the seq contains more than one element.
    let exactlyOneAsync<'T> : IQueryable<'T> -> Task<'T> = getExactlyOneAsync
    /// Execute SQLProvider query to get the only element of the sequence.
    /// Returns `None` if there are zero or more than one element in the seq.
    let tryExactlyOneAsync<'T> : IQueryable<'T> -> Task<'T option> = getTryExactlyOneAsync

module Array =
    /// Execute SQLProvider query and release the OS thread while query is being executed.
    let executeQueryAsync query = task { let! x = executeAsync query in return x |> Seq.toArray }
    /// <summary>Runs a query with its rows locked until the transaction ends (see <c>Seq.lockForUpdate</c>), releasing the
    /// OS thread while the database works.</summary>
    /// <param name="query">The query, as <c>Seq.lockForUpdate</c> takes it.</param>
    /// <returns>The rows read, each locked.</returns>
    let lockForUpdateAsync query = task { let! x = executeAsync (Seq.lockForUpdate query) in return x |> Seq.toArray }

module List =
    /// Helper function to run async computation non-parallel style for list of objects.
    /// This is needed if async database opreation is executed for a list of entities.
    let evaluateOneByOne = Sql.evaluateOneByOne
    /// Execute SQLProvider query and release the OS thread while query is being executed.
    let executeQueryAsync query = task { let! x = executeAsync query in return x |> Seq.toList }
    /// <summary>Runs a query with its rows locked until the transaction ends (see <c>Seq.lockForUpdate</c>), releasing the
    /// OS thread while the database works.</summary>
    /// <param name="query">The query, as <c>Seq.lockForUpdate</c> takes it.</param>
    /// <returns>The rows read, each locked.</returns>
    let lockForUpdateAsync query = task { let! x = executeAsync (Seq.lockForUpdate query) in return x |> Seq.toList }
