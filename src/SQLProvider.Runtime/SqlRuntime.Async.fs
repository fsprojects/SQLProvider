namespace FSharp.Data.Sql

open FSharp.Data.Sql.Runtime
open System
open System.Collections.Generic
open QueryImplementation
open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Patterns
open System.Linq

module AsyncOperations =

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
            if enu.MoveNext() then
                let firstItem = enu.Current
                if isNull firstItem then return None
                else
                return Some (firstItem :?> 'T)
            else return None
        }

    let getHeadAsync (s:Linq.IQueryable<'T>) : System.Threading.Tasks.Task<'T> =
        task {
            let! h = getTryHeadAsync s
            match h with
            | Some x -> return x
            | None -> return raise (ArgumentException "The input sequence was empty.")
        }
    let inline private getExactlyOneAnd ([<InlineIfLambda>] onSuccess: 'TSource -> 'TTarget) ([<InlineIfLambda>] onTooMany: 'TSource -> 'TTarget) ([<InlineIfLambda>] onNone: unit -> 'TTarget) (s:Linq.IQueryable<'TSource>) =
        task {
            let! res = fetchTakeN 2 s
            let enu = res.GetEnumerator()
            if enu.MoveNext() then
                let firstItem = enu.Current
                if enu.MoveNext() then
                    return firstItem :?>'TSource |> onTooMany
                else 
                if isNull firstItem then
                    return onNone()
                else
                
                return firstItem :?>'TSource |> onSuccess

            else return onNone()
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
                if res = box(DBNull.Value) then return 0 else

                let t = (Utilities.convertTypes res typeof<int>)

                return t |> unbox
            | None, _ ->
                return s |> Seq.length
        }

    let getAggAsync<'T when 'T : comparison> (agg:string) (s:Linq.IQueryable<'T>) : System.Threading.Tasks.Task<'T> =

            match findSqlService s with
            | Some svc, wrapper ->

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
                    task {
                        let! res = executeQueryScalarAsync source.DataContext source.Provider sqlExpression source.TupleIndex
                        if res = box(DBNull.Value) then return Unchecked.defaultof<'T> else
                        return (Utilities.convertTypes res typeof<'T>) |> unbox
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
    let sumAsync<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> System.Threading.Tasks.Task<'T>  = getAggAsync "Sum"
    /// Execute SQLProvider query to get the max of elements, and release the OS thread while query is being executed.
    let maxAsync<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> System.Threading.Tasks.Task<'T>  = getAggAsync "Max"
    /// Execute SQLProvider query to get the min of elements, and release the OS thread while query is being executed.
    let minAsync<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> System.Threading.Tasks.Task<'T>  = getAggAsync "Min"
    /// Execute SQLProvider query to get the avg of elements, and release the OS thread while query is being executed.
    let averageAsync<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> System.Threading.Tasks.Task<'T>  = getAggAsync "Average"
    /// Execute SQLProvider query to get the standard deviation of elements, and release the OS thread while query is being executed.
    let stdDevAsync<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> System.Threading.Tasks.Task<'T>  = getAggAsync "StdDev"
    /// Execute SQLProvider query to get the variance of elements, and release the OS thread while query is being executed.
    let varianceAsync<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> System.Threading.Tasks.Task<'T>  = getAggAsync "Variance"
    /// WARNING! Execute SQLProvider DELETE FROM query to remove elements from the database.
    let ``delete all items from single table``<'T> : System.Linq.IQueryable<'T> -> System.Threading.Tasks.Task<int> = function
        | :? IWithSqlService as source ->
            task {
                let! res = executeDeleteQueryAsync source.DataContext source.Provider source.SqlExpression source.TupleIndex
                if res = box(DBNull.Value) then return Unchecked.defaultof<int> else
                return (Utilities.convertTypes res typeof<int>) |> unbox
            }
        | x -> failwithf "Only SQLProvider queryables accepted. Only simple single-table deletion where-clauses supported. Unsupported type %O" x
    /// Execute SQLProvider query to get the only element of the sequence.
    /// Throws `ArgumentNullException` if the seq is empty.
    /// Throws `ArgumentException` if the seq contains more than one element.
    let exactlyOneAsync<'T> : System.Linq.IQueryable<'T> -> System.Threading.Tasks.Task<'T> = getExactlyOneAsync
    /// Execute SQLProvider query to get the only element of the sequence.
    /// Returns `None` if there are zero or more than one element in the seq.
    let tryExactlyOneAsync<'T> : System.Linq.IQueryable<'T> -> System.Threading.Tasks.Task<'T option> = getTryExactlyOneAsync

module Array =
    /// Execute SQLProvider query and release the OS thread while query is being executed.
    let executeQueryAsync query = task { let! x = executeAsync query in return x |> Seq.toArray }

module List =
    /// Helper function to run async computation non-parallel style for list of objects.
    /// This is needed if async database opreation is executed for a list of entities.
    let evaluateOneByOne = Sql.evaluateOneByOne
    /// Execute SQLProvider query and release the OS thread while query is being executed.
    let executeQueryAsync query = task { let! x = executeAsync query in return x |> Seq.toList }
