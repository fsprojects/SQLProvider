namespace FSharp.Data.Sql

open FSharp.Data.Sql.Runtime
open System
open System.Collections.Generic
open QueryImplementation
open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Patterns

module AsyncOperations =

    let executeAsync (s:Linq.IQueryable<'T>) =
        let yieldseq (en: IEnumerator<'T>) =
            seq {
                while en.MoveNext() do
                yield en.Current
            }
        async {
            match s with
            | :? IAsyncEnumerable<'T> as coll ->
                let! en = coll.GetAsyncEnumerator()
                return yieldseq en
            | c ->
                let en = c.GetEnumerator()
                return yieldseq en
        }

    let private fetchTakeOne (s:Linq.IQueryable<'T>) =
        async {
            match s with
            | :? IWithSqlService as svc ->
                return! executeQueryAsync svc.DataContext svc.Provider (Take(1, svc.SqlExpression)) svc.TupleIndex
            | c ->
                return c :> Collections.IEnumerable
        }

    let getHeadAsync (s:Linq.IQueryable<'T>) =
        async {
            let! res = fetchTakeOne s
            return res |> Seq.cast<'T> |> Seq.head
        }

    let getTryHeadAsync (s:Linq.IQueryable<'T>) =
        async {
            let! res = fetchTakeOne s
            return res |> Seq.cast<'T> |> Seq.tryPick Some
        }

    let getCountAsync (s:Linq.IQueryable<'T>) =
        async {
            match s with
            | :? IWithSqlService as svc ->
                let! res = executeQueryScalarAsync svc.DataContext svc.Provider (Count(svc.SqlExpression)) svc.TupleIndex
                if res = box(DBNull.Value) then return 0 else
                return (Utilities.convertTypes res typeof<int>) |> unbox
            | c ->
                return c |> Seq.length
        }

    let getAggAsync<'T when 'T : comparison> (agg:string) (s:Linq.IQueryable<'T>) : Async<'T> =
        async {
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

                    let! res = executeQueryScalarAsync source.DataContext source.Provider sqlExpression source.TupleIndex 
                    if res = box(DBNull.Value) then return Unchecked.defaultof<'T> else
                    return (Utilities.convertTypes res typeof<'T>) |> unbox
                | _ -> return failwithf "Not supported %s. You must have last a select clause to a single column to aggregate. %s" agg (svc.SqlExpression.ToString())
            | c -> return failwithf "Supported only on SQLProvider dataase IQueryables. Was %s" (c.GetType().FullName)
        }

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
    let sumAsync<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> Async<'T>  = getAggAsync "Sum"
    /// Execute SQLProvider query to get the max of elements, and release the OS thread while query is being executed.
    let maxAsync<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> Async<'T>  = getAggAsync "Max"
    /// Execute SQLProvider query to get the min of elements, and release the OS thread while query is being executed.
    let minAsync<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> Async<'T>  = getAggAsync "Min"
    /// Execute SQLProvider query to get the avg of elements, and release the OS thread while query is being executed.
    let averageAsync<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> Async<'T>  = getAggAsync "Average"
    /// Execute SQLProvider query to get the standard deviation of elements, and release the OS thread while query is being executed.
    let stdDevAsync<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> Async<'T>  = getAggAsync "StdDev"
    /// Execute SQLProvider query to get the variance of elements, and release the OS thread while query is being executed.
    let varianceAsync<'T when 'T : comparison> : System.Linq.IQueryable<'T> -> Async<'T>  = getAggAsync "Variance"
    /// WARNING! Execute SQLProvider DELETE FROM query to remove elements from the database.
    let ``delete all items from single table``<'T> : System.Linq.IQueryable<'T> -> Async<int> = function 
        | :? IWithSqlService as source ->
            async { 
                let! res = executeDeleteQueryAsync source.DataContext source.Provider source.SqlExpression source.TupleIndex 
                if res = box(DBNull.Value) then return Unchecked.defaultof<int> else
                return (Utilities.convertTypes res typeof<int>) |> unbox
            }
        | x -> failwithf "Only SQLProvider queryables accepted. Only simple single-table deletion where-clauses supported. Unsupported type %O" x

module Array =
    /// Execute SQLProvider query and release the OS thread while query is being executed.
    let executeQueryAsync query = async { let! x = executeAsync query in return x |> Seq.toArray }

module List =
    /// Helper function to run async computation non-parallel style for list of objects.
    /// This is needed if async database opreation is executed for a list of entities.
    let evaluateOneByOne = Sql.evaluateOneByOne 
    /// Execute SQLProvider query and release the OS thread while query is being executed.
    let executeQueryAsync query = async { let! x = executeAsync query in return x |> Seq.toList }
