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
                return (Utilities.convertTypes res typeof<'T>) |> unbox
            | c ->
                return c |> Seq.length
        }

    let getAggAsync<'T when 'T : comparison> (agg:string) (s:Linq.IQueryable<'T>) : Async<'T> =
        async {
            match s with
            | :? IWithSqlService as svc ->
                match svc.SqlExpression with
                | Projection(MethodCall(None, _, [SourceWithQueryData source; OptionalQuote (Lambda([ParamName param], SqlColumnGet(entity,key,_))) ]),_) ->
                    let alias =
                            match entity with
                            | "" when source.SqlExpression.HasAutoTupled() -> param
                            | "" -> ""
                            | _ -> FSharp.Data.Sql.Common.Utilities.resolveTuplePropertyName entity source.TupleIndex
                    let sqlExpression =
                            match agg, source.SqlExpression with
                            | "Sum", BaseTable("",entity)  -> AggregateOp(Sum(None),"",key,BaseTable(alias,entity))
                            | "Sum", _ ->  AggregateOp(Sum(None),alias,key,source.SqlExpression)
                            | "Max", BaseTable("",entity)  -> AggregateOp(Max(None),"",key,BaseTable(alias,entity))
                            | "Max", _ ->  AggregateOp(Max(None),alias,key,source.SqlExpression)
                            | "Min", BaseTable("",entity)  -> AggregateOp(Min(None),"",key,BaseTable(alias,entity))
                            | "Min", _ ->  AggregateOp(Min(None),alias,key,source.SqlExpression)
                            | "Average", BaseTable("",entity)  -> AggregateOp(Avg(None),"",key,BaseTable(alias,entity))
                            | "Average", _ ->  AggregateOp(Avg(None),alias,key,source.SqlExpression)
                            | _ -> failwithf "Unsupported aggregation `%s` in execution expression `%s`" agg (source.SqlExpression.ToString())
                    let! res = executeQueryScalarAsync source.DataContext source.Provider sqlExpression source.TupleIndex 
                    return (Utilities.convertTypes res typeof<'T>) |> unbox
                | _ -> return failwithf "Not supported %s. You must have last a select clause to a single column to aggregate. %s" agg (svc.SqlExpression.ToString())
            | c -> return failwithf "Supported only on SQLProvider dataase IQueryables"
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

module Array =
    /// Execute SQLProvider query and release the OS thread while query is being executed.
    let executeQueryAsync query = async { let! x = executeAsync query in return x |> Seq.toArray }

module List =
    /// Execute SQLProvider query and release the OS thread while query is being executed.
    let executeQueryAsync query = async { let! x = executeAsync query in return x |> Seq.toList }
    /// Helper function to run async computation non-parallel style for list of objects.
    /// This is needed if async database opreation is executed for a list of entities.
    let evaluateOneByOne asyncFunc entityList =
        let rec executeOneByOne' asyncFunc entityList acc =
            match entityList with
            | [] -> async { return acc }
            | h::t -> 
                async {
                    let! res = asyncFunc h
                    return! executeOneByOne' asyncFunc t (res::acc)
                }
        executeOneByOne' asyncFunc entityList []
