namespace FSharp.Data.Sql

open FSharp.Data.Sql.Runtime
open System
open System.Collections.Generic
open QueryImplementation
open FSharp.Data.Sql.Common

module AsyncOperations =

    let executeAsync (s:Linq.IQueryable<'T>) =
        let yieldseq (en: IEnumerator<'T>) =
            seq {
                while en.MoveNext() do
                yield en.Current
            }
        async {
            match s with
            | :? SqlQueryable<'T> as coll ->
                let! en = coll.GetAsyncEnumerator()
                return yieldseq en
            | :? SqlOrderedQueryable<'T> as coll ->
                let! en = coll.GetAsyncEnumerator()
                return yieldseq en
            | c ->
                let en = c.GetEnumerator()
                return yieldseq en
        }

    let private fetchTakeOne (s:Linq.IQueryable<'T>) =
        async {
            match s with
            | :? SqlQueryable<'T> as coll ->
                let svc = (coll :> IWithSqlService)
                return! executeQueryAsync svc.DataContext svc.Provider (Take(1, svc.SqlExpression)) svc.TupleIndex
            | :? SqlOrderedQueryable<'T> as coll ->
                let svc = (coll :> IWithSqlService)
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
            | :? SqlQueryable<'T> as coll ->
                let svc = (coll :> IWithSqlService)
                let! res = executeQueryScalarAsync svc.DataContext svc.Provider (Count(svc.SqlExpression)) svc.TupleIndex
                return res |> unbox
            | :? SqlOrderedQueryable<'T> as coll ->
                let svc = (coll :> IWithSqlService)
                let! res = executeQueryScalarAsync svc.DataContext svc.Provider (Count(svc.SqlExpression)) svc.TupleIndex
                return res |> unbox
            | c ->
                return c |> Seq.length
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

module List =
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
