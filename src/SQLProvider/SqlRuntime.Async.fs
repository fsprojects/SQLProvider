namespace FSharp.Data.Sql

open FSharp.Data.Sql.Runtime


module Seq =
    /// Execute SQLProvider query and release the OS thread while query is being executed.
    let executeQueryAsync = QueryImplementation.executeAsync

module Array =
    /// Execute SQLProvider query and release the OS thread while query is being executed.
    let executeQueryAsync = QueryImplementation.executeAsync

module List =
    /// Execute SQLProvider query and release the OS thread while query is being executed.
    let executeQueryAsync = QueryImplementation.executeAsync

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
