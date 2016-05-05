namespace FSharp.Data.Sql

open FSharp.Data.Sql.Runtime

module Seq =
    let executeQueryAsync = QueryImplementation.executeAsync

module List =
    let executeQueryAsync = QueryImplementation.executeAsync

module Array =
    let executeQueryAsync = QueryImplementation.executeAsync
