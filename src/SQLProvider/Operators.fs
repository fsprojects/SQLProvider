namespace FSharp.Data.Sql

open System.Linq

type ConditionOperator = 
    | Like
    | NotLike
    | Equal
    | NotEqual
    | GreaterThan
    | LessThan
    | GreaterEqual
    | LessEqual
    | IsNull
    | NotNull
    | In
    | NotIn
    with 
    override x.ToString() =
        // NOTE: these are MS SQL Server textual representations of the operators.
        // other providers may need to provide their own versions.
        match x with 
        | Like          -> "LIKE"
        | NotLike       -> "NOT LIKE"
        | Equal         -> "="
        | NotEqual      -> "<>"
        | GreaterThan   -> ">"
        | LessThan      -> "<"
        | GreaterEqual  -> ">="
        | LessEqual     -> "<="
        | IsNull        -> "IS NULL"
        | NotNull       -> "IS NOT NULL"
        | In            -> "IN"
        | NotIn         -> "NOT IN"

// Dummy operators, these are placeholders that are replaced in the expression tree traversal with special server-side operations such as In, Like
// The operators here are used to force the compiler to statically check against the correct types
[<AutoOpenAttribute>]
module Operators =
    /// In
    let (|=|) (a:'a) (b:'a seq) = false
    // Not In
    let (|<>|) (a:'a) (b:'a seq) = false
    // Like
    let (=%) (a:'a) (b:string) = false
    // Not Like
    let (<>%) (a:'a) (b:string) = false
    // Left join
    let (!!) (a:IQueryable<_>) = a