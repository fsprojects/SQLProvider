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
    | NestedIn
    | NestedNotIn
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
        | NestedIn      -> "IN"
        | NotIn         -> "NOT IN"
        | NestedNotIn      -> "NOT IN"

type AggregateOperation = // Aggregate (column name if not default)
| KeyOp of string
| MaxOp of string
| MinOp of string
| SumOp of string
| AvgOp of string
| CountOp of string

[<AutoOpenAttribute>]
module ColumnSchema =

    type CanonicalOp =
    //String functions
    | Substring of int
    | SubstringWithLength of int*int
    | ToUpper
    | ToLower
    | Trim
    | Length
    | Replace of string*string
    | IndexOf of string
    | IndexOfStart of string*int
    | IndexOfColumn of string*SqlColumnType //alias, column
    // Date functions
    | Date
    | Year
    | Month
    | Day
    | Hour
    | Minute
    | Second
    | AddYears of int
    | AddMonths of int
    | AddDays of float
    | AddHours of float
    | AddMinutes of float
    | AddSeconds of float
    // Numerical functions
    | Abs
    | Ceil
    | Floor
    | Round
    | RoundDecimals of int
    | Truncate
    // Other
    | BasicMath of string*obj //operation, constant
    | BasicMathOfColumns of string*string*SqlColumnType //operation, alias, column

    and SqlColumnType =
    | KeyColumn of string
    | CanonicalOperation of CanonicalOp * SqlColumnType
    | GroupColumn of AggregateOperation

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