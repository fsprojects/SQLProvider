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
    | Substring of SqlIntOrColumn
    | SubstringWithLength of SqlIntOrColumn*SqlIntOrColumn
    | ToUpper
    | ToLower
    | Trim
    | Length
    | Replace of SqlStringOrColumn*SqlStringOrColumn
    | IndexOf of SqlStringOrColumn
    | IndexOfStart of SqlStringOrColumn*SqlIntOrColumn
    // Date functions
    | Date
    | Year
    | Month
    | Day
    | Hour
    | Minute
    | Second
    | AddYears of SqlIntOrColumn
    | AddMonths of int
    | AddDays of SqlFloatOrColumn
    | AddHours of float
    | AddMinutes of SqlFloatOrColumn
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
    | GroupColumn of AggregateOperation * SqlColumnType

    and SqlStringOrColumn =
    | SqlStr of string
    | SqlStrCol of string*SqlColumnType //alias*column

    // More recursion, because you mighn want to say e.g.
    // where (x.Substring(x.IndexOf("."), (x.Length-x.IndexOf("."))
    and SqlIntOrColumn =
    | SqlInt of int
    | SqlIntCol of string*SqlColumnType //alias*column

    and SqlFloatOrColumn =
    | SqlFloat of float
    | SqlNumCol of string*SqlColumnType //alias*column

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

#if NETSTANDARD
// Hacks for .NET Core.
namespace FSharp.Data.Sql.Providers
type internal MSAccessProvider() = 
    member __.Note = "Not Supported in .NET core"
type internal OdbcProvider(quotehcar) =
    member __.Note = "Not Supported in .NET core"
#endif
