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
| StdDevOp of string
| VarianceOp of string

type SelectOperations =
| DotNetSide = 0
| DatabaseSide = 1 

[<AutoOpenAttribute>]
module ColumnSchema =

    type alias = string
    type Condition =
        // this is  (table alias * column name * operator * right hand value ) list  * (the same again list)
        // basically any AND or OR expression can have N terms and can have N nested condition children
        // this is largely from my CRM type provider. I don't think in practice for the SQL provider
        // you will ever have more than what is representable in a traditional binary expression tree, but
        // changing it would be a lot of effort ;)
        | And of (alias * SqlColumnType * ConditionOperator * obj option) list * (Condition list) option
        | Or of (alias * SqlColumnType * ConditionOperator * obj option) list * (Condition list) option
        | ConstantTrue
        | ConstantFalse
        | NotSupported of System.Linq.Expressions.Expression

    and CanonicalOp =
    //String functions
    | Substring of SqlItemOrColumn
    | SubstringWithLength of SqlItemOrColumn*SqlItemOrColumn
    | ToUpper
    | ToLower
    | Trim
    | Length
    | Replace of SqlItemOrColumn*SqlItemOrColumn
    | IndexOf of SqlItemOrColumn
    | IndexOfStart of SqlItemOrColumn*SqlItemOrColumn
    // Date functions
    | Date
    | Year
    | Month
    | Day
    | Hour
    | Minute
    | Second
    | AddYears of SqlItemOrColumn
    | AddMonths of int
    | AddDays of SqlItemOrColumn
    | AddHours of float
    | AddMinutes of SqlItemOrColumn
    | AddSeconds of float
    | DateDiffDays of SqlItemOrColumn
    | DateDiffSecs of SqlItemOrColumn

    // Numerical functions
    | Abs
    | Ceil
    | Floor
    | Round
    | RoundDecimals of int
    | Truncate
    | Sqrt
    | Sin
    | Cos
    | Tan
    | ASin
    | ACos
    | ATan
    | Greatest of SqlItemOrColumn
    | Least of SqlItemOrColumn
    // Other
    | BasicMath of string*obj //operation, constant
    | BasicMathLeft of string*obj //operation, constant
    | BasicMathOfColumns of string*string*SqlColumnType //operation, alias, column
    | CaseSql of Condition * SqlItemOrColumn // operation, if-false
    | CaseNotSql of Condition * SqlItemOrColumn // operation, if-true
    | CaseSqlPlain of Condition * obj * obj // with 2 constants
    | CastVarchar


    and SqlColumnType =
    | KeyColumn of string
    | CanonicalOperation of CanonicalOp * SqlColumnType
    | GroupColumn of AggregateOperation * SqlColumnType

    // More recursion, because you mighn want to say e.g.
    // where (x.Substring(x.IndexOf("."), (x.Length-x.IndexOf("."))
    and SqlItemOrColumn =
    | SqlCol of string*SqlColumnType //alias*column
    | SqlConstant of obj

    type ProjectionParameter =
    | EntityColumn of string
    | OperationColumn of string*SqlColumnType//name*operations

// Dummy operators, these are placeholders that are replaced in the expression tree traversal with special server-side operations such as In, Like
// The operators here are used to force the compiler to statically check against the correct types
[<AutoOpenAttribute>]
module Operators =
    //// In
    let (|=|) (a:'a) (b:'a seq) = false
    /// Not In
    let (|<>|) (a:'a) (b:'a seq) = false
    /// Like
    let (=%) (a:'a) (b:string) = false
    /// Not Like
    let (<>%) (a:'a) (b:string) = false
    /// Left join
    let (!!) (a:IQueryable<_>) = a
    
    /// Standard Deviation
    let StdDev (a:'a) = 1m

    /// Variance
    let Variance (a:'a) = 1m

#if NETSTANDARD
// Hacks for .NET Core.
namespace FSharp.Data.Sql.Providers
type internal MSAccessProvider() = 
    member __.Note = "Not Supported in .NET core"
type internal OdbcProvider(quotehcar) =
    member __.Note = "Not Supported in .NET core"
#endif
