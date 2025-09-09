namespace FSharp.Data.Sql

open System.Linq

/// <summary>
/// Represents SQL conditional operators used in WHERE clauses and query expressions.
/// These operators are translated to their SQL equivalents when building database queries.
/// </summary>
[<Struct>]
type ConditionOperator = 
    /// SQL LIKE operator for pattern matching with wildcards
    | Like
    /// SQL NOT LIKE operator for negated pattern matching
    | NotLike
    /// SQL equality operator (=)
    | Equal
    /// SQL inequality operator (<>)
    | NotEqual
    /// SQL greater than operator (>)
    | GreaterThan
    /// SQL less than operator (<)
    | LessThan
    /// SQL greater than or equal operator (>=)
    | GreaterEqual
    /// SQL less than or equal operator (<=)
    | LessEqual
    /// SQL IS NULL operator for checking null values
    | IsNull
    /// SQL IS NOT NULL operator for checking non-null values
    | NotNull
    /// SQL IN operator for membership testing
    | In
    /// SQL NOT IN operator for negated membership testing
    | NotIn
    /// SQL IN operator for nested subqueries
    | NestedIn
    /// SQL NOT IN operator for negated nested subqueries
    | NestedNotIn
    /// SQL EXISTS operator for subquery existence testing
    | NestedExists
    /// SQL NOT EXISTS operator for negated subquery existence testing
    | NestedNotExists
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
        | NestedNotIn   -> "NOT IN"
        | NestedExists  -> "EXISTS"
        | NestedNotExists  -> "NOT EXISTS"

/// <summary>
/// Represents SQL aggregate operations for GROUP BY clauses and data summarization.
/// Each operation includes the column name to aggregate.
/// </summary>
[<Struct>]
type AggregateOperation = // Aggregate (column name if not default)
/// Groups results by the specified key column
| KeyOp of key: string
/// Finds the maximum value in the specified column
| MaxOp of max: string
/// Finds the minimum value in the specified column
| MinOp of min: string
/// Calculates the sum of values in the specified column
| SumOp of sum: string
/// Calculates the average of values in the specified column
| AvgOp of avg: string
/// Counts the number of rows in the specified column
| CountOp of count: string
/// Counts the number of distinct values in the specified column
| CountDistOp of countDist: string
/// Calculates the standard deviation of values in the specified column
| StdDevOp of std: string
/// Calculates the variance of values in the specified column
| VarianceOp of var: string

/// <summary>
/// Specifies where SELECT operations should be executed for query optimization.
/// </summary>
type SelectOperations =
/// Execute the operation on the .NET client side
| DotNetSide = 0
/// Execute the operation on the database server side
| DatabaseSide = 1 

[<AutoOpenAttribute>]
module ColumnSchema =

    type alias = string
    /// <summary>
    /// Represents WHERE clause conditions for SQL queries.
    /// Supports complex boolean expressions with AND/OR operators and nested conditions.
    /// </summary>
    type Condition =
        // this is  (table alias * column name * operator * right hand value ) list  * (the same again list)
        // basically any AND or OR expression can have N terms and can have N nested condition children
        // this is largely from my CRM type provider. I don't think in practice for the SQL provider
        // you will ever have more than what is representable in a traditional binary expression tree, but
        // changing it would be a lot of effort ;)
        /// AND condition with a list of terms and optional nested conditions
        | And of (alias * SqlColumnType * ConditionOperator * obj option) list * (Condition list) option
        /// OR condition with a list of terms and optional nested conditions
        | Or of (alias * SqlColumnType * ConditionOperator * obj option) list * (Condition list) option
        /// Always evaluates to true
        | ConstantTrue
        /// Always evaluates to false
        | ConstantFalse
        /// Expression that cannot be translated to SQL
        | NotSupported of System.Linq.Expressions.Expression

    /// <summary>
    /// Represents SQL functions and operations that can be applied to columns.
    /// These operations are translated to database-specific SQL functions.
    /// </summary>
    and CanonicalOp =
    //String functions
    /// Extracts a substring from the specified position to the end
    | Substring of SqlItemOrColumn
    /// Extracts a substring with specified start position and length
    | SubstringWithLength of SqlItemOrColumn*SqlItemOrColumn
    /// Converts string to uppercase
    | ToUpper
    /// Converts string to lowercase
    | ToLower
    /// Removes leading and trailing whitespace
    | Trim
    /// Returns the length of a string
    | Length
    /// Replaces occurrences of a substring with another substring
    | Replace of SqlItemOrColumn*SqlItemOrColumn
    /// Returns the index of the first occurrence of a substring
    | IndexOf of SqlItemOrColumn
    /// Returns the index of a substring starting from a specified position
    | IndexOfStart of SqlItemOrColumn*SqlItemOrColumn
    // Date functions
    /// Extracts the date part from a datetime value
    | Date
    /// Extracts the year from a datetime value
    | Year
    /// Extracts the month from a datetime value
    | Month
    /// Extracts the day from a datetime value
    | Day
    /// Extracts the hour from a datetime value
    | Hour
    /// Extracts the minute from a datetime value
    | Minute
    /// Extracts the second from a datetime value
    | Second
    /// Adds the specified number of years to a datetime value
    | AddYears of SqlItemOrColumn
    /// Adds the specified number of months to a datetime value
    | AddMonths of int
    /// Adds the specified number of days to a datetime value
    | AddDays of SqlItemOrColumn
    /// Adds the specified number of hours to a datetime value
    | AddHours of float
    /// Adds the specified number of minutes to a datetime value
    | AddMinutes of SqlItemOrColumn
    /// Adds the specified number of seconds to a datetime value
    | AddSeconds of float
    /// Calculates the difference in days between two datetime values
    | DateDiffDays of SqlItemOrColumn
    /// Calculates the difference in seconds between two datetime values
    | DateDiffSecs of SqlItemOrColumn

    // Numerical functions
    /// Returns the absolute value of a number
    | Abs
    /// Returns the smallest integer greater than or equal to a number (ceiling)
    | Ceil
    /// Returns the largest integer less than or equal to a number (floor)
    | Floor
    /// Rounds a number to the nearest integer
    | Round
    /// Rounds a number to the specified number of decimal places
    | RoundDecimals of int
    /// Truncates a number to its integer part
    | Truncate
    /// Returns the square root of a number
    | Sqrt
    /// Returns the sine of an angle (in radians)
    | Sin
    /// Returns the cosine of an angle (in radians)
    | Cos
    /// Returns the tangent of an angle (in radians)
    | Tan
    /// Returns the arcsine of a number
    | ASin
    /// Returns the arccosine of a number
    | ACos
    /// Returns the arctangent of a number
    | ATan
    /// Raises a number to the power of another number
    | Pow of SqlItemOrColumn
    /// Raises a number to a constant power
    | PowConst of SqlItemOrColumn
    /// Returns the greatest value among the provided arguments
    | Greatest of SqlItemOrColumn
    /// Returns the smallest value among the provided arguments
    | Least of SqlItemOrColumn
    // Other
    /// Basic mathematical operation with a constant value
    | BasicMath of string*obj //operation, constant
    /// Left-side basic mathematical operation with a constant value
    | BasicMathLeft of string*obj //operation, constant
    /// Basic mathematical operation between columns
    | BasicMathOfColumns of string*string*SqlColumnType //operation, alias, column
    /// SQL CASE expression with condition and false value
    | CaseSql of Condition * SqlItemOrColumn // operation, if-false
    /// SQL CASE expression with negated condition and true value
    | CaseNotSql of Condition * SqlItemOrColumn // operation, if-true
    /// SQL CASE expression with condition and two constant values
    | CaseSqlPlain of Condition * obj * obj // with 2 constants
    /// Casts a value to VARCHAR type
    | CastVarchar
    /// Casts a value to INTEGER type
    | CastInt

    /// <summary>
    /// Represents a column with optional operations applied to it.
    /// </summary>
    and SqlColumnType =
    /// A key column used for grouping or joining
    | KeyColumn of string
    /// A column with a canonical operation applied
    | CanonicalOperation of CanonicalOp * SqlColumnType
    /// A column used in aggregate operations
    | GroupColumn of AggregateOperation * SqlColumnType

    /// <summary>
    /// Represents either a database column or a constant value in SQL expressions.
    /// </summary>
    // More recursion, because you mighn want to say e.g.
    // where (x.Substring(x.IndexOf("."), (x.Length-x.IndexOf("."))
    and SqlItemOrColumn =
    /// A database column with table alias and column type
    | SqlCol of string*SqlColumnType //alias*column
    /// A constant value to be used in SQL expressions
    | SqlConstant of obj

    /// <summary>
    /// Represents parameters for SQL projections (SELECT clause elements).
    /// </summary>
    type ProjectionParameter =
    /// A direct entity column reference
    | EntityColumn of string
    /// A column with operations applied for computed expressions
    | OperationColumn of string*SqlColumnType//name*operations

// Dummy operators, these are placeholders that are replaced in the expression tree traversal with special server-side operations such as In, Like
// The operators here are used to force the compiler to statically check against the correct types
/// <summary>
/// Contains custom SQL operators for use in query expressions.
/// These operators are translated to their SQL equivalents during query compilation.
/// </summary>
[<AutoOpenAttribute>]
module Operators =
    /// <summary>
    /// SQL IN operator. Tests if a value exists in a sequence.
    /// Example: where (customer.Country |=| ["USA"; "Canada"])
    /// </summary>
    /// <param name="a">The value to test</param>
    /// <param name="b">The sequence to search in</param>
    /// <returns>Always false at compile time - replaced with SQL IN during query execution</returns>
    let (|=|) (a:'a) (b:'a seq) = false
    
    /// <summary>
    /// SQL NOT IN operator. Tests if a value does not exist in a sequence.
    /// Example: where (customer.Country |<>| ["USA"; "Canada"])
    /// </summary>
    /// <param name="a">The value to test</param>
    /// <param name="b">The sequence to search in</param>
    /// <returns>Always false at compile time - replaced with SQL NOT IN during query execution</returns>
    let (|<>|) (a:'a) (b:'a seq) = false
    
    /// <summary>
    /// SQL LIKE operator for pattern matching with wildcards.
    /// Example: where (customer.Name =% "John%")
    /// </summary>
    /// <param name="a">The value to test</param>
    /// <param name="b">The pattern to match (use % and _ as wildcards)</param>
    /// <returns>Always false at compile time - replaced with SQL LIKE during query execution</returns>
    let (=%) (a:'a) (b:string) = false
    
    /// <summary>
    /// SQL NOT LIKE operator for negated pattern matching.
    /// Example: where (customer.Name <>% "John%")
    /// </summary>
    /// <param name="a">The value to test</param>
    /// <param name="b">The pattern to reject</param>
    /// <returns>Always false at compile time - replaced with SQL NOT LIKE during query execution</returns>
    let (<>%) (a:'a) (b:string) = false
    
    /// Left join helper function for internal use
    let private leftJoin (a:'a) = a
    
    /// <summary>
    /// Left outer join operator. Performs a left join on a related table.
    /// Example: for customer in ctx.Main.Customers do
    ///          for order in (!!) customer.``main.Orders by CustomerID`` do
    /// </summary>
    /// <param name="a">The related table queryable</param>
    /// <returns>A queryable that performs a left outer join</returns>
    let (!!) (a:IQueryable<'a>) = query { for x in a do select (leftJoin x) } 
    
    /// <summary>
    /// Calculates the standard deviation of numeric values in a column.
    /// Used in aggregate queries with groupBy.
    /// </summary>
    /// <param name="a">The column value</param>
    /// <returns>Always 1m at compile time - replaced with SQL STDDEV during query execution</returns>
    let StdDev (a:'a) = 1m

    /// <summary>
    /// Calculates the variance of numeric values in a column.
    /// Used in aggregate queries with groupBy.
    /// </summary>
    /// <param name="a">The column value</param>
    /// <returns>Always 1m at compile time - replaced with SQL VARIANCE during query execution</returns>
    let Variance (a:'a) = 1m
