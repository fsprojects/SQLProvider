(*** hide ***)
#I "../../../bin"
(*** hide ***)
[<Literal>]
let connectionString =
    "Data Source=" + __SOURCE_DIRECTORY__ + @"/../../../tests/SqlProvider.Tests/scripts/northwindEF.db;Version=3"
(*** hide ***)
[<Literal>]
let resolutionPath =
    __SOURCE_DIRECTORY__ + @"/../../../tests/SqlProvider.Tests/libs"
#r "FSharp.Data.SqlProvider.dll"
open FSharp.Data.Sql
(**


# Querying
*)
type sql  = SqlDataProvider<
                Common.DatabaseProviderTypes.SQLITE,
                connectionString,
                ResolutionPath = resolutionPath, 
                CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL
            >
let ctx = sql.GetDataContext()


(**
SQLProvider leverages F#'s `query {}` expression syntax to perform queries
against the database.  Though many are supported, not all LINQ expressions are.
*)

let example =
    query {
        for order in ctx.Main.Orders do
        where (order.Freight > 0m)
        sortBy (order.ShipPostalCode)
        skip 3
        take 4
        select (order)
    }

let test = example |> Seq.toArray |> Array.map(fun i -> i.ColumnValues |> Map.ofSeq)

let item =
    query {
        for order in ctx.Main.Orders do
        where (order.Freight > 0m)
        head
    }

(**

Or async versions:
*)

let exampleAsync =
    async {
        let! res =
            query {
                for order in ctx.Main.Orders do
                where (order.Freight > 0m)
                select (order)
            } |> Seq.executeQueryAsync
        return res
    } |> Async.StartAsTask

let itemAsync =
    async {
        let! item =
            query {
                for order in ctx.Main.Orders do
                where (order.Freight > 0m)
            } |> Seq.headAsync
        return item
    } |> Async.StartAsTask

(**

## Supported Query Expression Keywords

| Keyword            | Supported  |  Notes
| --------------------- |:-:|---------------------------------------|
.Contains()              |X | `open System.Linq`, in where, SQL IN-clause, nested query    | 
.Concat()                |X | `open System.Linq`, SQL UNION ALL-clause                     | 
.Union()                 |X | `open System.Linq`, SQL UNION-clause                         | 
all	                     |X |                                                       | 
averageBy                |X |                                                       | 
averageByNullable        |X |                                                       | 
contains                 |X |                                                       | 
count                    |X |                                                       | 
distinct                 |X |                                                       | 
exactlyOne               |X |                                                       | 
exactlyOneOrDefault      |X |                                                       | 
exists                   |X |                                                       | 
find                     |X |                                                       | 
groupBy                  |x | Initially only very simple groupBy (and having) is supported: On single-table with single-key-column and direct aggregates like `.Count()` or direct parameter calls like `.Sum(fun entity -> entity.UnitPrice)`. | 
groupJoin                |  |                                                       | 
groupValBy	             |  |                                                       | 
head                     |X |                                                       | 
headOrDefault            |X |                                                       | 
if                       |X |                                                       |
join                     |X |                                                       | 
last                     |  |                                                       | 
lastOrDefault            |  |                                                       | 
leftOuterJoin            |  |                                                       | 
let                      |x | ...but not using tmp variables in where-clauses       |
maxBy                    |X |                                                       | 
maxByNullable            |X |                                                       | 
minBy                    |X |                                                       | 
minByNullable            |X |                                                       | 
nth                      |X |                                                       | 
select                   |X |                                                       | 
skip                     |X |                                                       | 
skipWhile                |  |                                                       | 
sortBy                   |X |                                                       | 
sortByDescending	     |X |                                                       | 
sortByNullable           |X |                                                       | 
sortByNullableDescending |X |                                                       | 
sumBy                    |X |                                                       | 
sumByNullable            |X |                                                       | 
take                     |X |                                                       | 
takeWhile                |  |                                                       | 
thenBy	                 |X |                                                       |     
thenByDescending	     |X |                                                       |   
thenByNullable           |X |                                                       | 
thenByNullableDescending |X |                                                       |
where                    |x | Server side variables must be plain without .NET operations, so you can't say where (col.Days(+1)>2)  | 

### Canonical Functions 

Besides that, we support these .NET-functions to to transfer to SQL-clauses:

#### .NET String Functions (.NET)

| .NET          | MsSqlServer     | PostgreSql   | MySql      | Oracle      | SQLite | MSAccess| Odbc        |  Notes
|---------------|-----------------|--------------|------------|-------------|--------|---------|-------------|--------------------------|
.Substring(x)   | SUBSTRING       | SUBSTRING    | MID        | SUBSTR      | SUBSTR | Mid     | SUBSTRING   | Start position may vary. |
.Substring(x, y)| SUBSTRING       | SUBSTRING    | MID        | SUBSTR      | SUBSTR | MID     | SUBSTRING   | (0 or 1 based.) |
.ToUpper()      | UPPER           | UPPER        | UPPER      | UPPER       | UPPER  | UCase   | UCASE       |                 |
.ToLower()      | LOWER           | LOWER        | LOWER      | LOWER       | LOWER  | LCase   | LCASE       |                 |
.Trim()         | LTRIM(RTRIM)    | TRIM(BOTH...)| TRIM       | LTRIM(RTRIM)| TRIM   | Trim    | LTRIM(RTRIM)|                 |
.Length()       | DATALENGTH      | CHAR_LENGTH  | CHAR_LENGTH| LENGTH      | LENGTH | Len     | CHARACTER_LENGTH |            |
.Replace(a, b)  | REPLACE         | REPLACE      | REPLACE    | REPLACE     | REPLACE| Replace | REPLACE     |                 |
.IndexOf(x)     | CHARINDEX       | STRPOS       | LOCATE     | INSTR       | INSTR  | InStr   | LOCATE      |                 |
.IndexOf(x, i)  | CHARINDEX       |              | LOCATE     | INSTR       |        | InStr   | LOCATE      |                 |
(+)             | `+`             | `||`         | CONCAT     | `||`        | `||`   | &       | CONCAT      |                 |

In where-clauses you can also use `.Contains()`, `.StartsWith()` and `.EndsWith()`, which are translated to 
corresponding `LIKE`-clauses (e.g. StartsWith("abc") is `LIKE ('asdf%')`

#### .NET DateTime Functions

| .NET         | MsSqlServer    | PostgreSql| MySql    | Oracle    | SQLite  | MSAccess  | Odbc      |  Notes
|--------------|----------------|-----------|----------|-----------|---------|-----------|-----------|--------------------------|
.Date          | CAST(AS DATE)  | DATE_TRUNC| DATE     | TRUNC     | STRFTIME| DateValue(Format)| CONVERT(SQL_DATE)  |   |
.Year          | YEAR           | DATE_PART | YEAR     | EXTRACT   | STRFTIME| Year      | YEAR       |   |
.Month         | MONTH          | DATE_PART | MONTH    | EXTRACT   | STRFTIME| Month     | MONTH      |   |
.Day           | DAY            | DATE_PART | DAY      | EXTRACT   | STRFTIME| Day       | DAYOFMONTH |   |
.Hour          | DATEPART HOUR  | DATE_PART | HOUR     | EXTRACT   | STRFTIME| Hour      | HOUR       |   |
.Minute        | DATEPART MINUTE| DATE_PART | MINUTE   | EXTRACT   | STRFTIME| Minute    | MINUTE     |   |
.Second        | DATEPART SECOND| DATE_PART | SECOND   | EXTRACT   | STRFTIME| Second    | SECOND     |   |
.AddYears(i)   | DATEADD YEAR   | + INTERVAL| DATE_ADD | + INTERVAL| DATE    | DateAdd   |            |   |
.AddMonths(i)  | DATEADD MONTH  | + INTERVAL| DATE_ADD | + INTERVAL| DATE    | DateAdd   |            |   |
.AddDays(f)    | DATEADD DAY    | + INTERVAL| DATE_ADD | + INTERVAL| DATE    | DateAdd   |            | .NET has float, bus SQL may ignore decimal fraction |
.AddHours(f)   | DATEADD HOUR   | + INTERVAL| DATE_ADD | + INTERVAL| DATE    | DateAdd   |            |   |
.AddMinutes(f) | DATEADD MINUTE | + INTERVAL| DATE_ADD | + INTERVAL| DATE    | DateAdd   |            |   |
.AddSeconds(f) | DATEADD SECOND | + INTERVAL| DATE_ADD | + INTERVAL| DATE    | DateAdd   |            |   |

#### Numerical Functions (e.g. Microsoft.FSharp.Core.Operators)

| .NET          | MsSqlServer| PostgreSql| MySql   | Oracle| SQLite     | MSAccess| Odbc    |  Notes
|---------------|------------|-----------|---------|-------|------------|---------|---------|--------------------------|
abs(i)          | ABS        | ABS       | ABS     | ABS   | ABS        | Abs     | ABS     |   |
ceil(i)         | CEILING    | CEILING   | CEILING | CEIL  | CAST + 0.5 | Fix+1   | CEILING |   |
floor(i)        | FLOOR      | FLOOR     | FLOOR   | FLOOR | CAST AS INT| Int     | FLOOR   |   |
round(i)        | ROUND      | ROUND     | ROUND   | ROUND | ROUND      | Round   | ROUND   |   |
Math.Round(i,x) | ROUND      | ROUND     | ROUND   | ROUND | ROUND      | Round   | ROUND   |   |
truncate(i)     | TRUNCATE   | TRUNC     | TRUNCATE| TRUNC |            | Fix     | TRUNCATE|   |
(+)             | +          | +         | +       | +     | +          | +       | +       |   |
(-)             | -          | -         | -       | -     | -          | -       | -       |   |
(*)             | *          | *         | *       | *     | *          | *       | *       |   |
(/)             | /          | /         | /       | /     | /          | /       | /       |   |
(%)             | %          | %         | %       | %     | %          | %       | %       |   |

#### Aggregate Functions 

Also you can use these on group-by clause:
`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`

*)

(**
To debug your SQL-clauses you can add listener for your logging framework to SqlQueryEvent:
*)

FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %s")

(**

By default `query { ... }` is `IQueryable<T>` which is lazy. To execute the query you have to do `Seq.toList`, `Seq.toArray`, or some corresponding operation. If you don't do that but just continue inside another `query { ... }` or use System.Linq `.Where(...)` etc, that will still be combined to the same SQL-query.

There are some limitation of complexity of your queries, but for example
this is still ok and will give you very simple select-clause:

*)

let randomBoolean = 
    let r = System.Random()
    fun () -> r.NextDouble() > 0.5
let c1 = randomBoolean()
let c2 = randomBoolean()
let c3 = randomBoolean()

let sample =
    query {
        for order in ctx.Main.Orders do
        where ((c1 || order.Freight > 0m) && c2)
        let x = "Region: " + order.ShipAddress
        select (x, if c3 then order.ShipCountry else order.ShipRegion)
    } |> Seq.toArray

(**
It can be for example (but it can also leave [Freight]-condition away and select ShipRegion instead of ShipAddress, depending on your randon values):

```sql
    SELECT 
        [_arg2].[ShipAddress] as 'ShipAddress',
        [_arg2].[ShipCountry] as 'ShipCountry' 
    FROM main.Orders as [_arg2] 
    WHERE (([_arg2].[Freight]> @param1)) 
```

## Expressions

These operators perform no specific function in the code itself, rather they
are placeholders replaced by their database-specific server-side operations.
Their utility is in forcing the compiler to check against the correct types.

*)

let bergs = ctx.Main.Customers.Individuals.BERGS


(**
### Operators

You can find some custom operators `using FSharp.Data.Sql`:

* `|=|` (In set)
* `|<>|` (Not in set)
* `=%` (Like)
* `<>%` (Not like)
* `!!` (Left join)
*)
