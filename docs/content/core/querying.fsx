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
