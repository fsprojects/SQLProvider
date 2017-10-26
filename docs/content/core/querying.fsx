(*** hide ***)
#I "../../../bin/net451"
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
open System
(*** hide ***)
type CustomOrder = {OrderId: int64; Status: string; OrderRows: (int64*int64*int16)[]; Timezone: string}
(*** hide ***)
let parseTimezoneFunction(region:string,sdate:DateTime,customer:int) = ""
(**


## How to see the SQL-clause?

To display / debug your SQL-clauses you can add listener for your logging framework to SqlQueryEvent:
*)

FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %O")

(**

The event has separate fields of Command and Parameters 
for you to store your clauses with a strongly typed logging system like [Logary](https://github.com/logary/logary).

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

If you consider using asynchronous queries, read more from the [async documentation](async.html).


## Supported Query Expression Keywords

| Keyword            | Supported  |  Notes
| --------------------- |:-:|---------------------------------------|
.Contains()              |X | `open System.Linq`, in where, SQL IN-clause, nested query    | 
.Concat()                |X | `open System.Linq`, SQL UNION ALL-clause                     | 
.Union()                 |X | `open System.Linq`, SQL UNION-clause                         | 
all	                     |X |                                                       | 
averageBy                |X | Single table                                          | 
averageByNullable        |X | Single table                                          | 
contains                 |X |                                                       | 
count                    |X |                                                       | 
distinct                 |X |                                                       | 
exactlyOne               |X |                                                       | 
exactlyOneOrDefault      |X |                                                       | 
exists                   |X |                                                       | 
find                     |X |                                                       | 
groupBy                  |x | Initially only very simple groupBy (and having) is supported: On single-table with direct aggregates like `.Count()` or direct parameter calls like `.Sum(fun entity -> entity.UnitPrice)`. | 
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
maxBy                    |X | Single table                                          | 
maxByNullable            |X | Single table                                          | 
minBy                    |X | Single table                                          | 
minByNullable            |X | Single table                                          | 
nth                      |X |                                                       | 
select                   |X |                                                       | 
skip                     |X |                                                       | 
skipWhile                |  |                                                       | 
sortBy                   |X |                                                       | 
sortByDescending	     |X |                                                       | 
sortByNullable           |X |                                                       | 
sortByNullableDescending |X |                                                       | 
sumBy                    |X | Single table                                          | 
sumByNullable            |X | Single table                                          | 
take                     |X |                                                       | 
takeWhile                |  |                                                       | 
thenBy	                 |X |                                                       |     
thenByDescending	     |X |                                                       |   
thenByNullable           |X |                                                       | 
thenByNullableDescending |X |                                                       |
where                    |x | Server side variables must be plain without .NET operations or use the supported canonical functions. | 

### Canonical Functions 

Besides that, we support these .NET-functions to transfer the logics to SQL-clauses (starting from SQLProvider version 1.0.57).
If you use these, remember to check your database indexes.


#### .NET String Functions (.NET)

| .NET          | MsSqlServer     | PostgreSql   | MySql      | Oracle      | SQLite | MSAccess| Odbc        |  Notes
|---------------|-----------------|--------------|------------|-------------|--------|---------|-------------|--------------------------|
.Substring(x)   | SUBSTRING       | SUBSTRING    | MID        | SUBSTR      | SUBSTR | Mid     | SUBSTRING   | Start position may vary (0 or 1 based.) |
.ToUpper()      | UPPER           | UPPER        | UPPER      | UPPER       | UPPER  | UCase   | UCASE       |                 |
.ToLower()      | LOWER           | LOWER        | LOWER      | LOWER       | LOWER  | LCase   | LCASE       |                 |
.Trim()         | LTRIM(RTRIM)    | TRIM(BOTH...)| TRIM       | TRIM        | TRIM   | Trim    | LTRIM(RTRIM)|                 |
.Length()       | DATALENGTH      | CHAR_LENGTH  | CHAR_LENGTH| LENGTH      | LENGTH | Len     | CHARACTER_LENGTH |            |
.Replace(a,b)   | REPLACE         | REPLACE      | REPLACE    | REPLACE     | REPLACE| Replace | REPLACE     |                 |
.IndexOf(x)     | CHARINDEX       | STRPOS       | LOCATE     | INSTR       | INSTR  | InStr   | LOCATE      |                 |
.IndexOf(x, i)  | CHARINDEX       |              | LOCATE     | INSTR       |        | InStr   | LOCATE      |                 |
(+)             | `+`             | `||`         | CONCAT     | `||`        | `||`   | &       | CONCAT      |                 |

In where-clauses you can also use `.Contains("...")`, `.StartsWith("...")` and `.EndsWith("...")`, which are translated to 
corresponding `LIKE`-clauses (e.g. StartsWith("abc") is `LIKE ('asdf%')`.

`Substring(startpos,length)` is supported. IndexOf with length paramter is supported except PostgreSql and SQLite.

Operations do support parameters to be either constants or other SQL-columns (e.g. `x.Substring(x.Length() - 1)`).

 
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
.AddYears(i)   | DATEADD YEAR   | + INTERVAL| DATE_ADD | + INTERVAL| DATETIME| DateAdd   |            |   |
.AddMonths(i)  | DATEADD MONTH  | + INTERVAL| DATE_ADD | + INTERVAL| DATETIME| DateAdd   |            |   |
.AddDays(f)    | DATEADD DAY    | + INTERVAL| DATE_ADD | + INTERVAL| DATETIME| DateAdd   |            |   |
.AddHours(f)   | DATEADD HOUR   | + INTERVAL| DATE_ADD | + INTERVAL| DATETIME| DateAdd   |            |   |
.AddMinutes(f) | DATEADD MINUTE | + INTERVAL| DATE_ADD | + INTERVAL| DATETIME| DateAdd   |            |   |
.AddSeconds(f) | DATEADD SECOND | + INTERVAL| DATE_ADD | + INTERVAL| DATETIME| DateAdd   |            |   |

AddYears, AddDays and AddMinutes parameter can be either constant or other SQL-column, except in SQLite which supports only constant. 
AddMonths, AddHours and AddSeconds supports only constants for now. 
Odbc standard doesn't seem to have a date-add functionality.
.NET has float parameters on some time-functions like AddDays, but SQL may ignore the decimal fraction.


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

## More details

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

## Best practices working with queries

### When using Option types, check IsSome in where-clauses.

You may want to use F# Option to represent database null, with SQLProvider 
static constructor parameter `UseOptionTypes = true`.
Database null-checking is done with `x IS NULL`.
With option types, easiest way to do that is to check `IsSome` and `IsNone`:

```fsharp
let result =
    query {
        for order in ctx.Main.Orders do
        where (
            order.ShippedDate.IsSome && 
            order.ShippedDate.Value.Year = 2015)
        select (order.OrderId, order.Freight)
    } |> Array.executeQueryAsync
```

### Using booleans and simple variables (from outside a scope) in where-clauses

This is how you make your code easier to read when you have multiple code paths.
SQLProvider will optimize the SQL-clause before sending it to the database,
so it will still be simple.

Consider how clean is this source-code compared to other with similar logic:

*)
open System.Linq
let getOrders(futureOrders:bool, shipYears:int list) =

    let today = DateTime.UtcNow.Date
    let pastOrders = not futureOrders
    let noYearFilter = shipYears.IsEmpty

    let result =
        query {
            for order in ctx.Main.Orders do
            where ( 
                (noYearFilter || shipYears.Contains(order.ShippedDate.Year))
                &&
                ((futureOrders && order.OrderDate > today) ||
                 (pastOrders   && order.OrderDate <= today))
            ) 
            select (order.OrderId, order.Freight)
        } |> Array.executeQueryAsync
    result

(**


### Don't select all the fields if you don't need them

In general you should select only columns you need 
and not a whole object if you don't update its fields.

*)

// Select all the fields from a table, basically:
// SELECT TOP 1 Address, City, CompanyName, 
//      ContactName, ContactTitle, Country, 
//      CustomerID, Fax, Phone, PostalCode, 
//      Region FROM main.Customers
let selectFullObject =
    query {
        for customer in ctx.Main.Customers do
        select customer
    } |> Seq.tryHeadAsync

// Select only two fields, basically:
// SELECT TOP 1 Address, City FROM main.Customers
let selectSmallObject =
    query {
        for customer in ctx.Main.Customers do
        select (customer.Address, customer.City)
    } |> Seq.tryHeadAsync

(**

If you still want the whole objects and return those to a client 
as untyped records, you can use `ColumnValues |> Map.ofSeq`:

*)

let someQuery =
    query {
        for customer in ctx.Main.Customers do
        //where(...)
        select customer
    } |> Seq.toArray

someQuery |> Array.map(fun c -> c.ColumnValues |> Map.ofSeq)

(**

F# Map values are accessed like this: `myItem.["City"]`


### Using code logic in select-clause

Don't be scared to insert non-Sql syntax to select-clauses.
They will be parsed business-logic side!

*)

let fetchOrders customerZone =
    let currentDate = DateTime.UtcNow.Date
    query {
        for order in ctx.Main.Orders do
        // where(...)
        select {
            OrderId = order.OrderId
            Timezone = 
                parseTimezoneFunction(order.ShipRegion, order.ShippedDate, customerZone);
            Status = 
                if order.ShippedDate > currentDate then "Shipped"
                elif order.OrderDate > currentDate then "Ordered"
                elif order.RequiredDate > currentDate then "Late"
                else "Waiting"
            OrderRows = [||];
        }
    } |> Seq.toArray

(**

You can't have a `let` inside a select, but you can have custom function calls like
`parseTimezoneFunction` here. Just be careful, they are executed for each result item separately.
So if you want also SQL to execute there, it's rather better to do a separate function taking
a collection as parameter. See below.


### Using one sub-query to populate items

Sometimes you want to fetch efficiently sub-items, like
"Give me all orders with their order-rows"

In the previous example we fetched OrderRows as empty array.
Now we populate those with one query in immutable way:

*)

let orders = fetchOrders 123

let orderIds = 
    orders 
    |> Array.map(fun o -> o.OrderId) 
    |> Array.distinct
    
// Fetch all rows with one query
let subItems =
    query {
        for row in ctx.Main.OrderDetails do
        where (orderIds.Contains(row.OrderId))
        select (row.OrderId, row.ProductId, row.Quantity)
    } |> Seq.toArray
    
let ordersWithDetails =
    orders 
    |> Array.map(fun order ->
        {order with 
            // Match the corresponding sub items
            // to a parent item's colleciton:
            OrderRows = 
                subItems 
                |> Array.filter(fun (orderId,_,_) -> 
                    order.OrderId = orderId)
                })

(**

### How to deal with large IN-queries?

The pervious query had `orderIds.Contains(row.OrderId)`.
Which is fine if your collection has 50 items but what if there are 5000 orderIds?
SQL-IN will fail. You have two easy options to deal with that.

#### Chunk your collection:

F# has built-in chunkBySize function!

*)

let chunked = orderIds |> Array.chunkBySize 100

for chunk in chunked do
    let all =
        query {
            for row in ctx.Main.OrderDetails do
            where (chunk.Contains(row.OrderId))
            select (row)
        } |> Seq.toArray

    all |> Array.iter(fun row -> row.Discount <- 0.1)
    ctx.SubmitUpdates()

(**

#### Creating a nested query

By leaving the last `|> Seq.toArray` away from your main query you create a lazy
`IQueryable<...>`-query. Which means your IN-objects are not fetched from
the database, but is actually a nested query.

*)
let nestedOrders =
    query {
        for order in ctx.Main.Orders do
        // where(...)
        select (order.OrderId)
    } 

let subItemsAll =
    query {
        for row in ctx.Main.OrderDetails do
        where (nestedOrders.Contains(row.OrderId))
        select (row.OrderId, row.ProductId, row.Quantity)
    } |> Seq.toArray

// similar as previous fetchOrders
let fetchOrders2 customerZone =
    let currentDate = DateTime.UtcNow.Date
    query {
        for order in ctx.Main.Orders do
        // where(...)
        select {
            OrderId = order.OrderId
            Timezone = 
                parseTimezoneFunction(order.ShipRegion, order.ShippedDate, customerZone);
            Status = 
                if order.ShippedDate > currentDate then "Shipped"
                elif order.OrderDate > currentDate then "Ordered"
                elif order.RequiredDate > currentDate then "Late"
                else "Waiting"
            OrderRows = 
                subItemsAll |> (Array.filter(fun (orderId,_,_) -> 
                    order.OrderId = orderId));
        }
    } |> Seq.toArray

(**

That way order hit count doesn't matter as the database is taking care of it.

### Group-by and more complex query scenarios

One problem with SQLProvider is that monitorin the SQL-clause performance hitting to
database indexes is hard to track. So **the best way to handle complex SQL
is to create a database view and query that from SQLProvider**.

Still, if you want to use LINQ groupBy, this is how it's done:

*)

let freightsByCity =
    query {
        for o in ctx.Main.Orders do
        //where (...)
        groupBy o.ShipCity into cites
        select (cites.Key, cites.Sum(fun order -> order.Freight))
    } |> Array.executeQueryAsync
