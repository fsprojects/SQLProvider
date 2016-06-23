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
groupBy                  |  |                                                       | 
groupJoin                |  |                                                       | 
groupValBy	             |  |                                                       | 
head                     |X |                                                       | 
headOrDefault            |X |                                                       | 
if                       |X |                                                       |
join                     |X |                                                       | 
last                     |  |                                                       | 
lastOrDefault            |  |                                                       | 
leftOuterJoin            |  |                                                       | 
let                      |  |                                                       |
maxBy                    |X |                                                       | 
maxByNullable            |X |                                                       | 
minBy                    |X |                                                       | 
minByNullable            |X |                                                       | 
nth                      |  |                                                       | 
select                   |X |                                                       | 
skip                     |X |Broken on SQLServer when combined with sortByDescending + take     | 
skipWhile                |  |                                                       | 
sortBy                   |X |                                                       | 
sortByDescending	     |X |Broken on SQLServer when combined with skip+take       | 
sortByNullable           |X |                                                       | 
sortByNullableDescending |X |                                                       | 
sumBy                    |X |                                                       | 
sumByNullable            |X |                                                       | 
take                     |X |Broken on SQLServer when combined with skip+sortByDescending      | 
takeWhile                |  |                                                       | 
thenBy	                 |X |                                                       |     
thenByDescending	     |X |                                                       |   
thenByNullable           |X |                                                       | 
thenByNullableDescending |X |                                                       |
where                    |X | Server side variables must be on left side and only left side of predicates  | 

*)

(**
## Expressions

These operators perform no specific function in the code itself, rather they
are placeholders replaced by their database-specific server-side operations.
Their utility is in forcing the compiler to check against the correct types.

*)

let bergs = ctx.Main.Customers.Individuals.BERGS


(**
### Operators

* `|=|` (In set)
* `|<>|` (Not in set)
* `=%` (Like)
* `<>%` (Not like)
* `!!` (Left join)
*)
