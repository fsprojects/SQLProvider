(*** hide ***)
#I "../../../bin/net451"
(*** hide ***)
[<Literal>]
let connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"/../../../tests/SqlProvider.Tests/scripts/northwindEF.db;Version=3"
(*** hide ***)
[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + @"/../../../tests/SqlProvider.Tests/libs"
(*** hide ***)
open System.Linq
#r "FSharp.Data.SqlProvider.dll"
open FSharp.Data.Sql
(*** hide ***)
type sql  = SqlDataProvider<
                Common.DatabaseProviderTypes.SQLITE,
                connectionString,
                ResolutionPath = resolutionPath,
                CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL
            >
(*** hide ***)
let ctx = sql.GetDataContext()
(**
# Composable Queries

## Definition of Composable Queries
Basicly composable methods are those that you can chain together to build the desired functionality out of smaller parts.
By passing functions as parameters you are able to generate higher-order query operations.
Therefore composable query means that you can do logics to compose just one database-SQL-query from multiple queryables.
By using composable queries you can shorten the Database transactions and keep the connection open a minimum of time.

## Generate composable queries by using Linq IQueryable

With composable queries you can combine two queries in multiple ways, and one query can be used as the building block for the other query.
To see how this works, let’s look at a simple query:
*)

let query1 =
    query {
      for customers  in ctx.Main.Customers do
      where (customers.ContactTitle = "USA")
      select (customers)}

(**
The variable that is returned from the query is sometimes called a computation. If you write evaluate
(e.g. a foreach loop or `|> Seq.toList`) and display the address field from the customers returned by this
computation, you see the following output:

```
GREAL|Great Lakes Food Market|Howard Snyder|Marketing Manager|2732 Baker Blvd.|Eugene|OR|97403|USA|(503) 555-7555|
HUNGC|Hungry Coyote Import Store|Yoshi Latimer|Sales Representative|City Center Plaza 516 Main St.|Elgin|OR|97827|USA|(503) 555-6874|(503) 555-2376
...
TRAIH|Trail's Head Gourmet Provisioners|Helvetius Nagy|Sales Associate|722 DaVinci Blvd.|Kirkland|WA|98034|USA|(206) 555-8257|(206) 555-2174
WHITC|White Clover Markets|Karl Jablonski|Owner|305 - 14th Ave. S. Suite 3B|Seattle|WA|98128|USA|(206) 555-4112|(206) 555-4115
```

You can now write a second query against the result of this query:
*)

let query2 =
    query {
      for customers in query1 do
      where (customers.CompanyName = "The Big Cheese")
      select customers}
    |> Seq.toArray

(**
Notice that the last word in the first line of this query is the computation returned from the previous query.
This second query produces the following output:

```
THEBI|The Big Cheese|Liz Nixon|Marketing Manager|89 Jefferson Way Suite 2|Portland|OR|97201|USA|(503) 555-3612|
```

SQLProvider to Objects queries are composable because they operate on and usually return variables of type `IQueryable<T>`.
In other words, SQLProvider queries typically follow this pattern:

*)

let (qry:IQueryable<'T>) =
    query {
        //for is like C# foreach
        for x in (xs:IQueryable<'T>) do
        select x
    }

//
(**
This is a simple mechanism to understand, but it yields powerful results.
It allows you to take complex problems, break them into manageable pieces, and solve them with code that is easy to understand and easy to maintain.

### Generate composable queries by using .NET LINQ functions with IQueryable.

The difference between IEnumerable and IQueryable is basically that IEnumerable is executed in the IL while IQueryable can be translated as en expression tree to some other context (like a database query).
They are both lazy by nature, meaning they aren’t evaluated until you enumerate over the results.

Here an example:

First you have to add .NET LINQ:
*)
open System.Linq

(**
Then you can define a composable query outside the main query
*)

let companyNameFilter inUse queryable =
    let queryable:(IQueryable<CustomersEntity> -> IQueryable<CustomersEntity>) =
        match inUse with
        |true ->
            (fun iq -> iq.Where(fun (c:CustomersEntity) -> c.CompanyName = "The Big Cheese"))
        |false -> (fun iq -> iq.Where(fun (c:CustomersEntity) -> c))
    queryable

(**
(Let's asume that your inUse some complex data:
 E.g. Your sub-queries would come from other functions. Basic booleans you can just include to your where-clause)

Then you can create the main query
*)
let query1 =
    query {
        for customers  in ctx.Main.Customers do
        where (customers.ContactTitle = "USA")
        select (customers)}


(**
and now call you are able to call the second query like this
*)
let query2 =
    companyNameFilter true query1 |> Seq.toArray

    let qry1 =
        query { for u in dbContext.Users do
                select (u.Id, u.Name, u.Email)
        }

    let qry2 =
        query { for c in dbContext.Cars do
                select (c.UserId, c.Brand, c.Year)
        }

    query { for (i,n,e) in qry1 do
            join (u,b,y) in qry2 on (i = u)
            where (y > 2015)
            select (i,n,e,u,b,y)
        } |> Seq.toArray

(**
### Generate composable queries by using FSharp.Linq.ComposableQuery

The SQLProvider also supports composable queries by integrating following library FSharpLinqComposableQuery.
You can read more about that library here: [FSharp.Linq.ComposableQuery](http://fsprojects.github.io/FSharp.Linq.ComposableQuery)

Because it is implemented in the SQLProvider you dont need to add FSharpComposableQuery in your script.

Example for using FSharpComposableQuery
*)
let qry1 =
    query { for u in dbContext.Users do
            select (u.Id, u.Name, u.Email)
    }

let qry2 =
    query { for c in dbContext.Cars do
            select (c.UserId, c.Brand, c.Year)
    }

query { for (i,n,e) in qry1 do
        join (u,b,y) in qry2 on (i = u)
        where (y > 2015)
        select (i,n,e,u,b,y)
    } |> Seq.toArray

(**

### Nested Select Where Queries

You can create a query like `SELECT * FROM xs WHERE xs.x IN (SELECT y FROM ys))`
with either LINQ Contains or custom operators: in `|=|` and not-in `|<>|`
This is done by not saying `|> Seq.toArray` to the first query:

*)

open System.Linq

let nestedQueryTest =
    let qry1 = query {
        for emp in ctx.Hr.Employees do
        where (emp.FirstName.StartsWith("S"))
        select (emp.FirstName)
    }
    query {
        for emp in ctx.Hr.Employees do
        where (qry1.Contains(emp.FirstName))
        select (emp.FirstName, emp.LastName)
    } |> Seq.toArray

(**

## Generate composable queries from quotations

You can also construct composable queries using the F# quotation mechanism. For
example, if you need to select a filter function at runtime, you could write the
filters as quotations, and then include them into query like that:

*)

let johnFilter = <@ fun (employee : sql.dataContext.``main.EmployeesEntity``) -> employee.FirstName = "John" @>
let pamFilter = <@ fun (employee : sql.dataContext.``main.EmployeesEntity``) -> employee.FirstName = "Pam" @>

let runtimeSelectedFilter = if 1 = 1 then johnFilter else pamFilter
let employees =
    query {
        for emp in ctx.Main.Employees do
        where ((%runtimeSelectedFilter) emp)
        select emp
    } |> Seq.toArray
