(*** hide ***)
#I @"../../files/sqlite"
(*** hide ***)
#I "../../../bin"
(*** hide ***)
[<Literal>]
let connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"\northwindEF.db;Version=3"
(*** hide ***)
[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + @"..\..\..\files\sqlite"
#r "FSharp.Data.SqlProvider.dll"
open FSharp.Data.Sql

(**
# Composable Queries

The SQLProvider supports composable queries.
With composable queries you can combine two queries in multiple ways, and one query can be used as the building block for the other query.
To see how this works, let’s look at a simple query:
*)

let query1 =
    query {
      for customers  in ctx.Main.Customers do
      where (customers.ContactTitle = "USA")
      select (customers)} |> Seq.toArray

(**
The variable that is returned from the query is sometimes called a computation.
If you write a foreach loop and display the address field from the customers returned by this computation, you see the following output:
* GREAL|Great Lakes Food Market|Howard Snyder|Marketing Manager|2732 Baker Blvd.|Eugene|OR|97403|USA|(503) 555-7555|
* HUNGC|Hungry Coyote Import Store|Yoshi Latimer|Sales Representative|City Center Plaza 516 Main St.|Elgin|OR|97827|USA|(503) 555-6874|(503) 555-2376
* ...
* TRAIH|Trail's Head Gourmet Provisioners|Helvetius Nagy|Sales Associate|722 DaVinci Blvd.|Kirkland|WA|98034|USA|(206) 555-8257|(206) 555-2174
* WHITC|White Clover Markets|Karl Jablonski|Owner|305 - 14th Ave. S. Suite 3B|Seattle|WA|98128|USA|(206) 555-4112|(206) 555-4115// You can now write a second query against the results of this query:

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

* THEBI|The Big Cheese|Liz Nixon|Marketing Manager|89 Jefferson Way Suite 2|Portland|OR|97201|USA|(503) 555-3612|

SQLProvider to Objects queries are composable because they operate on and usually return variables of type IEnumerable<T>. In other words, SQLProvider queries typically follow this pattern:

let IEnumerable<T> query = from x in IEnumerable<T> do
                           select x

This is a simple mechanism to understand, but it yields powerful results.
It allows you to take complex problems, break them into manageable pieces, and solve them with code that is easy to understand and easy to maintain.

*)
