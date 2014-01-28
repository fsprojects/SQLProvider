(*** hide ***)
#I "../../bin"

(**
SQLProvider
===========

A general SQL database type provider, supporting LINQ queries, schema exploration, individuals and much more besides.
<div class="row">
  <div class="span1"></div>
  <div class="span6">
    <div class="well well-small" id="nuget">
      The library can be <a href="https://nuget.org/packages/SQLProvider">installed from NuGet</a>:
      <pre>PM> Install-Package SQLProvider -prerelease</pre>
    </div>
  </div>
  <div class="span1"></div>
</div>

Example
-------

This example demonstrates the use of the SQL type provider:

*)
// reference the type provider dll
#r "FSharp.Data.SQLProvider.dll"
open System
open System.Linq
open FSharp.Data.Sql

// create a type alias with the connection string and database vendor you are using, along with a resolution path for 
// 3rd party assemblies if you are not using MS SQL Server.  The last two parameters are the amount of individuals 
// to project into the devleopment einvronment, and finally whether to generate nullable columns as F# option types.
type sql = SqlDataProvider< @"Data Source=F:\sqlite\northwindEF.db ;Version=3", Common.DatabaseProviderTypes.SQLITE, @"F:\sqlite\3", 1000, true >
let ctx = sql.GetDataContext()

// pick individual entities from the database 
let christina = ctx.``[main].[Customers]``.Individuals.``As ContactName``.``BERGS, Christina Berglund``

// directly enumerate an entity's relationships, 
// this creates and triggers the relevant query in the background
let christinasOrders = christina.FK_Orders_0_0 |> Seq.toArray

let mattisOrderDetails =
    query { for c in ctx.``[main].[Customers]`` do
            // you can directly enumerate relationships with no join information
            for o in c.FK_Orders_0_0 do
            // or you can explcitly join on the fields you choose
            join od in ctx.``[main].[OrderDetails]`` on (o.OrderID = od.OrderID)
            //  the (!!) operator will perform an outer join on a relationship
            for prod in (!!) od.FK_OrderDetails_0_0 do 
            // nullable columns can be represented as option types. The following generates IS NOT NULL
            where c.CompanyName.IsSome                
            // standard operators will work as expected; the following shows the like operator and IN operator
            where (c.ContactName =% ("Matti%") && o.ShipCountry |=| [|"Finland";"England"|] )
            sortBy o.ShipName
            // arbitrarily complex projections are supported
            select (c.ContactName,o.ShipAddress,o.ShipCountry,prod.ProductName,prod.UnitPrice) } 
    |> Seq.toArray

(**

Samples & documentation
-----------------------

The library comes with comprehensible documentation.

 * [API Reference](reference/index.html) contains automatically generated documentation for all types, modules
   and functions in the library. This includes additional brief samples on using most of the
   functions.
 
Contributing and copyright
--------------------------

The project is hosted on [GitHub][gh] where you can [report issues][issues], fork 
the project and submit pull requests. If you're adding new public API, please also 
consider adding [samples][content] that can be turned into a documentation. You might
also want to read [library design notes][readme] to understand how it works.

The library is available under Public Domain license, which allows modification and 
redistribution for both commercial and non-commercial purposes. For more information see the 
[License file][license] in the GitHub repository. 

  [content]: https://github.com/fsprojects/SQLProvider/tree/master/docs/content
  [gh]: https://github.com/fsprojects/SQLProvider
  [issues]: https://github.com/fsprojects/SQLProvider/issues
  [readme]: https://github.com/fsprojects/SQLProvider/blob/master/README.md
  [license]: https://github.com/fsprojects/SQLProvider/blob/master/LICENSE.md
*)
