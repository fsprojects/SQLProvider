(*** hide ***)
#I "../../bin"

(**
SQLProvider
===========

A general SQL database type provider, supporting LINQ queries, schema exploration, individuals and much more besides.

The provider currently supports MS SQL Server, SQLite, PostgreSQL, Oracle, MySQL and MS Access. All database vendors except SQL Server and MS Access will require 3rd party ADO.NET connector objects to function. These are dynamically loaded at runtime so that the SQL provider project is not dependent on them. You must supply the location of the assemblies with the "ResolutionPath" static parameter.

SQLite is based on the .NET drivers found [here](http://system.data.sqlite.org/index.html/doc/trunk/www/downloads.wiki). You will need the correct version for your specific architecture and setup.

PostgreSQL is based on the .NET drivers found [here](http://npgsql.projects.pgfoundry.org/).  The type provider will make frequent calls to the database. I found that using the default settings for the PostgreSQL server on my Windows machine would deny the provider constant access - you may need to try setting  `Pooling=false` in the connection string, increasing timeouts or setting other relevant security settings to enable a frictionless experience.

MySQL is based on the .NET drivers found [here](http://dev.mysql.com/downloads/connector/net/1.0.html). You will need the correct version for your specific architecture and setup.

Oracle is based on the current release (12.1.0.1.2) of the managed ODP.NET driver found [here](http://www.oracle.com/technetwork/topics/dotnet/downloads/index.html). However although the managed version is recommended it should also work with previous versions of the native driver.

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

// create a type alias with the connection string and database vendor settings
type sql = SqlDataProvider< 
              ConnectionString = @"Data Source=F:\sqlite\northwindEF.db ;Version=3",
              DatabaseVendor = Common.DatabaseProviderTypes.SQLITE,
              ResolutionPath = @"F:\sqlite\3",
              IndividualsAmount = 1000,
              UseOptionTypes = true >
let ctx = sql.GetDataContext()

// pick individual entities from the database 
let christina = ctx.Customers.Individuals.``As ContactName``.``BERGS, Christina Berglund``

// directly enumerate an entity's relationships, 
// this creates and triggers the relevant query in the background
let christinasOrders = christina.FK_Orders_0_0 |> Seq.toArray

let mattisOrderDetails =
    query { for c in ctx.Customers do
            // you can directly enumerate relationships with no join information
            for o in c.FK_Orders_0_0 do
            // or you can explicitly join on the fields you choose
            join od in ctx.OrderDetails on (o.OrderID = od.OrderID)
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
