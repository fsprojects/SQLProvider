(*** hide ***)

(**
SQLProvider
===========

A general .NET/Mono SQL database type provider. Current features:

* [LINQ queries](core/querying.html)
* Lazy schema exploration
* Automatic constraint navigation
* [Individuals](core/individuals.html)
* Transactional [CRUD](core/crud.html) operations with identity support
* [Stored Procedures](core/programmability.html)
* [Functions](core/programmability.html)
* Packages (Oracle)
* [Composable Query](core/composable.html) integration
* Optional option types
* [Mapping to record types](core/mappers.html)
* Custom Operators
* Supports [Asynchronous Operations](core/async.html)
* Supports [.NET Standard / .NET Core](core/netstandard.html)
* Supports [saving DB schema offline](core/parameters.html#ContextSchemaPath), and [SQL-Server *.dacpac files](core/mssqlssdt.html)

The provider currently has explicit implementations for the following database vendors:

* SQL Server
* SQL Server SSDT
* SQLite
* PostgreSQL
* Oracle
* MySQL
* MsAccess
* Firebird
* DuckDB

There is also an ODBC provider that will let you connect to any ODBC source with limited features.

All database vendors except SQL Server and MS Access will require 3rd party ADO.NET connector objects to function. These are dynamically loaded at runtime so that the SQL provider project is not dependent on them. You must supply the location of the assemblies with the "ResolutionPath" static parameter.

SQLite is based on the .NET drivers found [here](http://system.data.sqlite.org/index.html/doc/trunk/www/downloads.wiki). You will need the correct version for your specific architecture and setup.

PostgreSQL is based on the Npgsql .NET drivers found [here](http://www.npgsql.org/doc/). The type provider will make frequent calls to the database. Npgsql provides a set of [performance related connection strings parameters](http://www.npgsql.org/doc/connection-string-parameters.html#performance) for tweaking its performance

MySQL is based on the .NET drivers found [here](http://dev.mysql.com/downloads/connector/net/1.0.html). You will need the correct version for your specific architecture and setup. You also need to specify ResolutionPath, which points to the folder containing the dll files for the MySQL driver.

Oracle is based on the current release (12.1.0.1.2) of the managed ODP.NET driver found [here](http://www.oracle.com/technetwork/topics/dotnet/downloads/index.html). However, although the managed version is recommended, it should also work with previous versions of the native driver.

SQL Server SSDT is based on the current release (160.20216.14) found [here](https://www.nuget.org/packages/Microsoft.SqlServer.Management.SqlParser/).

| Database | Nuget Package | TypeProvider Class | NuGet Status |
| ------- | ----- | -----------| ---------- |
| Microsoft SQL Server | [SQLProvider.MsSql](https://www.nuget.org/packages/SQLProvider.MsSql) | FSharp.Data.Sql.MsSql.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.MsSql) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.MsSql)](https://www.nuget.org/packages/SQLProvider.MsSql)
| PostgreSQL | [SQLProvider.PostgreSql](https://www.nuget.org/packages/SQLProvider.PostgreSql) | FSharp.Data.Sql.PostgreSql.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.PostgreSql) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.PostgreSql)](https://www.nuget.org/packages/SQLProvider.PostgreSql)
| MySQL and MariaDB | [SQLProvider.MySqlConnector](https://www.nuget.org/packages/SQLProvider.MySqMySqlConnectorl) |  FSharp.Data.Sql.MySqlConnector.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.MySqlConnector) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.MySqlConnector)](https://www.nuget.org/packages/SQLProvider.MySqMySqlConnectorl)
| MySQL | [SQLProvider.MySql](https://www.nuget.org/packages/SQLProvider.MySql)  | FSharp.Data.Sql.MySql.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.MySql) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.MySql)](https://www.nuget.org/packages/SQLProvider.MySql)
| Oracle | [SQLProvider.Oracle](https://www.nuget.org/packages/SQLProvider.Oracle)  | FSharp.Data.Sql.Oracle.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.Oracle) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.Oracle)](https://www.nuget.org/packages/SQLProvider.Oracle)
| Any ODBC connection | [SQLProvider.Odbc](https://www.nuget.org/packages/SQLProvider.Odbc) | FSharp.Data.Sql.Odbc.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.Odbc) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.Odbc)](https://www.nuget.org/packages/SQLProvider.Odbc)
| SQLite | [SQLProvider.SQLite](https://www.nuget.org/packages/SQLProvider.SQLite) | FSharp.Data.Sql.SQLite.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.SQLite) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.SQLite)](https://www.nuget.org/packages/SQLProvider.SQLite)
| Microsoft Access | [SQLProvider.MsAccess](https://www.nuget.org/packages/SQLProvider.MsAccess)  | FSharp.Data.Sql.MsAccess.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.MsAccess) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.MsAccess)](https://www.nuget.org/packages/SQLProvider.MsAccess)
| Firebird | [SQLProvider.Firebird](https://www.nuget.org/packages/SQLProvider.Firebird) | FSharp.Data.Sql.Firebird.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.Firebird) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.Firebird)](https://www.nuget.org/packages/SQLProvider.Firebird)
| DuckDb | [SQLProvider.DuckDb](https://www.nuget.org/packages/SQLProvider.DuckDb) | FSharp.Data.Sql.DuckDb.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.DuckDb) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.DuckDb)](https://www.nuget.org/packages/SQLProvider.DuckDb)
| General, all via manual config | [SQLProvider](https://www.nuget.org/packages/SQLProvider) | FSharp.Data.Sql.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider)](https://www.nuget.org/packages/SQLProvider)

<div class="row">
  <div class="span1"></div>
  <div class="span6">
    <div class="well well-small" id="nuget">
      The library can be <a href="https://nuget.org/packages/SQLProvider">installed from NuGet</a>:
      <pre>PM> Install-Package SQLProvider</pre>
    </div>
  </div>
  <div class="span1"></div>
</div>

Depending on the used provider, the namespace of the SqlDataProvider type class varies as above.
The general version of SQLProvider is the most popular because it is the oldest one. It often uses reflection, so the database-specific ones may be better in performance. It also requires manual configuration with a reference assembly path to seek the reference database drivers.

Example
-------

![No OR-mapping: FSharp compiles your database to .NET-types.](https://raw.githubusercontent.com/fsprojects/SQLProvider/master/docs/files/sqlprovider.gif "No OR-mapping: FSharp compiles your database to .NET-types.")

This example demonstrates the use of the SQL type provider:

*)
// reference the type provider dll

#r "../../bin/lib/netstandard2.0/FSharp.Data.SqlProvider.Common.dll"
#r "../../bin/lib/netstandard2.0/FSharp.Data.SqlProvider.dll"

open FSharp.Data.Sql

let [<Literal>] resolutionPath = __SOURCE_DIRECTORY__ + @"/../files/sqlite"
let [<Literal>] connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"\northwindEF.db;Version=3;Read Only=false;FailIfMissing=True;"
// create a type alias with the connection string and database vendor settings
type sql = SqlDataProvider<
              ConnectionString = connectionString,
              DatabaseVendor = Common.DatabaseProviderTypes.SQLITE,
              SQLiteLibrary=Common.SQLiteLibrary.SystemDataSQLite,
              ResolutionPath = resolutionPath,
              IndividualsAmount = 1000,
              UseOptionTypes = Common.NullableColumnType.OPTION
              >
let ctx = sql.GetDataContext()

// To use dynamic runtime connectionString, you could use:
// let ctx = sql.GetDataContext connectionString2

// pick individual entities from the database
let christina = ctx.Main.Customers.Individuals.``As ContactName``.``BERGS, Christina Berglund``

// directly enumerate an entity's relationships,
// this creates and triggers the relevant query in the background
let christinasOrders = christina.``main.Orders by CustomerID`` |> Seq.toArray

let mattisOrderDetails =
    query { for c in ctx.Main.Customers do
            // you can directly enumerate relationships with no join information
            for o in c.``main.Orders by CustomerID`` do
            // or you can explicitly join on the fields you choose
            join od in ctx.Main.OrderDetails on (o.OrderId = od.OrderId)
            //  the (!!) operator will perform an outer join on a relationship
            for prod in (!!) od.``main.Products by ProductID`` do
            // nullable columns can be represented as option types; the following generates IS NOT NULL
            where o.ShipCountry.IsSome
            // standard operators will work as expected; the following shows the like operator and IN operator
            where (c.ContactName =% ("Matti%") && c.CompanyName |=| [|"Squirrelcomapny";"DaveCompant"|] )
            sortBy o.ShipName
            // arbitrarily complex projections are supported
            select (c.ContactName,o.ShipAddress,o.ShipCountry,prod.ProductName,prod.UnitPrice) }
    |> Seq.toArray

(**

Samples & documentation
-----------------------

The library comes with comprehensive documentation.

 * [General](core/general.html) a high level view on the type providers' abilities and limitations
 * [Static Parameters](core/parameters.html) available static parameters
 * [Querying](core/querying.html) information on supported LINQ keywords and custom operators with examples
 * [Relationships](core/constraints-relationships.html) how to use automatic constraint navigation in your queries
 * [CRUD](core/crud.html) usage and limitations of transactional create - update - delete support
 * [Programmability](core/programmability.html) usage and limitations of stored procedures and functions
 * [Individuals](core/individuals.html) usage and limitations of this unique feature
 * [Composable Query](core/composable.html) information on integrating this project with the SQL provider
 * [Mapping to record types](core/mappers.html)
 * [Unit-testing](core/unittest.html) your SQL-query logics without a database.
 * [API Reference](reference/index.html) contains automatically generated documentation for all types, modules
   and functions in the library.

Database vendor specific issues and considerations are documented on their separate pages. Please see the menu on the right.

Contributing and copyright
--------------------------

The project is hosted on [GitHub][gh] where you can [report issues][issues], fork
the project and submit [pull requests](core/contributing.html). If you're adding new public API, please also
consider adding [samples][content] that can be turned into a documentation. You might
also want to read [library design notes][readme] to understand how it works.
Our tests have [more samples][tests]. Learn more tech [tech details](core/techdetails.html).

The library is available under Public Domain license, which allows modification and
redistribution for both commercial and non-commercial purposes. For more information see the
[License file][license] in the GitHub repository.

  [content]: https://github.com/fsprojects/SQLProvider/tree/master/docs/content/core
  [tests]: https://github.com/fsprojects/SQLProvider/tree/master/tests/SqlProvider.Tests/scripts
  [gh]: https://github.com/fsprojects/SQLProvider
  [issues]: https://github.com/fsprojects/SQLProvider/issues
  [readme]: https://github.com/fsprojects/SQLProvider/blob/master/README.md
  [license]: https://github.com/fsprojects/SQLProvider/blob/master/LICENSE.txt
*)
