(*** hide ***)
#I "../../files/sqlite"
(*** hide ***)
#I "../../../bin/net451"
(*** hide ***)
#r @"../../../bin/net451/FSharp.Data.SqlProvider.dll"

(*** hide ***)
[<Literal>]
let connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"/../../../tests/SqlProvider.Tests/scripts/northwindEF.db;Version=3"

(*** hide ***)
let connectionString2 = "Data Source=" + __SOURCE_DIRECTORY__ + @"/../../../tests/SqlProvider.Tests/scripts/northwindEF.db;Version=3"

(*** hide ***)
[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + @"/../../../tests/SqlProvider.Tests/libs"

(**


# SQL Provider Basics

The SQL provider is an erasing type provider which enables you to instantly 
connect to a variety of database sources in the IDE and explore them in a 
type-safe manner, without the inconvenience of a code-generation step.

SQL Provider supports the following database types:

* [MSSQL](mssql.html) 
* [Oracle](oracle.html) 
* [SQLite](sqlite.html) 
* [PostgreSQL](postgresql.html)
* [MySQL](mysql.html)
* [MsAccess](msaccess.html)
* [ODBC](odbc.html) (_Experimental_, only supports SELECT & MAKE)

After you have installed the nuget package or built the type provider assembly 
from source, you should reference the assembly either as a project reference 
or by using an F# interactive script file.

```
// when using the SQLProvider in a script file (.fsx), the file needs to be referenced
// using F#'s `#r` command:
#r "../../packages/SQLProvider.1.0.1/lib/net40/FSharp.Data.SqlProvider.dll"
// whether referencing in a script, or added to project as an assembly
// reference, the library needs to be opened
```
*)
open FSharp.Data.Sql

(** 


To use the type provider you must first create a type alias. 

In this declaration you are able to pass various pieces of information known 
as static parameters to initialize properties such as the connection string 
and database vendor type that you are connecting to. 

In the following examples a SQLite database will be used.  You can read in 
more detail about the available static parameters in other areas of the 
documentation.
*)

type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE,
                           connectionString,
                           ResolutionPath = resolutionPath,
                           CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL>

(** 
Now we have a type ``sql`` that represents the SQLite database provided in 
the connectionString parameter.  In order to start exploring the database's 
schema and reading its data, you create a *DataContext* value.
*)

let ctx = sql.GetDataContext()

(**
If you want to use non-literal connectionString at runtime (e.g. crypted production
passwords), you can pass your runtime connectionString parameter to GetDataContext:
*)

let ctx2 = sql.GetDataContext connectionString2

(**

When you press ``.`` on ``ctx``, intellisense will display a list of properties 
representing the available tables and views within the database.  

In the simplest case, you can treat these properties as sequences that can 
be enumerated.
*)

let customers = ctx.Main.Customers |> Seq.toArray
(**
This is the equivalent of executing a query that selects all rows and 
columns from the ``[main].[customers]`` table.  

Notice the resulting type is an array of ``[Main].[Customers]Entity``.  These 
entities will contain properties relating to each column name from the table.
*)

let firstCustomer = customers.[0]
let name = firstCustomer.ContactName
(**
Each property is correctly typed depending on the database column 
definitions.  In this example, ``firstCustomer.ContactName`` is a string.

Most of the databases support some kind of comments/descriptions/remarks to
tables and columns for documentation purposes. These descriptions are fetched
to tooltips for the tables and columns.
*)

(**
## Constraints and Relationships 

A typical relational database will have many connected tables and views 
through foreign key constraints.  The SQL provider is able to show you these 
constraints on entities.  They appear as properties named the same as the 
constraint in the database.

You can gain access to these child or parent entities by simply enumerating 
the property in question.
*)

let orders = firstCustomer.``main.Orders by CustomerID`` |> Seq.toArray

(**
``orders`` now contains all the orders belonging to firstCustomer. You will 
see the orders type is an array of ``[Main].[Orders]Entity`` indicating the 
resulting entities are from the ``[main].[Orders]`` table in the database.
If you hover over ``FK_Orders_0_0`` intellisense will display information 
about the constraint in question including the names of the tables involved 
and the key names.

Behind the scenes the SQL provider has automatically constructed and executed 
a relevant query using the entity's primary key.


## Basic Querying
The SQL provider supports LINQ queries using F#'s *query expression* syntax.
*)

let customersQuery = 
    query { 
        for customer in ctx.Main.Customers do
            select customer
    }
    |> Seq.toArray

(**

Support also async queries

 *)

let customersQueryAsync = 
    query { 
        for customer in ctx.Main.Customers do
            select customer
    }
    |> Seq.executeQueryAsync |> Async.StartAsTask


(**
The above example is identical to the query that was executed when 
``ctx.[main].[Customers] |> Seq.toArray`` was evaluated.

You can extend this basic query include to filter criteria by introducing 
one or more *where* clauses
*)

let filteredQuery = 
    query { 
        for customer in ctx.Main.Customers do
            where (customer.ContactName = "John Smith")
            select customer
    }
    |> Seq.toArray

let multipleFilteredQuery = 
    query { 
        for customer in ctx.Main.Customers do
            where ((customer.ContactName = "John Smith" && customer.Country = "England") || customer.ContactName = "Joe Bloggs")
            select customer
    }
    |> Seq.toArray

(**
The SQL provider will accept any level of nested complex conditional logic 
in the *where* clause.

To access related data, you can either enumerate directly over the constraint 
property of an entity, or you can perform an explicit join.
*)

let automaticJoinQuery = 
    query { 
        for customer in ctx.Main.Customers do
            for order in customer.``main.Orders by CustomerID`` do
                where (customer.ContactName = "John Smith")
                select (customer, order)
    }
    |> Seq.toArray

let explicitJoinQuery = 
    query { 
        for customer in ctx.Main.Customers do
            join order in ctx.Main.Orders on (customer.CustomerId = order.CustomerId)
            where (customer.ContactName = "John Smith")
            select (customer, order)
    }
    |> Seq.toArray

(**
Both of these queries have identical results, the only difference is that one 
requires explicit knowledge of which tables join where and how, and the other doesn't.
You might have noticed the select expression has now changed to (customer, order). 
As you may expect, this will return an array of tuples where the first item 
is a ``[Main].[Customers]Entity`` and the second a ``[Main].[Orders]Entity``.
Often you will not be interested in selecting entire entities from the database.
Changing the select expression to use the entities' properties will cause the 
SQL provider to select only the columns you have asked for, which is an 
important optimization.
*)

let ordersQuery = 
    query { 
        for customer in ctx.Main.Customers do
            for order in customer.``main.Orders by CustomerID`` do
                where (customer.ContactName = "John Smith")
                select (customer.ContactName, order.OrderDate, order.ShipAddress)
    }
    |> Seq.toArray

(**
The results of this query will return the name, order date and ship address 
only.  By doing this you no longer have access to entity types.
The SQL provider supports various other query keywords and features that you 
can read about elsewhere in this documentation.


## Individuals
The SQL provider has the ability via intellisense to navigate the actual data 
held within a table or view. You can then bind that data as an entity to a value.
*)

let BERGS = ctx.Main.Customers.Individuals.BERGS
(**
Every table and view has an ``Individuals`` property. When you press dot on 
this property, intellisense will display a list of the data in that table, 
using whatever the primary key is as the text for each one.
In this case, the primary key for ``[main].[Customers]`` is a string, and I 
have selected one named BERGS. You will see the resulting type is 
``[main].[Customers]Entity``.

The primary key is not usually very useful for identifying data however, so 
in addition to this you will see a series of properties named "As X" where X 
is the name of a column in the table.
When you press . on one of these properties, the data is re-projected to you 
using both the primary key and the text of the column you have selected.
*)

let christina = ctx.Main.Customers.Individuals.``As ContactName``.``BERGS, Christina Berglund``
