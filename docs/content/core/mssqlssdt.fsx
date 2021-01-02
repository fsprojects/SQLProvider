(*** hide ***)
//#I @"../../files/mssqlssdt/SSDT Project"
(*** hide ***)
#I "../../../bin/netstandard2.0"
(*** hide ***)
#r "FSharp.Data.SqlProvider.dll"
open FSharp.Data.Sql
open System

(**


# MSSQL SSDT Provider

The SSDT provider allows types to be provided via SQL Server schema scripts in an SSDT project. No live database connection is required!

## Parameters

### Database Vendor (required)

Use `MSSQLSERVER_SSDT` from the `FSharp.Data.Sql.Common.DatabaseProviderTypes` enumeration.

*)
[<Literal>]
let dbVendor = Common.DatabaseProviderTypes.MSSQLSERVER_SSDT

(**
### SSDT Path (required)

The SsdtPath must point to a .dacpac file.
Notes:
* For development, you can set the SsdtPath to point to the .dacpac file in the SSDT project Debug folder.
* For deployment, the SSDT provider will search for the .dacpac file in the executing assembly folder. (Set the .dacpac file to "Copy to Output Directory" to ensure it is available.)

*)
[<Literal>]
let ssdtPath = __SOURCE_DIRECTORY__ + @"/../../files/mssqlssdt/AdventureWorks_SSDT.dacpac"

(**
### Use Option Types

If true, F# option types will be used in place of nullable database columns.  If false, you will always receive the default value of the column's type even if it is null in the database.

*)
[<Literal>]
let useOptTypes = true

(**
## Example

### Table Names Filter

Because MSSQL databases can be huge, there is an optional constructor parameter `TableNames` that can be used as a filter.
The SSDT provider currently supports a simple comma delimited list of allowed table names (wildcards are not yet supported).

*)
[<Literal>]
let exampleAllowedTableNames = "Projects, ProjectTasks, ProjectTaskCategories, Users"

(**

### Example of the minimal required options for the SSDT provider:

*)
 
type DB = SqlDataProvider<
                Common.DatabaseProviderTypes.MSSQLSERVER_SSDT,
                SsdtPath = ssdtPath>

// To reload schema: 1) uncomment the line below; 2) save; 3) recomment; 4) save again and wait.
//DB.GetDataContext().``Design Time Commands``.ClearDatabaseSchemaCache

let ctx = DB.GetDataContext()

(**
### AdventureWorks SSDT Example
*)

let orderDetails =
    query {
        for o in ctx.SalesLt.SalesOrderHeader do
        for d in o.``SalesLT.SalesOrderDetail by SalesOrderID`` do
        select (o.SalesOrderId, o.OrderDate, o.SubTotal, d.OrderQty, d.ProductId, d.UnitPrice)
    }


(**

## What is SSDT?

SQL Server Data Tools (SSDT) is a modern development tool for building SQL Server relational databases, databases in Azure SQL,
Analysis Services (AS) data models, Integration Services (IS) packages, and Reporting Services (RS) reports.

It allows you to easily compare and synchronize schema changes between your SQL Server database and the current state of your .sql scripts in source control.
Schemas can be synchronized bi-directionally (SSDT -> SQL Server or SQL Server -> SSDT).

### Advantages of using the SSDT provider
The main advantage to using the SSDT provider is that it does not require a live connection to the database.
This makes it easier to run on a build server without having to manually spin up a database instance.

Another advantage is that since your SSDT scripts are checked into your source control, you can easily have different schemas in each branch, so each branch can compile according its local schema snapshot.

### How to create an SSDT Project

SSDT Projects can be created in two ways:
* Visual Studio [SSDT](https://docs.microsoft.com/en-us/sql/ssdt/download-sql-server-data-tools-ssdt?view=sql-server-ver15)
* Azure Data Studio via the [SQL Database Projects Extension](https://docs.microsoft.com/en-us/sql/azure-data-studio/extensions/sql-database-project-extension?view=sql-server-ver15)

## Known Issues

### Tables
* User defined data types are not currently supported

### Views
* Some view columns may have a data type of System.Object if the referenced column type cannot be fully traced by the parser.

### Functions
* Functions are not currently implemented

### Individuals
* Get "Individuals" feature is not implemented (because it requires a database connection)


*)

