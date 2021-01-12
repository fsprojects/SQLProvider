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

### DatabaseVendor (required)

Use `MSSQLSERVER_SSDT` from the `FSharp.Data.Sql.Common.DatabaseProviderTypes` enumeration.

*)
[<Literal>]
let dbVendor = Common.DatabaseProviderTypes.MSSQLSERVER_SSDT

(**
### SsdtPath (required)

The SsdtPath must point to a .dacpac file.

#### Notes:
* A .dacpac file is generated when an SSDT project is built, and can be found in the bin/Debug folder.
* For development, you can set the SsdtPath to point to the generated .dacpac file in the SSDT project Debug folder. (Using a `[<Literal>]` ssdtPath allows relative pathing).
* For deployment, the SSDT provider will search for the .dacpac file in the entry assembly folder. 
* Linking the generated .dacpac file to your project and setting it to `CopyToOutputDirectory` will ensure that it will exist in the assembly folder for deployment.


*)
[<Literal>]
let ssdtPath = __SOURCE_DIRECTORY__ + @"/../../files/mssqlssdt/AdventureWorks_SSDT.dacpac"

(**

## Example of the minimal required options for the SSDT provider:

*)
 
type DB = SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER_SSDT, SsdtPath = ssdtPath>

// To reload schema: 1) uncomment the line below; 2) save; 3) recomment; 4) save again and wait.
//DB.GetDataContext().``Design Time Commands``.ClearDatabaseSchemaCache

(**
#### Reloading the schema

It is helpful to keep the above Design Time Command commented out just below your SqlDataProvider type for refreshing the generated types after a schema change.
*)

(**
## Optional Parameters

### UseOptionTypes

If true, F# option types will be used in place of nullable database columns. If false, you will always receive the default value of the column's type even if it is null in the database.

### Table Names Filter

The SSDT provider currently supports a simple comma delimited list of allowed table names (wildcards are not currently supported).

*)

(**
## AdventureWorks Example
*)

let ctx = DB.GetDataContext()

let orderDetails =
    query {
        for o in ctx.SalesLt.SalesOrderHeader do
        for d in o.``SalesLT.SalesOrderDetail by SalesOrderID`` do
        select (o.SalesOrderId, o.OrderDate, o.SubTotal, d.OrderQty, d.ProductId, d.LineTotal)
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
* User defined data types are not yet supported
* Computed table columns will default to a data type of `System.Object` since the data type is not listed in the .dacpac file. (See Type Annotations below.)

### Views
* Computed view columns will default to a data type of `System.Object` since the data type is not listed in the .dacpac file. (See Type Annotations below.)

#### Type Annotations
As a work-around for computed table and view columns having unresolved data types, the SSDT provider allows you to add type annotations directly to the table or view as in-line comments.

In the SalesOrderDetail.sql example table below, `[LineTotal]` is a computed column. Since the .dacpac file cannot determine the datatype for computed columns, the data type of the generated property will be defaulted to `obj`.
As a workaround, an in-line type annotation `/* MONEY NOT NULL /*` can be added.
NOTE: for computed table columns, the comment annotation must be contained within the parentheses.

```sql
CREATE TABLE [SalesLT].[SalesOrderDetail] (
[SalesOrderID]       INT              NOT NULL,
[SalesOrderDetailID] INT              IDENTITY (1, 1) NOT NULL,
[OrderQty]           SMALLINT         NOT NULL,
[ProductID]          INT              NOT NULL,
[UnitPrice]          MONEY            NOT NULL,
[UnitPriceDiscount]  MONEY            CONSTRAINT [DF_SalesOrderDetail_UnitPriceDiscount] DEFAULT ((0.0)) NOT NULL,
[LineTotal]          AS               (isnull(([UnitPrice]*((1.0)-[UnitPriceDiscount]))*[OrderQty],(0.0)) /* MONEY NOT NULL */ ),
[rowguid]            UNIQUEIDENTIFIER CONSTRAINT [DF_SalesOrderDetail_rowguid] DEFAULT (newid()) ROWGUIDCOL NOT NULL,
[ModifiedDate]       DATETIME         CONSTRAINT [DF_SalesOrderDetail_ModifiedDate] DEFAULT (getdate()) NOT NULL,
...
```

In the example `dbo.v_Hours` view below, the `Hours` column is not linked back to the `dbo.TimeEntries.Hours` column in the .dacpac metadata because it is a calculated field, so the data type of the generated property will be defaulted to `obj`.
Adding a type annotation within an in-line comment will inform the SSDT provider of the data type to use in the generated `Hours` property:

```sql
CREATE VIEW dbo.v_Hours
AS
SELECT dbo.Projects.Name AS ProjectName, COALESCE (dbo.TimeEntries.Hours, 0) AS Hours /* decimal not null */, dbo.Users.Username
FROM dbo.Projects
INNER JOIN dbo.TimeEntries on dbo.Projects.Id = dbo.TimeEntries.ProjectId
INNER JOIN dbo.Users on dboUsers.Id = dbo.TimeEntries.UserId
```

##### Notes:
* If no null constraint is added after the column type, it will allow nulls by default.
* The annotations are case-insensitive.
* Hovering over a generated view property will designate if the data type was derived from a type annotations (or if it needs one).
* Do not include length information in the type annotation. For example, use `varchar`, not `varchar(20)`.

### Functions
* Functions are not yet implemented

### Individuals
* Get "Individuals" feature is not implemented (because it requires a database connection)


*)

