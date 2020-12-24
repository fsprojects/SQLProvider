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

The SsdtPath can either point directly to the SSDT Project File (.sqlproj), or it can point to a root folder that contains subdirectories and .sql files.
If it points to a .sqlproj file, scripts that are set to Build will be parsed and analyzed.
If it points to a folder, that folder will be recursively scanned for .sql files to be parsed and analyzed.
This is required for the SSDT provider to work. Both absolute and relative paths are supported.

*)
[<Literal>]
let ssdtPath = __SOURCE_DIRECTORY__ + @"/../../files/mssqlssdt/SSDT Project/"

(**
### Resolution Path (required)

Path to search for Microsoft.SqlServer.Management.SqlParser assembly. Both absolute and relative paths are supported.
If the ResoluationPath does not properly identify the path containing the 'Microsoft.SqlServer.Management.SqlParser.dll' assembly, the SSDT provider will yield many error messages about not being able to resolve various libraries used by the SqlParser.

*)

[<Literal>]
let resPath = __SOURCE_DIRECTORY__ + @"/../../../packages/Microsoft.SqlServer.Management.SqlParser/lib/netstandard2.0"

(**

#### Instructions for resolve SqlParser dll in a netstandard project in Visual Studio 2019:
* Add **'Microsoft.SqlServer.Management.SqlParser'** to the project via NuGet Package Manager.
* After installing, expand Dependencies/Packages for your project, click on the **'Microsoft.SqlServer.Management.SqlParser'** package and view the 'Path' property to find the location. (It should be stored in the [NuGet global-packages folder](https://docs.microsoft.com/en-us/nuget/consume-packages/managing-the-global-packages-and-cache-folders).)
* Browse to the location, then open the '**lib/netstandard2.0**' folder and copy the **'Microsoft.SqlServer.Management.SqlParser.dll'** assembly.
* Paste the dependency into your project folder (you can optionally put it in a new 'SSDT' subfolder, or something similar).
* Add the .dll to your .gitignore file (or the equivalent for your source control provider of choice).
* Finally, create a literal for your ResolutionPath.  If you put it in an 'SSDT' subfolder like the example, it may look like this:
*)

[<Literal>]
let exampleResPath = __SOURCE_DIRECTORY__ + "/SSDT"

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
let allowedTableNames = "Projects, ProjectTasks, ProjectTaskCategories, Users"

(**

### Example of the minimal required options for the SSDT provider:

*)
 
type DB = SqlDataProvider<
                dbVendor
                ,SsdtPath = ssdtPath
                ,ResolutionPath = resPath
            >

// To reload schema: 1) uncomment the line below; 2) save; 3) recomment; 4) save again and wait.
//DB.GetDataContext().``Design Time Commands``.ClearDatabaseSchemaCache

let ctx = DB.GetDataContext()


(**
### Employee Contact / Details example

#### TABLE: EmployeeContact.sql
CREATE TABLE [dbo].[EmployeeContact](  
  [EmpId] [int] NOT NULL,  
  [MobileNo] [nvarchar](50) NOT NULL,
  CONSTRAINT [PK_EmployeeContact] PRIMARY KEY CLUSTERED ([EmpId] ASC)
)

#### TABLE: EmployeeDetails.sql
CREATE TABLE [dbo].[EmployeeDetails] (  
  [EmpId] [int] IDENTITY(1,1) NOT NULL,  
  [EmpName] [nvarchar](50) NOT NULL,  
  [EmpCity] [nvarchar](50) NOT NULL,  
  [EmpSalary] [int] NOT NULL,  
  CONSTRAINT [PK_EmployeeDetails] PRIMARY KEY CLUSTERED ([EmpId] ASC),
  CONSTRAINT [FK_EmployeeContact_EmployeeDetails] FOREIGN KEY ([EmpId]) REFERENCES [dbo].[EmployeeContact] ([Id])
)

#### VIEW: v_Employee.sql
CREATE VIEW [dbo].[v_Employee]
AS  
  SELECT EmployeeDetails.EmpId, EmpName, EmployeeDetails.EmpSalary, EmployeeContact.MobileNo as MobilePhone
  FROM [dbo].EmployeeDetails   
  LEFT OUTER JOIN [dbo].EmployeeContact ON
  dbo.EmployeeDetails.Emp_Id = dbo.EmployeeContact.EmpId
  WHERE dbo.EmployeeDetails.EmpId > 2
*)

let employeeJoin =
    query {
        for c in ctx.Dbo.EmployeeContact do
        for d in c.``dbo.EmployeeDetails by Id`` do
        select (c.EmpId, c.MobileNo, d.EmpName, d.EmpSalary)
    }

let employeeView =
    query {
        for v in ctx.Dbo.VEmployee do
        select (v.EmpId, v.MobilePhone, v.EmpName, v.EmpSalary)
    }


(**
### An example of getting table column values
*)
let users = 
    ctx.Dbo.Users
    |> Seq.map (fun e -> e.ColumnValues |> Seq.toList)
    |> Seq.toList

(**
### An example query with joins
*)
let tasksWithCategories =
    query {
        for p in ctx.Dbo.Projects do
        for t in p.``dbo.ProjectTasks by Id`` do
        for c in t.``dbo.ProjectTaskCategories by Id`` do
        where (p.IsActive && not (p.IsDeleted))
        sortBy t.Sort
        select
            {| Id = t.Id
               Task = t.Name
               Category = c.Name
               Project = p.Name |}
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
* Computed columns in tables are currently ignored

### Views
* Views are only partially implemented at this point:
	* Some view columns may have a data type of System.Object if the type cannot be properly parsed.
For example, a view column that is wrapped in a COALLESCE() function will show as a property of type System.Object.
(This limitation can possibly be remedied with some more time spent in the parser).
	* Some view columns may be ignored (missing) if they can not be properly parsed
For example, more work needs to be done on the parser to handle WITH common table expressions, or anything other than a simple query.

### Procedures
* Stored procs are not currently implemented

### Functions
* Functions are not currently implemented

### Individuals
* Get "Individuals" feature is not implemented (because it requires a database connection)


*)

