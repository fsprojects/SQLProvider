(*** hide ***)
#I @"../../files/sqlite"
(*** hide ***)
#I "../../../bin/net451"
(*** hide ***)
let connectionString =
    "Data Source=" + __SOURCE_DIRECTORY__ + @"\northwindEF.db;Version=3"
(*** hide ***)
[<Literal>]
let resolutionPath =
    __SOURCE_DIRECTORY__ + @"..\..\..\files\sqlite"
#r "FSharp.Data.SqlProvider.dll"
open FSharp.Data.Sql
(**


# PostgreSQL Provider

## Parameters

### DatabaseVendor

From the `FSharp.Data.Sql.Common.DatabaseProviderTypes` enumeration. For PostgreSQL,
use `Common.DatabaseProviderTypes.POSTGRESQL`.

*)

let [<Literal>] dbVendor = Common.DatabaseProviderTypes.POSTGRESQL

(**

### ConnectionString

Basic connection string used to connect to PostgreSQL instance; typical 
connection strings for the driver apply here. See
[PostgreSQL Connecting Strings Documentation](http://www.npgsql.org/doc/connection-string-parameters.html)
for a complete list of connection string options.

*)

let [<Literal>] connString = "Host=localhost;Database=test;Username=test;Password=test"

(**
### ConnectionStringName

Instead of storing the connection string in the source code / `fsx` script, you
can store values in the `App.config` file.  This is the name of the
connectionString key/value pair stored in App.config (TODO: confirm file name).
*)

// found in App.config (TOOD: confirm)
let [<Literal>] connexStringName = "DefaultConnectionString"

(**
### Resolution Path

Path to search for assemblies containing database vendor specific connections 
and custom types. Type the path where `Npgsql.Data.dll` is stored.

*)

let [<Literal>] resPath = @"C:\Projects\Libs\Npgsql"

(**
### IndividualsAmount

Sets the count to load for each individual. See [individuals](individuals.html)
for further info.

*)

let [<Literal>] indivAmount = 1000

(**
### UseOptionTypes

If true, F# option types will be used in place of nullable database columns.  
If false, you will always receive the default value of the column's type, even 
if it is null in the database.

*)

let [<Literal>] useOptTypes  = true

(**
### Owner

Indicates the schema or schemas to which SqlProvider will try to provide access to.
Multiple schemas can be indicated, separated by commas or semicolons.
Defaults to "public".

*)

let [<Literal>] owner = "public, admin, references"


type sql =
    SqlDataProvider<
        dbVendor,
        connString,
        "",         //ConnectionNameString can be left empty 
        resPath,
        indivAmount,
        useOptTypes,
        owner>

        
(**
### OnConflict

Beginning with version 9.5, [PostgreSQL supports the ON CONFLICT clause to INSERT statements.](https://www.postgresql.org/docs/current/static/sql-insert.html#SQL-ON-CONFLICT)
It allows the user to specify if a unique constraint violation should be solved by ignoring the statement (DO NOTHING) or updating existing rows (DO UPDATE).
SqlProvider can leverage this feature by setting the `OnConflict` property on a row object. Setting it to `DoNothing` will add the DO NOTHING clause. Setting it to `Update` will add a DO UPDATE clause that updates all existing columns if the conflict happened on the primary key constraint.

It is not currently supported to emit a DO UPDATE clause for non-primary key unique constraints.
*)

let ctx = sql.GetDataContext()

let orders = ctx.Main.Orders

let row = orders.Create()
row.CustomerId <- customer.CustomerId
row.EmployeeId <- employee.EmployeeId
row.Freight <- 10M
row.OrderDate <- now.AddDays(-1.0)
row.RequiredDate <- now.AddDays(1.0)
row.ShipAddress <- "10 Downing St"
row.ShipCity <- "London"
row.ShipName <- "Dragons den"
row.ShipPostalCode <- "SW1A 2AA"
row.ShipRegion <- "UK"
row.ShippedDate <- now

row.OnConflict <- FSharp.Data.Sql.Common.OnConflict.Update

ctx.SubmitUpdates()
