(*** hide ***)
#I @"../../files/sqlite"
(*** hide ***)
#I "../../../bin/net451"
(*** hide ***)
[<Literal>]
let connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"\northwindEF.db;Version=3"
(*** hide ***)
[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + @"..\..\..\files\sqlite"
#r "FSharp.Data.SqlProvider.dll"
open FSharp.Data.Sql

(**
# MSSQL Provider
## Parameters


### DatabaseVendor

From the `FSharp.Data.Sql.Common.DatabaseProviderTypes` enumeration. For MSSQL,
use `Common.DatabaseProviderTypes.MSSQLSERVER`.

*)

let [<Literal>] dbVendor = Common.DatabaseProviderTypes.MSSQLSERVER

(**
### ConnectionString

Basic connection string used to connect to MSSQL instance; typical
connection strings for the driver apply here. See
[MSSQL Connecting Strings Documentation](https://www.connectionstrings.com/sql-server/)
for a complete list of connection string options.

*)

let [<Literal>] connString = "Server=localhost;Database=test;User Id=test;Password=test"

(**
### ConnectionStringName

Instead of storing the connection string in the source code / `fsx` script, you
can store values in the `App.config` file.  This is the name of the
connectionString key/value pair stored in App.config.
*)

// found in App.config
let [<Literal>] connexStringName = "DefaultConnectionString"

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

type sql =
    SqlDataProvider<
        dbVendor,
        connString,
        IndividualsAmount = indivAmount,
        UseOptionTypes = useOptTypes>

(**

Because MSSQL databases can be huge, there is an optional constructor parameter `TableNames` that can be used as a filter.

*)
