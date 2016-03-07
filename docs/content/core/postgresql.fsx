(*** hide ***)
#I @"../../files/sqlite"
(*** hide ***)
#I "../../../bin"
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

### ConnectionString

Basic connection string used to connect to PostgreSQL instance; typical 
connection strings for the driver apply here. See
[PostgreSQL Connecting Strings Documentation](https://github.com/npgsql/Npgsql/wiki/User-Manual)
for a complete list of connection string options.

*)

let [<Literal>] connString = "Server=localhost;Database=test;User Id=test;Password=test"

(**
### ConnectionStringName

Instead of storing the connection string in the source code / `fsx` script, you
can store values in the `App.config` file.  This is the name of the
connectionString key/value pair stored in App.config (TODO: confirm file name).
*)

// found in App.config (TOOD: confirm)
let [<Literal>] connexStringName = "DefaultConnectionString"

(**
### DatabaseVendor

From the `FSharp.Data.Sql.Common.DatabaseProviderTypes` enumeration. For PostgreSQL,
use `Common.DatabaseProviderTypes.POSTGRESQL`.

*)

let [<Literal>] dbVendor = Common.DatabaseProviderTypes.POSTGRESQL

(**
### Resolution Path

Path to search for assemblies containing database vendor specific connections 
and custom types. Type the path where `Npgsql.Data.dll` is stored.

*)

let [<Literal>] resPath = @"C:\Projects\Libs\Npgsql\"

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

let sql =
    SqlDataProvider<
        connString,
        dbVendor,
        resPath,
        indivAmount,
        useOptTypes>
