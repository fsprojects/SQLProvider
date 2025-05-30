(*** hide ***)
#I @"../../files/sqlite"
(*** hide ***)
#I "../../../bin/lib/netstandard2.0"
(*** hide ***)
let connectionString =
    "Data Source=" + __SOURCE_DIRECTORY__ + @"\northwindEF.db;Version=3"
(*** hide ***)
[<Literal>]
let resolutionPath =
    __SOURCE_DIRECTORY__ + @"..\..\..\files\sqlite"
#r "FSharp.Data.SqlProvider.dll"
#r "FSharp.Data.SqlProvider.Common.dll"
open FSharp.Data.Sql
(**


# PostgreSQL Provider

SQLProvider with Oracle is available via both NuGet-Packages:

- NuGet: [SQLProvider.PostgreSql](https://www.nuget.org/packages/SQLProvider.PostgreSql) - Fixed Npgsql. Doesn't need resolution path. TypeProvider class: `FSharp.Data.Sql.PostgreSql.SqlDataProvider`
- NuGet: [SQLProvider](https://www.nuget.org/packages/SQLProvider) - Dynamic version. TypeProvider class: `FSharp.Data.Sql.SqlDataProvider`


## Parameters

### DatabaseVendor

From the `FSharp.Data.Sql.Common.DatabaseProviderTypes` enumeration. For PostgreSQL,
use `Common.DatabaseProviderTypes.POSTGRESQL`.

*)

let [<Literal>] dbVendor = Common.DatabaseProviderTypes.POSTGRESQL

(**

### ConnectionString

A basic connection string used to connect to PostgreSQL instance; typical
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

Path to search for assemblies containing database vendor-specific connections
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
if it is null in the database. You can also use VALUE_OPTION.

*)

let [<Literal>] useOptTypes  = FSharp.Data.Sql.Common.NullableColumnType.OPTION

(**
### Owner

Indicates the schema or schemas to which SqlProvider will try to provide access to.
Multiple schemas can be indicated and separated by commas or semicolons.
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

