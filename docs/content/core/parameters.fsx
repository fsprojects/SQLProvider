(*** hide ***)
#I @"../../files/sqlite"
(*** hide ***)
#I "../../../bin"
(*** hide ***)
#r "FSharp.Data.SqlProvider.dll"
open FSharp.Data.Sql
(**


# SQL Provider Static Parameters

## Global parameters

These are the "common" parameters used by all SqlProviders.

All static parameters must be known at compile time, for strings this can be 
achieved by adding the `[<Literal>]` attribute if you are not passing it inline.

### ConnectionString

This is the connection string commonly used to connect to a database server 
instance.  See the documentation on your desired database type to find out 
more. 
*)

[<Literal>]
let sqliteConnectionString =
    "Data Source=" + __SOURCE_DIRECTORY__ + @"\northwindEF.db;Version=3"

(**
### ConnectionStringName

Instead of storing the connection string in the source code / `fsx` script, you
can store values in the `App.config` file.  This is the name of the
connectionString key/value pair stored in App.config 
*)

let connexStringName = "MyDatabase"

(**
### DatabaseVendor

Select enumeration from `Common.DatabaseProviderTypes` to specify which database
type the provider will be connecting to.
*)

[<Literal>]
let dbVendor = Common.DatabaseProviderTypes.SQLITE

(**
### ResolutionPath

When using database vendors other than SQL Server, Access and ODBC, a third party driver
is required. This parameter should point to an absolute or relative directory where the
relevant assemblies are located. See the database vendor specific page for more details.
*)

[<Literal>]
let resolutionPath =
    __SOURCE_DIRECTORY__ + @"..\..\..\files\sqlite"

(**
### IndividualsAmount

Number of instances to retrieve when using the [individuals](core/individuals.html) feature.
Default is 1000.
*)

let indivAmt = 500


(**
### UseOptionTypes

If set to true, all nullable fields will be represented by F# option types.  If false, nullable
fields will be represented by the default value of the column type - this is important because
the provider will return 0 instead of null, which might cause problems in some scenarios.
*)
[<Literal>]
let useOptionTypes = true


(**
## Platform Considerations

### MSSQL

No extra parameters.

### Oracle

Currently there is only one added parameter.

#### Owner (Oracle only)

This sets the owner of the scheme when running queries against the server.

### SQLite

No extra parameters.

### PostgreSQL

No extra parameters.

### MySQL

No extra parameters.

### ODBC

No extra parameters.
*)


(**
###Example
It is recommended to use named static parameters in your type provider definition like so

*)
type sql = SqlDataProvider<
            ConnectionString = sqliteConnectionString,
            DatabaseVendor = dbVendor,
            ResolutionPath = resolutionPath,
            UseOptionTypes = useOptionTypes
          >

