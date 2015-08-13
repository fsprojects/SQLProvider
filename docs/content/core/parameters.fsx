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


# SQL Provider Parameters

## Global parameters

These are the "common" parameters used by all SqlProviders.

### ConnectionString

This is the connection string commonly used to connect to a database server 
instance.  See the documentation on your desired database type to find out 
more.
*)

let mysqlConnexString =
    "Server=localhost; Database=test; User=test; Password=test"

(**
### ConnectionStringName

Instead of storing the connection string in the source code / `fsx` script, you
can store values in the `App.config` file.  This is the name of the
connectionString key/value pair stored in App.config (TODO: confirm file name).
*)

// found in App.config (TOOD: confirm)
let connexStringName = "DefaultConnectionString"

(**
### IndividualsAmount

Number of instances to query when getting an individual (TODO: clarify).
*)

let indivAmt = 5000

(**
### DatabaseVendor

Select enumeration from `Common.DatabaseProviderTypes` to specify which database
type the provider will be connecting to.
*)

let dbVendor = Common.DatabaseProviderTypes.SQLITE

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

let sql = SqlDataProvider<
            mysqlConnexString,
            connexStringName,
            dbVendor,
            resolutePath,
            useOptionTypes
          >

