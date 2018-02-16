(*** hide ***)
#I @"../../files/sqlite"
(*** hide ***)
#I "../../../bin/net451"
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

Number of instances to retrieve when using the [individuals](individuals.html) feature.
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

TableNames to filter amount of tables.

### Oracle

TableNames to filter amount of tables, and Owner.

#### Owner (Used by Oracle, MySQL and PostgreSQL)

This has different meanings when running queries against different database vendors

For PostgreSQL, this sets the schema name where the target tables belong to
For MySQL, this sets the database name (Or schema name, for MySQL, it's the same thing)
For Oracle, this sets the owner of the scheme

### SQLite

The additional [SQLiteLibrary parameter](sqlite.html#SQLiteLibrary) can be used to specify
which SQLite library to load.

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

(**

# SQL Provider Data Context Parameters

Besides the static parameters the `.GetDataContext(...)` method has optional parameters:

* connectionString - The database connection string on runtime.
* resolutionPath - The location to look for dynamically loaded assemblies containing database vendor specific connections and custom types
* transactionOptions - TransactionOptions for the transaction created on SubmitChanges.
* commandTimeout - SQL command timeout. Maximum time for single SQL-command in seconds.
* selectOperations - Execute select-clause operations in SQL database rahter than .NET-side.
			  
*)
