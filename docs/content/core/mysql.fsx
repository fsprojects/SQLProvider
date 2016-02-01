(*** hide ***)
#I @"../../files/sqlite"
(*** hide ***)
#I "../../../bin"
(*** hide ***)
#r "FSharp.Data.SqlProvider.dll"
open FSharp.Data.Sql
(**


# MySQL Provider

## Parameters

### ConnectionString

Basic connection string used to connect to MySQL instance; typical connection 
string parameters apply here.  See 
[MySQL Connector/NET Connection Strings Documentation](https://dev.mysql.com/doc/connector-net/en/connector-net-connection-options.html) 
for a complete list of connection string options.

*)

let connString  = "Server=localhost;Database=test;User=test;Password=test"

(**
### ConnectionStringName

Instead of storing the connection string in the source code, you
can store it in the `App.config` file.  This is the name of the
connectionString key/value pair stored in App.config (TODO: confirm filename).
*)

// found in App.config (TODO: confirm)
let connexStringName = "DefaultConnectionString"

(**
### Database Vendor

Use `MYSQL` from the `FSharp.Data.Sql.Common.DatabaseProviderTypes` enumeration.

*)

let dbVendor    = Common.DatabaseProviderTypes.MYSQL

(**
### Resolution Path

Path to search for assemblies containing database vendor specific connections and custom types. Type the path where
`Mysql.Data.dll` is stored. Both absolute and relative paths are supported.

*)

let resPath     = @"C:\Users\JHaas\Documents\Projects\jeroldhaas\CompleteFSharpWebApplication\CompleteFSharWebApplication\packages\MySql.Data.6.9.3\lib\net45"

(**
### Individuals Amount

Sets the count of records to load for each table. See [individuals](individuals.html) for further info.

*)

let indivAmount = 1000

(**
### Use Option Types

If true, F# option types will be used in place of nullable database columns.  If false, you will always receive the default value of the column's type even if it is null in the database.

*)
let useOptTypes = true

(**
### Example

*)

type sql = SqlDataProvider<
                connString,
                dbVendor,
                resPath,
                indivAmount,
                useOptTypes
            >
let ctx = () // TODO: resolve

(**


## Caveats / Additional Info

Check [General](general.html), [Static Parameters](parameters.html) and [Querying](querying.html) documentation.

More to be added.
*)
