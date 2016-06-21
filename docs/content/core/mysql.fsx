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
[<Literal>]
let connString  = "Server=localhost;Database=HR;User=root;Password=password"

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
[<Literal>]
let dbVendor    = Common.DatabaseProviderTypes.MYSQL

(**
### Resolution Path

Path to search for assemblies containing database vendor specific connections and custom types. Type the path where
`Mysql.Data.dll` is stored. Both absolute and relative paths are supported.

*)
[<Literal>]
let resPath = __SOURCE_DIRECTORY__ + @"/../../../packages/scripts/MySql.Data/lib/net45"

(**
### Individuals Amount

Sets the count of records to load for each table. See [individuals](individuals.html) for further info.

*)
[<Literal>]
let indivAmount = 1000

(**
### Use Option Types

If true, F# option types will be used in place of nullable database columns.  If false, you will always receive the default value of the column's type even if it is null in the database.

*)
[<Literal>]
let useOptTypes = true

(**
### Example

*)

type sql = SqlDataProvider<
                dbVendor,
                connString,
                ResolutionPath = resPath,
                IndividualsAmount = indivAmount,
                UseOptionTypes = useOptTypes,
                Owner = "HR"
            >
let ctx = sql.GetDataContext()

let employees = 
    ctx.Hr.Employees 
    |> Seq.map (fun e -> e.ColumnValues |> Seq.toList)
    |> Seq.toList

(**


## Caveats / Additional Info

Check [General](general.html), [Static Parameters](parameters.html) and [Querying](querying.html) documentation.

More to be added.
*)
