(*** hide ***)
#I @"../../files/msaccess"
(*** hide ***)
#I "../../../bin/lib/netstandard2.0"
(*** hide ***)
#r @"../../../bin/lib/netstandard2.0/FSharp.Data.SqlProvider.dll"

//type mdb = SqlDataProvider< "Provider=Microsoft.Jet.OLEDB.4.0; Data Source= C:\\ACCESS\\BT.mdb", Common.DatabaseProviderTypes.MSACCESS, "c:\\ACCESS" , 100, true >

(**
# SQL Provider for MSAccess

MSAccess is based on System.Data.OleDb. Use ACE drivers for databases > Access 2007 (with
.accdb extension). For databases < 2007 (with .mdb extension),
JET drivers can be used, although ACE will also work.

[http://www.microsoft.com/download/en/confirmation.aspx?id=23734](http://www.microsoft.com/download/en/confirmation.aspx?id=23734)

SQLProvider with Microsoft Access is available via both NuGet-Packages:
- NuGet: SQLProvider.MsAccess - Fixed libraries. Doesn't need resolution path. TypeProvider class: FSharp.Data.Sql.MsAccess.SqlDataProvider
- NuGet: SQLProvider - Dynamic version. TypeProvider class: FSharp.Data.Sql.SqlDataProvider


## Parameters

### ConnectionString

A connection string used to connect to Microsoft Access instance; typical
connection strings for the driver apply here. See
(MSAccess Connection Strings Documentation) []
for a complete list of connection string options.

*)
//TODO: link to reference

[<Literal>]
let connectionString1 = "Provider=Microsoft.ACE.OLEDB.12.0; Data Source= " +  __SOURCE_DIRECTORY__ + @"..\..\..\files\msaccess\Northwind.accdb"

[<Literal>]
let connectionString2 =
    "Provider=Microsoft.Jet.OLEDB.4.0; Data Source= " +
    __SOURCE_DIRECTORY__ +
    @"..\..\..\files\msaccess\Northwind.mdb"

(**
### ConnectionStringName

Instead of storing the connection string in the source code / `fsx` script, you
can store values in the `App.config` file. This is the name of the
connectionString key/value pair stored in App.config (TODO: confirm file name).
*)

// found in App.config (TODO:confirm)
let connexStringName = "DefaultConnectionString"

(**
### DatabaseVendor

From the `FSharp.Data.Sql.Common.DatabaseProviderTypes` enumeration. For MSAccess,
use `Common.DatabaseProviderTypes.MSACCESS`.

*)

let dbVendor = FSharp.Data.Sql.Common.DatabaseProviderTypes.MSACCESS

(**
### ResolutionPath

Path to search for assemblies containing database vendor-specific connections
and custom types. Type the path where `Npgsql.Data.dll` is stored.

*)
[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__

(**
### IndividualsAmount

Sets the count to load for each individual. See (individuals)[individuals.html]
for further info.

### UseOptionTypes

If true, F# option types will be used in place of nullable database columns.
If false, you will always receive the default value of the column's type, even
if it is null in the database. You can also use VALUE_OPTION.

*)

[<Literal>]
let useOptTypes = FSharp.Data.Sql.Common.NullableColumnType.OPTION


#r "FSharp.Data.SqlProvider.dll"
open FSharp.Data.Sql

//type accdb = SqlDataProvider<Common.DatabaseProviderTypes.MSACCESS , connectionString1, ResolutionPath=resolutionPath >
//let accdbctx = accdb.GetDataContext()
//
//let accdbcustomers = accdbctx.Northwind.Customers|> Seq.toArray

type mdb = SqlDataProvider<Common.DatabaseProviderTypes.MSACCESS, connectionString2, ResolutionPath=resolutionPath, UseOptionTypes=useOptTypes >
let mdbctx = mdb.GetDataContext()

let mdbcustomers =
    mdbctx.Northwind.Customers
    |> Seq.map(fun c ->
        c.ColumnValues |> Seq.toList)
    |> Seq.toList
