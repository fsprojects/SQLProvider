(*** hide ***)
#I @"../../files/msaccess"
(*** hide ***)
#I "../../../bin"


//type mdb = SqlDataProvider< "Provider=Microsoft.Jet.OLEDB.4.0; Data Source= C:\\ACCESS\\BT.mdb", Common.DatabaseProviderTypes.MSACCESS, "c:\\ACCESS" , 100, true >
[<Literal>]
(*** hide ***)
[<Literal>]
(*** hide ***)

[<Literal>]
(*** hide ***)

(**
# SQL Provider for MSAccess

MSAccess is based on System.Data.OleDb. For databases > Access 2007 (with 
.accdb extension), use ACE drivers. For dbs < 2007 (with .mdb extension), 
JET drivers can be used, although ACE will also work.

## Parameters

### ConnectionString

Basic connection string used to connect to PostgreSQL instance; typical 
connection strings for the driver apply here. See
(MSAccess Connection Strings Documentation) [] 
for a complete list of connection string options.

*)
//TODO: link to reference
let connectionString1 =
    "Provider=Microsoft.ACE.OLEDB.12.0; Data Source= " +
    __SOURCE_DIRECTORY__ +
    @"..\..\..\files\msaccess\Northwind.accdb"
let connectionString2 = 
    "Provider=Microsoft.Jet.OLEDB.4.0; Data Source= " +
    __SOURCE_DIRECTORY__ + 
    @"..\..\..\files\msaccess\Northwind.mdb"

(**
### ConnectionStringName

Instead of storing the connection string in the source code / `fsx` script, you
can store values in the `App.config` file.  This is the name of the
connectionString key/value pair stored in App.config (TODO: confirm file name).
*)

// found in App.config (TODO:confirm)
let connexStringName = "DefaultConnectionString"

(**
### DatabaseVendor

From the `FSharp.Data.Sql.Common.DatabaseProviderTypes` enumeration. For MSAccess,
use `Common.DatabaseProviderTypes.MSACCESS`.

*)

let dbVendor = Common.DatabaseProviderTypes.MSSQL

(**
### ResolutionPath

Path to search for assemblies containing database vendor specific connections 
and custom types. Type the path where `Npgsql.Data.dll` is stored.

*)

let resolutionPath = __SOURCE_DIRECTORY__

(**
### IndividualsAmount

Sets the count to load for each individual. See (individuals)[individuals.html] 
for further info.

*)

let useOptTypes = true


#r "FSharp.Data.SqlProvider.dll"
open FSharp.Data.Sql

type accdb = SqlDataProvider< connectionString1, Common.DatabaseProviderTypes.MSACCESS , resolutionPath >
let accdbctx = accdb.GetDataContext()

let accdbcustomers = accdbctx.``[Northwind].[Customers]``|> Seq.toArray

type mdb = SqlDataProvider< connectionString2, Common.DatabaseProviderTypes.MSACCESS , resolutionPath >
let mdbctx = mdb.GetDataContext()

let mdbcustomers = mdbbctx.``[Northwind].[Customers]``|> Seq.toArray
