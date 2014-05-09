(*** hide ***)
#I @"../../files/msaccess"
(*** hide ***)
#I "../../../bin"


//type mdb = SqlDataProvider< "Provider=Microsoft.Jet.OLEDB.4.0; Data Source= C:\\ACCESS\\BT.mdb", Common.DatabaseProviderTypes.MSACCESS, "c:\\ACCESS" , 100, true >
[<Literal>]
(*** hide ***)
let connectionString1 = "Provider=Microsoft.ACE.OLEDB.12.0; Data Source= " + __SOURCE_DIRECTORY__ + "..\\..\\..\\files\\msaccess\\Northwind.accdb"
[<Literal>]
(*** hide ***)
let connectionString2 = "Provider=Microsoft.Jet.OLEDB.4.0; Data Source= " + __SOURCE_DIRECTORY__ + "..\\..\\..\\files\\msaccess\\Northwind.mdb"

[<Literal>]
(*** hide ***)
let resolutionPath = __SOURCE_DIRECTORY__

(**
# SQL Provider for MSAccess

MSAccess is based on System.Data.OleDb. For databases > Access 2007 (with .accdb extension), use ACE drivers. For dbs < 2007 (with .mdb extension), JET drivers can be used, although ACE will also work.
*)

#r "FSharp.Data.SqlProvider.dll"

open FSharp.Data.Sql

type accdb = SqlDataProvider< connectionString1, Common.DatabaseProviderTypes.MSACCESS , resolutionPath >
let accdbctx = accdb.GetDataContext()

let accdbcustomers = accdbctx.``[Northwind].[Customers]``|> Seq.toArray

type mdb = SqlDataProvider< connectionString2, Common.DatabaseProviderTypes.MSACCESS , resolutionPath >
let mdbctx = mdb.GetDataContext()

let mdbcustomers = mdbbctx.``[Northwind].[Customers]``|> Seq.toArray
