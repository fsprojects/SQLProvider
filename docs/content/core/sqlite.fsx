(*** hide ***)
#I @"../../../bin"
(*** hide ***)
[<Literal>]
let connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"/../../../tests/SqlProvider.Tests/scripts/northwindEF.db;Version=3"
(*** hide ***)
[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + @"/../../../tests/SqlProvider.Tests/libs"
#r "FSharp.Data.SqlProvider.dll"
open System
open FSharp.Data.Sql
(**

# SQLite

*)

type sql = SqlDataProvider<
                Common.DatabaseProviderTypes.SQLITE, 
                connectionString, 
                ResolutionPath = resolutionPath, 
                CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL>

let ctx = sql.GetDataContext()

let customers = 
    ctx.Main.Customers
    |> Seq.map(fun c -> c.ContactName)
    |> Seq.toList

(**

# CRUD

When you do insert operation, after .SubmitUpdates call you can get inserted rowid like this:

*)

let myCustomer = ctx.Main.Customers.``Create(CompanyName)``("MyCompany")
ctx.SubmitUpdates()
let rowid = myCustomer.GetColumn("rowid") : int
