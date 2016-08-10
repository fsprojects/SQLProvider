(*** hide ***)
#I "../../files/sqlite"
(*** hide ***)
#I "../../../bin"
(*** hide ***)
#r @"../../../bin/FSharp.Data.SqlProvider.dll"

(*** hide ***)
[<Literal>]
let connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"/../../../tests/SqlProvider.Tests/scripts/northwindEF.db;Version=3"

(*** hide ***)
[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + @"/../"

(**
# CRUD sample
*)

open System
open FSharp.Data.Sql

type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE,
                           connectionString,
                           ResolutionPath = resolutionPath,
                           CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL>

let ctx = sql.GetDataContext()

let orders = ctx.Main.Orders

let customer = ctx.Main.Customers |> Seq.head 
let employee = ctx.Main.Employees |> Seq.head
let now = DateTime.Now

(**
Create a new row
*)

let row = orders.Create()
row.CustomerId <- customer.CustomerId
row.EmployeeId <- employee.EmployeeId
row.Freight <- 10M
row.OrderDate <- now.AddDays(-1.0)
row.RequiredDate <- now.AddDays(1.0)
row.ShipAddress <- "10 Downing St"
row.ShipCity <- "London"
row.ShipName <- "Dragons den"
row.ShipPostalCode <- "SW1A 2AA"
row.ShipRegion <- "UK"
row.ShippedDate <- now

(**
Submit updates to the database
*)
ctx.SubmitUpdates()

(**
After update your item (row) is having Id property.

You can also create with longer ``Create(...)``(parameters)-method like this:

*)

let emp = ctx.Main.Employees.``Create(FirstName, LastName)``("Don", "Syme")

(**

Delete the row
*)
row.Delete()

(**
Submit updates to the database
*)
ctx.SubmitUpdates()

(** 

Inside SubmitUpdate transaction is created by default TransactionOption, which is Required: Shares a transaction, if one exists, and creates a new transaction if necessary. So e.g. if you have query-operation before SubmitUpdates, you may want to create your own transaction to wrap these to the same transaction.

SQLProvider also support also async database operations: 

*)

ctx.SubmitUpdatesAsync() |> Async.StartAsTask
