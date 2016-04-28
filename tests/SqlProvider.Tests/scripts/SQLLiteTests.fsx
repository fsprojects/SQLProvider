#r @"../../../bin/FSharp.Data.SqlProvider.dll"

open System
open FSharp.Data.Sql

[<Literal>]
let connectionString = @"Data Source=" + __SOURCE_DIRECTORY__ + @"/northwindEF.db;Version=3"

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + "/../libs"


open FSharp.Data.Sql


type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, ResolutionPath = resolutionPath, CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL>

let ctx = sql.GetDataContext()

let customers = ctx.Main.Customers |> Seq.toArray

let firstCustomer = customers.[0]
let name = firstCustomer.ContactName

let orders = firstCustomer.``main.Orders by CustomerID`` |> Seq.toArray

type Simple = {First : string}

type Dummy<'t> = D of 't

let employeesFirstName1 =
    query {
        for emp in ctx.Main.Customers do
        select (D {First=emp.ContactName})
    } |> Seq.toList

let employeesFirstName2 =
    query {
        for emp in ctx.Main.Customers do
        select ({First=emp.ContactName} |> D)
    } |> Seq.toList

let customersQuery =
    query { for customer in ctx.Main.Customers do
            select customer } |> Seq.toArray


let customersQuerySortedByName =
    query { for customer in ctx.Main.Customers do
            sortBy customer.CompanyName
            thenBy customer.ContactName
            select (customer.CompanyName, customer.ContactName) } |> Seq.toArray

let filteredQuery =
    query { for customer in ctx.Main.Customers do
            where (customer.ContactName = "John Smith")
            select customer } |> Seq.toArray

let multipleFilteredQuery =
    query { for customer in ctx.Main.Customers do
            where ((customer.ContactName = "John Smith" && customer.Country = "England")
                    || customer.ContactName = "Joe Bloggs")
            select customer } |> Seq.toArray

let automaticJoinQuery =
   query { for customer in ctx.Main.Customers do
           for order in customer.``main.Orders by CustomerID`` do
           where (customer.ContactName = "John Steel")
           select (customer,order) } |> Seq.toArray

let explicitJoinQuery =
   query { for customer in ctx.Main.Customers do
           join order in ctx.Main.Orders on (customer.CustomerId = order.CustomerId)
           where (customer.ContactName = "John Smith")
           select (customer,order) } |> Seq.toArray

let ordersQuery =
   query { for customer in ctx.Main.Customers do
           for order in customer.``main.Orders by CustomerID`` do
           where (customer.ContactName = "John Smith")
           select (customer.ContactName, order.OrderDate, order.ShipAddress) } |> Seq.toArray

let BERGS = ctx.Main.Customers.Individuals.BERGS


let christina = ctx.Main.Customers.Individuals.``As ContactName``.``BERGS, Christina Berglund``.Address

type Employee = {
    EmployeeId : int64
    FirstName : string
    LastName : string
    HireDate : DateTime
}

let mapEmployee (dbRecord:sql.dataContext.``main.EmployeesEntity``) : Employee =
    { EmployeeId = dbRecord.EmployeeId
      FirstName = dbRecord.FirstName
      LastName = dbRecord.LastName
      HireDate = dbRecord.HireDate }
