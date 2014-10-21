#r @"..\..\bin\FSharp.Data.SqlProvider.dll"

open System
open FSharp.Data.Sql

[<Literal>]
let connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"\northwindEF.db;Version=3"

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ 


open FSharp.Data.Sql


type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, ResolutionPath = resolutionPath >

let ctx = sql.GetDataContext()

let customers = ctx.Customers |> Seq.toArray

let firstCustomer = customers.[0]
let name = firstCustomer.ContactName

let orders = firstCustomer.FK_Orders_0_0 |> Seq.toArray

let customersQuery =
    query { for customer in ctx.Customers do
            select customer } |> Seq.toArray

let filteredQuery =
    query { for customer in ctx.Customers do
            where (customer.ContactName = "John Smith")
            select customer } |> Seq.toArray

let multipleFilteredQuery =
    query { for customer in ctx.Customers do
            where ((customer.ContactName = "John Smith" && customer.Country = "England") 
                    || customer.ContactName = "Joe Bloggs")
            select customer } |> Seq.toArray


let automaticJoinQuery =
   query { for customer in ctx.Customers do
           for order in customer.FK_Orders_0_0 do
           where (customer.ContactName = "John Smith")
           select (customer,order) } |> Seq.toArray

let explicitJoinQuery =
   query { for customer in ctx.Customers do
           join order in ctx.Customers on (customer.CustomerId = order.CustomerId)
           where (customer.ContactName = "John Smith")
           select (customer,order) } |> Seq.toArray

let ordersQuery =
   query { for customer in ctx.Customers do
           for order in customer.FK_Orders_0_0 do
           where (customer.ContactName = "John Smith")
           select (customer.ContactName, order.OrderDate, order.ShipAddress) } |> Seq.toArray

let BERGS = ctx.Customers.Individuals.BERGS


let christina = ctx.Customers.Individuals.``As ContactName``.``BERGS, Christina Berglund``

