#r @"..\..\bin\FSharp.Data.SqlProvider.dll"

open System
open FSharp.Data.Sql

[<Literal>]
let connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"\northwindEF.db;Version=3"

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ 


open FSharp.Data.Sql


type sql = SqlDataProvider<connectionString, DatabaseVendor = Common.DatabaseProviderTypes.SQLITE, ResolutionPath = resolutionPath >

let ctx = sql.GetDataContext()

let customers = ctx.``[MAIN].[CUSTOMERS]`` |> Seq.toArray

let firstCustomer = customers.[0]
let name = firstCustomer.CONTACTNAME

let orders = firstCustomer.FK_Orders_0_0 |> Seq.toArray

let customersQuery =
    query { for customer in ctx.``[MAIN].[CUSTOMERS]`` do
            select customer } |> Seq.toArray

let filteredQuery =
    query { for customer in ctx.``[MAIN].[CUSTOMERS]`` do
            where (customer.CONTACTNAME = "John Smith")
            select customer } |> Seq.toArray

let multipleFilteredQuery =
    query { for customer in ctx.``[MAIN].[CUSTOMERS]`` do
            where ((customer.CONTACTNAME = "John Smith" && customer.COUNTRY = "England") 
                    || customer.CONTACTNAME = "Joe Bloggs")
            select customer } |> Seq.toArray


let automaticJoinQuery =
   query { for customer in ctx.``[MAIN].[CUSTOMERS]`` do
           for order in customer.FK_Orders_0_0 do
           where (customer.CONTACTNAME = "John Smith")
           select (customer,order) } |> Seq.toArray

let explicitJoinQuery =
   query { for customer in ctx.``[MAIN].[CUSTOMERS]`` do
           join order in ctx.``[MAIN].[CUSTOMERS]`` on (customer.CUSTOMERID = order.CUSTOMERID)
           where (customer.CONTACTNAME = "John Smith")
           select (customer,order) } |> Seq.toArray

let ordersQuery =
   query { for customer in ctx.``[MAIN].[CUSTOMERS]`` do
           for order in customer.FK_Orders_0_0 do
           where (customer.CONTACTNAME = "John Smith")
           select (customer.CONTACTNAME, order.ORDERDATE, order.SHIPADDRESS) } |> Seq.toArray

let BERGS = ctx.``[MAIN].[CUSTOMERS]``.Individuals.BERGS


let christina = ctx.``[MAIN].[CUSTOMERS]``.Individuals.``As CONTACTNAME``.``BERGS, Christina Berglund``

