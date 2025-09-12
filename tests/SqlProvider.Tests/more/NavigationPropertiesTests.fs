#if INTERACTIVE
#I @"../../bin/lib/net48/"
#r "FSharp.Data.SqlProvider.Common.dll"
#r "FSharp.Data.SqlProvider.dll"
#r @"../../packages/tests/NUnit/lib/netstandard2.0/nunit.framework.dll"
#else
module NavigationPropertiesTests
#endif

open System
open FSharp.Data.Sql
open System.Linq
open NUnit.Framework

[<Literal>]
let connectionString =  @"Data Source=" + __SOURCE_DIRECTORY__ + @"/../db/northwindEF.db;Version=3;Read Only=false;FailIfMissing=True;"

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + "/../libs"

// If you want to run these in Visual Studio Test Explorer, please install:
// Tools -> Extensions and Updates... -> Online -> NUnit Test Adapter for Visual Studio
// http://nunit.org/index.php?p=vsTestAdapter&r=2.6.4

type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, CaseSensitivityChange=Common.CaseSensitivityChange.ORIGINAL, ResolutionPath = resolutionPath, SQLiteLibrary=Common.SQLiteLibrary.SystemDataSQLite, UseOptionTypes=Common.NullableColumnType.VALUE_OPTION>
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %O")

[<Test>]
let ``basic navigation property usage`` () =
    let ctx = sql.GetDataContext()
    
    let customerOrders =
        query {
            for customer in ctx.Main.Customers do
            for order in customer.``main.Orders by CustomerID`` do
            select (customer.CompanyName, order.OrderDate)
        } |> Seq.toList
    
    Assert.IsNotEmpty(customerOrders)

[<Test>]
let ``multiple level navigation`` () =
    let ctx = sql.GetDataContext()
    
    let customerOrderDetails =
        query {
            for customer in ctx.Main.Customers do
            for order in customer.``main.Orders by CustomerID`` do
            for detail in order.``main.OrderDetails by OrderID`` do
            select (customer.CompanyName, order.OrderDate, detail.Quantity, detail.UnitPrice)
        } |> Seq.toList
    
    Assert.IsNotEmpty(customerOrderDetails)

[<Test>]
let ``navigation with filtering`` () =
    let ctx = sql.GetDataContext()
    
    let recentCustomerOrders =
        query {
            for customer in ctx.Main.Customers do
            for order in customer.``main.Orders by CustomerID`` do
            where (order.OrderDate.Value >= DateTime(1998, 1, 1))
            select (customer.CompanyName, order.OrderDate)
        } |> Seq.toList
    
    Assert.IsNotEmpty(recentCustomerOrders)

[<Test>]
let ``navigation with aggregation`` () =
    let ctx = sql.GetDataContext()
    
    let customerOrderCounts =
        query {
            for customer in ctx.Main.Customers do
            select (
                customer.CompanyName,
                query {
                    for order in customer.``main.Orders by CustomerID`` do
                    count
                }
            )
        } |> Seq.toList
    
    Assert.IsNotEmpty(customerOrderCounts)

[<Test>]
let ``reverse navigation properties`` () =
    let ctx = sql.GetDataContext()
    
    let orderCustomers =
        query {
            for order in ctx.Main.Orders do
            for customer in order.``main.Customers by CustomerID`` do
            select (order.OrderId, customer.CompanyName)
        } |> Seq.toList
    
    Assert.IsNotEmpty(orderCustomers)

[<Test>]
let ``navigation with sorting`` () =
    let ctx = sql.GetDataContext()
    
    let sortedCustomerOrders =
        query {
            for customer in ctx.Main.Customers do
            for order in customer.``main.Orders by CustomerID`` do
            sortBy customer.CompanyName
            thenByDescending order.OrderDate
            select (customer.CompanyName, order.OrderDate)
        } |> Seq.toList
    
    Assert.IsNotEmpty(sortedCustomerOrders)

[<Test>]
let ``navigation with date filtering`` () =
    let ctx = sql.GetDataContext()
    
    let recentCustomerActivity =
        query {
            for customer in ctx.Main.Customers do
            for order in customer.``main.Orders by CustomerID`` do
            where (order.OrderDate.IsSome && order.OrderDate.Value >= DateTime(1998, 1, 1) && order.OrderDate.Value < DateTime(1999, 1, 1))
            select (customer.CompanyName, order.OrderDate)
        } |> Seq.toList
    
    Assert.IsNotEmpty(recentCustomerActivity)

[<Test>]
let ``navigation with distinct results`` () =
    let ctx = sql.GetDataContext()
    
    let customersWithLargeOrders =
        query {
            for customer in ctx.Main.Customers do
            for order in customer.``main.Orders by CustomerID`` do
            for detail in order.``main.OrderDetails by OrderID`` do
            where (float detail.UnitPrice * float detail.Quantity > 100.0)
            select customer.CompanyName
            distinct
        } |> Seq.toList
    
    Assert.IsNotEmpty(customersWithLargeOrders)

[<Test>]
let ``navigation with conditional logic`` () =
    let ctx = sql.GetDataContext()
    
    let customerCategories =
        query {
            for customer in ctx.Main.Customers do
            select (
                customer.CompanyName,
                let orderCount = query {
                    for order in customer.``main.Orders by CustomerID`` do
                    count
                }
                if orderCount > 10 then "High Volume"
                elif orderCount > 5 then "Medium Volume"
                else "Low Volume"
            )
        } |> Seq.toList
    
    Assert.IsNotEmpty(customerCategories)

[<Test>]
let ``navigation with pagination`` () =
    let ctx = sql.GetDataContext()
    let pageSize = 10
    let pageNumber = 1
    
    let pagedCustomerOrders =
        query {
            for customer in ctx.Main.Customers do
            for order in customer.``main.Orders by CustomerID`` do
            sortBy customer.CompanyName
            thenBy order.OrderDate
            skip ((pageNumber - 1) * pageSize)
            take pageSize
            select (customer.CompanyName, order.OrderDate)
        } |> Seq.toList
    
    Assert.LessOrEqual(pagedCustomerOrders.Length, pageSize)
