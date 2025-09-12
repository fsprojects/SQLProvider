#if INTERACTIVE
#I @"../../bin/lib/net48/"
#r "FSharp.Data.SqlProvider.Common.dll"
#r "FSharp.Data.SqlProvider.dll"
#r @"../../packages/tests/NUnit/lib/netstandard2.0/nunit.framework.dll"
#else
module ComplexJoinTests
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

let dc = sql.GetDataContext()

[<Test>]
let ``three table join with nullable foreign key should work``() =
    let result = 
        query {
            for order in dc.Main.Orders do
            join customer in dc.Main.Customers on (order.CustomerId.Value = customer.CustomerId)
            join employee in (!!) dc.Main.Employees on (order.EmployeeId.Value = employee.EmployeeId)
            where (order.EmployeeId.IsSome)
            take 5
            select (order.OrderId, customer.CompanyName, employee.FirstName + " " + employee.LastName)
        } |> Seq.toList
    
    Assert.IsTrue(result.Length > 0)
    result |> List.iter (fun (orderId, company, employee) -> 
        Assert.IsTrue(orderId > 0)
        Assert.IsTrue(not (String.IsNullOrEmpty company))
        Assert.IsTrue(not (String.IsNullOrEmpty employee))
        printfn "Order %d: %s handled by %s" orderId company employee)

[<Test>]
let ``four table join should work correctly``() =
    let result = 
        query {
            for order in dc.Main.Orders do
            join customer in dc.Main.Customers on (order.CustomerId.Value = customer.CustomerId)
            join orderDetail in dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            join product in dc.Main.Products on (orderDetail.ProductId = product.ProductId)
            where (orderDetail.Quantity > 10s)
            take 5
            select {|
                OrderId = order.OrderId
                CompanyName = customer.CompanyName
                ProductName = product.ProductName
                Quantity = orderDetail.Quantity
                UnitPrice = orderDetail.UnitPrice
            |}
        } |> Seq.toList
    
    Assert.IsTrue(result.Length > 0)
    result |> List.iter (fun order -> 
        Assert.IsTrue(order.OrderId > 0)
        Assert.IsTrue(order.Quantity > 10s)
        Assert.IsTrue(not (String.IsNullOrEmpty order.CompanyName))
        Assert.IsTrue(not (String.IsNullOrEmpty order.ProductName))
        printfn "Order %d: %s ordered %d x %s at $%.2f" 
                order.OrderId order.CompanyName order.Quantity order.ProductName order.UnitPrice)

[<Test>]
let ``left join should include records with no matches``() =
    let result = 
        query {
            for customer in dc.Main.Customers do
            join order in (!!) dc.Main.Orders on (customer.CustomerId = order.CustomerId.Value)
            select (customer.CompanyName, 
                   match order.OrderDate with
                   | ValueNone -> "No Orders"
                   | ValueSome date -> sprintf $"Order at {date}")
        } |> Seq.toList
    
    Assert.IsTrue(result.Length > 0)
    
    let hasNoOrders = result |> List.exists (fun (_, orderInfo) -> orderInfo = "No Orders")
    let hasOrders = result |> List.exists (fun (_, orderInfo) -> orderInfo.StartsWith("Order "))
    
    // At least one customer should have no orders
    Assert.IsTrue(hasNoOrders)
    
    // At least one customer should have orders
    Assert.IsTrue(hasOrders)
    
    result |> List.iter (fun (company, orderInfo) -> 
        Assert.IsTrue(not (String.IsNullOrEmpty company))
        printfn "%s: %s" company orderInfo)

[<Test>]
let ``conditional join with complex where clause should work``() =
    let startDate = DateTime(1996, 1, 1)
    let endDate = DateTime(1996, 12, 31)
    
    let result = 
        query {
            for order in dc.Main.Orders do
            join customer in dc.Main.Customers on (order.CustomerId.Value = customer.CustomerId)
            join orderDetail in dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            where (order.OrderDate.IsSome && 
                   order.OrderDate.Value >= startDate &&
                   order.OrderDate.Value <= endDate &&
                   orderDetail.Quantity > 5s &&
                   customer.City.Value = "London")
            take 3
            select (customer.CompanyName, order.OrderId, orderDetail.Quantity, order.OrderDate.Value)
        } |> Seq.toList
    
    // May return 0 results if no London customers ordered >5 items in 1996
    result |> List.iter (fun (company, orderId, quantity, orderDate) -> 
        Assert.IsTrue(quantity > 5s)
        Assert.IsTrue(orderDate >= startDate && orderDate <= endDate)
        printfn "London customer %s: Order %d (%d items) on %A" company orderId quantity orderDate)

[<Test>]
let ``multiple joins with aggregation should work``() =
    let result = 
        query {
            for customer in dc.Main.Customers do
            join order in dc.Main.Orders on (customer.CustomerId = order.CustomerId.Value)
            join orderDetail in dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            where (customer.City.Value = "London")
            groupBy customer.CompanyName into customerGroup
            select (customerGroup.Key, customerGroup.Count())
        } |> Seq.toList
    
    Assert.IsTrue(result.Length > 0)
    result |> List.iter (fun (company, orderDetailCount) -> 
        Assert.IsTrue(orderDetailCount > 0)
        printfn "London customer %s has %d order details" company orderDetailCount)

[<Test>]
let ``join with calculated fields should work``() =
    let result = 
        query {
            for order in dc.Main.Orders do
            join orderDetail in dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            join product in dc.Main.Products on (orderDetail.ProductId = product.ProductId)
            take 5
            select {|
                OrderId = order.OrderId
                ProductName = product.ProductName
                Quantity = orderDetail.Quantity
                UnitPrice = orderDetail.UnitPrice
                LineTotal = (decimal orderDetail.Quantity) * orderDetail.UnitPrice
                Discount = orderDetail.Discount
                DiscountedTotal = (decimal orderDetail.Quantity) * orderDetail.UnitPrice * (1m - decimal orderDetail.Discount)
            |}
        } |> Seq.toList
    
    Assert.AreEqual(5, result.Length)
    result |> List.iter (fun line -> 
        let expectedLineTotal = (decimal line.Quantity) * line.UnitPrice
        Assert.AreEqual(expectedLineTotal, line.LineTotal)
        
        let expectedDiscounted = float line.LineTotal * (1.0 - float line.Discount)
        Assert.AreEqual(expectedDiscounted, line.DiscountedTotal)
        
        printfn "Order %d: %s, Qty: %d, Price: $%.2f, Total: $%.2f, Discounted: $%.2f" 
                line.OrderId line.ProductName line.Quantity line.UnitPrice line.LineTotal line.DiscountedTotal)

[<Test>]
let ``complex join with multiple conditions should work``() =
    let result = 
        query {
            for order in dc.Main.Orders do
            join customer in dc.Main.Customers on (order.CustomerId.Value = customer.CustomerId)
            join orderDetail in dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            join product in dc.Main.Products on (orderDetail.ProductId = product.ProductId)
            where (customer.Country.Value = "UK" &&
                   order.OrderDate.IsSome &&
                   orderDetail.UnitPrice > 20.0m &&
                   product.CategoryId.IsSome)
            take 3
            select (customer.CompanyName, product.ProductName, orderDetail.UnitPrice, order.OrderDate.Value)
        } |> Seq.toList
    
    result |> List.iter (fun (company, product, price, orderDate) -> 
        Assert.IsTrue(price > 20.0m)
        Assert.IsTrue(not (String.IsNullOrEmpty company))
        Assert.IsTrue(not (String.IsNullOrEmpty product))
        printfn "UK customer %s ordered %s at $%.2f on %A" company product price orderDate)

[<Test>]
let ``join with union-like pattern should work``() =
    // Get customer info with order summary
    let customerOrders = 
        query {
            for customer in dc.Main.Customers do
            join order in dc.Main.Orders on (customer.CustomerId = order.CustomerId.Value)
            groupBy customer.CustomerId into customerGroup
            select (customerGroup.Key, customerGroup.Count())
        } |> Map.ofSeq
    
    let result = 
        query {
            for customer in dc.Main.Customers do
            take 5
            select {|
                CompanyName = customer.CompanyName
                CustomerId = customer.CustomerId
                OrderCount = 
                    match customerOrders.TryFind customer.CustomerId with
                    | Some count -> count
                    | None -> 0
            |}
        } |> Seq.toList
    
    Assert.AreEqual(5, result.Length)
    result |> List.iter (fun customer -> 
        Assert.IsTrue(customer.OrderCount >= 0)
        printfn "Customer %s (%s) has %d orders" customer.CompanyName customer.CustomerId customer.OrderCount)

[<Test>]
let ``nested join conditions should work``() =
    // Find orders where the employee's city matches the customer's city
    let result = 
        query {
            for order in dc.Main.Orders do
            join customer in dc.Main.Customers on (order.CustomerId.Value = customer.CustomerId)
            join employee in (!!) dc.Main.Employees on (order.EmployeeId.Value = employee.EmployeeId)
            where (order.EmployeeId.IsSome &&
                   customer.City.IsSome && employee.City.IsSome &&
                   customer.City.Value = employee.City.Value)
            take 3
            select (order.OrderId, customer.CompanyName, employee.FirstName + " " + employee.LastName, customer.City.Value)
        } |> Seq.toList
    
    result |> List.iter (fun (orderId, company, employee, city) -> 
        Assert.IsTrue(orderId > 0)
        Assert.IsTrue(not (String.IsNullOrEmpty city))
        printfn "Order %d: %s in %s handled by local employee %s" orderId company city employee)

[<Test>]
let ``join performance with filtering should be efficient``() =
    // Test that filtering before joining is more efficient
    let stopwatch = System.Diagnostics.Stopwatch.StartNew()
    
    let result = 
        query {
            for order in dc.Main.Orders do
            where (order.OrderDate.IsSome && order.OrderDate.Value >= DateTime(1997, 1, 1))
            join customer in dc.Main.Customers on (order.CustomerId.Value = customer.CustomerId)
            join orderDetail in dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            where (orderDetail.UnitPrice > 10.0m)
            take 10
            select (order.OrderId, customer.CompanyName, orderDetail.UnitPrice)
        } |> Seq.toList
    
    stopwatch.Stop()
    
    Assert.IsTrue(result.Length > 0)
    Assert.IsTrue(stopwatch.ElapsedMilliseconds < 5000) // Should complete quickly
    
    result |> List.iter (fun (orderId, company, price) -> 
        Assert.IsTrue(price > 10.0m)
        printfn "Order %d: %s at $%.2f" orderId company price)
    
    printfn "Query completed in %d ms" stopwatch.ElapsedMilliseconds
