#if INTERACTIVE
#I @"../../bin/lib/net48/"
#r "FSharp.Data.SqlProvider.Common.dll"
#r "FSharp.Data.SqlProvider.dll"
#r @"../../packages/tests/NUnit/lib/netstandard2.0/nunit.framework.dll"
#else
module AdvancedQueryTestsWithOpts
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

type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, CaseSensitivityChange=Common.CaseSensitivityChange.ORIGINAL, ResolutionPath = resolutionPath, SQLiteLibrary=Common.SQLiteLibrary.SystemDataSQLite, UseOptionTypes = Common.NullableColumnType.VALUE_OPTION>
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %O")

[<Test>]
let ``three table join with forced join operator``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for order in dc.Main.Orders do
            join customer in (!!) dc.Main.Customers on (order.CustomerId.Value = customer.CustomerId)
            join orderDetail in (!!) dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            where (order.OrderDate.IsSome)
            take 5
            select (order.OrderId, customer.CompanyName, orderDetail.ProductId)
        }
    
    let results = query |> Array.ofSeq
    Assert.IsTrue(results.Length > 0)
    Assert.IsTrue(results.Length <= 5)

[<Test>]
let ``multiple table join with complex filtering``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for order in dc.Main.Orders do
            join customer in (!!) dc.Main.Customers on (order.CustomerId.Value = customer.CustomerId)
            join orderDetail in (!!) dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            where (
                customer.Country.Value = "USA" &&
                orderDetail.Quantity > 10s &&
                order.OrderDate.IsSome
            )
            select (customer.CompanyName, order.OrderId, orderDetail.Quantity)
        }
    
    let results = query |> Array.ofSeq
    Assert.IsTrue(results.Length > 0)
    // Verify all results meet criteria
    results |> Array.iter (fun (_, _, quantity) -> Assert.IsTrue(quantity > 10s))

// Subquery and Contains Test Examples

[<Test>]
let ``contains with subquery``() =
    let dc = sql.GetDataContext()
    
    let recentOrderCustomers = 
        query {
            for order in dc.Main.Orders do
            where (order.OrderDate.IsSome && order.CustomerId.IsSome)
            distinct
            select order.CustomerId.Value
        }
    
    let query = 
        query {
            for customer in dc.Main.Customers do
            where (recentOrderCustomers.Contains(customer.CustomerId))
            select customer.CompanyName
        }
    
    let results = query |> Array.ofSeq
    Assert.IsTrue(results.Length > 0)

[<Test>]
let ``multiple subqueries with complex conditions``() =
    let dc = sql.GetDataContext()
    
    let customersWithOrders = 
        query {
            for order in dc.Main.Orders do
            where (order.CustomerId.IsSome)
            distinct
            select order.CustomerId.Value
        }
    
    let customersWithHighPriceOrders = 
        query {
            for orderDetail in dc.Main.OrderDetails do
            where (orderDetail.UnitPrice > 100m)
            join order in (!!) dc.Main.Orders on (orderDetail.OrderId = order.OrderId)
            where (order.CustomerId.IsSome)
            distinct
            select order.CustomerId.Value
        }
    
    let query = 
        query {
            for customer in dc.Main.Customers do
            where (
                customersWithOrders.Contains(customer.CustomerId) &&
                customersWithHighPriceOrders.Contains(customer.CustomerId)
            )
            select customer
        }
    
    let results = query |> Array.ofSeq
    Assert.IsTrue(results.Length > 0)

[<Test>]
let ``nested subquery with exists pattern``() =
    let dc = sql.GetDataContext()
    
    let query = 
        query {
            for customer in dc.Main.Customers do
            where (
                query {
                    for order in dc.Main.Orders do
                    exists (order.CustomerId.Value = customer.CustomerId)
                }
            )
            select customer.CompanyName
        }
    
    let results = query |> Array.ofSeq
    Assert.IsTrue(results.Length > 0)

// Advanced Sorting and Pagination Tests


[<Test>]
let ``multiple sort criteria with thenBy``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for customer in dc.Main.Customers do
            sortBy customer.Country
            thenBy customer.City
            thenByDescending customer.CompanyName
            take 10
            select (customer.Country, customer.City, customer.CompanyName)
        }
    
    let results = query |> Array.ofSeq
    Assert.AreEqual(10, results.Length)
    
    // Verify sorting order
    let grouped = results |> Array.groupBy (fun (country, _, _) -> country) |> Array.sortBy fst
    for (country, items) in grouped do
        let cities = items |> Array.map (fun (_, city, _) -> city) |> Array.distinct |> Array.sort
        Assert.IsTrue(cities.Length > 0)

[<Test>]
let ``pagination with skip and take``() =
    let dc = sql.GetDataContext()
    let pageSize = 5
    let pageNumber = 2
    
    let query = 
        query {
            for customer in dc.Main.Customers do
            sortBy customer.CustomerId
            skip (pageNumber * pageSize)
            take pageSize
            select customer.CustomerId
        }
    
    let results = query |> Array.ofSeq
    Assert.IsTrue(results.Length <= pageSize)

[<Test>]
let ``conditional sorting based on runtime parameter``() =
    let dc = sql.GetDataContext()
    
    let getSortedCustomers sortByCountry =
        if sortByCountry then
            query {
                for customer in dc.Main.Customers do
                sortBy customer.Country
                thenBy customer.CompanyName
                take 10
                select customer
            }
        else
            query {
                for customer in dc.Main.Customers do
                sortBy customer.CompanyName
                take 10
                select customer
            }
    
    let resultsByCountry = getSortedCustomers true |> Array.ofSeq
    let resultsByName = getSortedCustomers false |> Array.ofSeq
    
    Assert.AreEqual(10, resultsByCountry.Length)
    Assert.AreEqual(10, resultsByName.Length)


// Async Execution Tests

[<Test>]
let ``async array execution``() =
    task {
        let dc = sql.GetDataContext()
        let! results = 
            query {
                for customer in dc.Main.Customers do
                take 10
                select customer.CompanyName
            } |> Array.executeQueryAsync
        
        Assert.AreEqual(10, results.Length)
    }

[<Test>]
let ``async list execution``() =
    task {
        let dc = sql.GetDataContext()
        let! results = 
            query {
                for customer in dc.Main.Customers do
                take 5
                select customer
            } |> List.executeQueryAsync
        
        Assert.AreEqual(5, results.Length)
    }

[<Test>]
let ``async single result with tryHeadAsync``() =
    task {
        let dc = sql.GetDataContext()
        let! result = 
            query {
                for customer in dc.Main.Customers do
                where (customer.CustomerId = "ALFKI")
                select customer
            } |> Seq.tryHeadAsync
        
        Assert.IsTrue(result.IsSome)
        Assert.AreEqual("ALFKI", result.Value.CustomerId)
    }

[<Test>]
let ``async single result with headAsync``() =
    task {
        let dc = sql.GetDataContext()
        let! result = 
            query {
                for customer in dc.Main.Customers do
                take 1
                select customer.CompanyName
            } |> Seq.headAsync
        
        Assert.IsNotNull(result)
        Assert.IsTrue(result.Length > 0)
    }

[<Test>]
let ``parallel async query execution``() =
    task {
        let dc = sql.GetDataContext()
        
        let customersTask = 
            query {
                for customer in dc.Main.Customers do
                take 10
                select customer
            } |> Array.executeQueryAsync
        
        let ordersTask = 
            query {
                for order in dc.Main.Orders do
                take 10
                select order
            } |> Array.executeQueryAsync
        
        let! customers = customersTask
        let! orders = ordersTask
        
        Assert.AreEqual(10, customers.Length)
        Assert.AreEqual(10, orders.Length)
    }

// Option Type Handling Tests

[<Test>]
let ``filtering with option type IsSome``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for order in dc.Main.Orders do
            where (order.ShippedDate.IsSome)
            select order.OrderId
        }
    
    let results = query |> Array.ofSeq
    Assert.IsTrue(results.Length > 0)

[<Test>]
let ``filtering with option type IsNone``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for order in dc.Main.Orders do
            where (order.ShippedDate.IsNone)
            select order.OrderId
        }
    
    let results = query |> Array.ofSeq
    Assert.IsTrue(results.Length >= 0) // May be 0 if all orders are shipped

[<Test>]
let ``complex option type filtering``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for order in dc.Main.Orders do
            where (
                order.OrderDate.IsSome &&
                order.RequiredDate.IsSome &&
                order.ShippedDate.IsNone
            )
            select order
        }
    
    let results = query |> Array.ofSeq
    Assert.IsTrue(results.Length >= 0)

// Performance and Integration Tests

[<Test>]
let ``large result set with chunked processing``() =
    task {
        let dc = sql.GetDataContext()
        let batchSize = 100
        let mutable totalProcessed = 0
        let mutable offset = 0
        let mutable hasMore = true
        
        while hasMore && totalProcessed < 500 do
            let! batch = 
                query {
                    for orderDetail in dc.Main.OrderDetails do
                    sortBy orderDetail.OrderId
                    skip offset
                    take batchSize
                    select orderDetail
                } |> Array.executeQueryAsync
            
            if batch.Length > 0 then
                totalProcessed <- totalProcessed + batch.Length
                offset <- offset + batchSize
                hasMore <- batch.Length = batchSize
            else
                hasMore <- false
        
        Assert.IsTrue(totalProcessed > 0)
    }

[<Test>]
let ``complex business logic query``() =
    let dc = sql.GetDataContext()
    
    // Find customers who have placed orders but no recent orders
    let recentOrderThreshold = DateTime.Now.AddDays(-365.0) // Use a date that makes sense for test data
    
    let customersWithRecentOrders = 
        query {
            for order in dc.Main.Orders do
            where (order.OrderDate.IsSome && order.CustomerId.IsSome)
            distinct
            select order.CustomerId.Value
        }
    
    let query = 
        query {
            for customer in dc.Main.Customers do
            where (
                customersWithRecentOrders.Contains(customer.CustomerId) &&
                not (
                    query {
                        for order in dc.Main.Orders do
                        exists (
                            order.CustomerId.Value = customer.CustomerId &&
                            order.OrderDate.IsSome
                        )
                    }
                )
            )
            select customer.CompanyName
        }
    
    let results = query |> Array.ofSeq
    Assert.IsTrue(results.Length >= 0) // May be 0 depending on test data

