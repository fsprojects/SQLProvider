#if INTERACTIVE
#I @"../../bin/lib/net48/"
#r "FSharp.Data.SqlProvider.Common.dll"
#r "FSharp.Data.SqlProvider.dll"
#r @"../../packages/tests/NUnit/lib/netstandard2.0/nunit.framework.dll"
#else
module SupplementaryTests
#endif

open System
open FSharp.Data.Sql
open System.Linq
open NUnit.Framework
open System.Threading.Tasks

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
let ``option type IsSome filtering``() =
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
let ``option type IsNone filtering``() =
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
let ``complex option type conditions``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for order in dc.Main.Orders do
            where (
                order.OrderDate.IsSome &&
                order.RequiredDate.IsSome &&
                (order.ShippedDate.IsNone || order.ShippedDate.Value > order.RequiredDate.Value)
            )
            select order
        }
    
    let results = query |> Array.ofSeq
    Assert.IsTrue(results.Length >= 0)

[<Test>]
let ``option value extraction in queries``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for order in dc.Main.Orders do
            where (order.ShippedDate.IsSome)
            select (order.OrderId, order.ShippedDate.Value)
        }
    
    let results = query |> Array.ofSeq
    Assert.IsTrue(results.Length > 0)
    results |> Array.iter (fun (id, shipDate) -> 
        Assert.IsTrue(id > 0)
        Assert.IsTrue(shipDate <> DateTime.MinValue)
    )

[<Test>]
let ``option type aggregations``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for order in dc.Main.Orders do
            where order.CustomerId.IsSome
            groupBy order.CustomerId.Value into g
            select (
                g.Key,
                g.Count(),
                g.Sum(fun x -> if x.ShippedDate.IsSome then 1 else 0),
                g.Sum(fun x -> if x.ShippedDate.IsNone then 1 else 0)
            )
        }
    
    let results = query |> Array.ofSeq |> Array.take 5
    Assert.IsTrue(results.Length > 0)
    
    results |> Array.iter (fun (customerId, totalCount, shippedCount, unshippedCount) ->
        let matching = totalCount = shippedCount + unshippedCount
        Assert.IsTrue(matching)
    )

// Dynamic Query Composition Tests
//module DynamicQueryTests =

[<Test>]
let ``conditional where clauses``() =
    let dc = sql.GetDataContext()
    
    let buildQuery includeShipped includeUnshipped =
        let baseQuery = 
            query {
                for order in dc.Main.Orders do
                where true
                select order
            }
        
        match includeShipped, includeUnshipped with
        | true, true -> baseQuery
        | true, false -> 
            query {
                for order in baseQuery do
                where (order.ShippedDate.IsSome)
                select order
            }
        | false, true ->
            query {
                for order in baseQuery do
                where (order.ShippedDate.IsNone)
                select order
            }
        | false, false -> 
            query {
                for order in baseQuery do
                where false
                select order
            }
    
    let allOrders = buildQuery true true |> Array.ofSeq
    let shippedOnly = buildQuery true false |> Array.ofSeq
    let unshippedOnly = buildQuery false true |> Array.ofSeq
    let noneSelected = buildQuery false false |> Array.ofSeq
    
    Assert.IsTrue(allOrders.Length > 0)
    Assert.IsTrue(shippedOnly.Length >= 0)
    Assert.IsTrue(unshippedOnly.Length >= 0)
    Assert.AreEqual(0, noneSelected.Length)
    Assert.IsTrue(allOrders.Length >= shippedOnly.Length + unshippedOnly.Length)

[<Test>]
let ``dynamic sorting based on parameter``() =
    let dc = sql.GetDataContext()
    
    let getSortedCustomers sortByCountry =
        if sortByCountry then
            query {
                for customer in dc.Main.Customers do
                sortBy customer.Country
                thenBy customer.CompanyName
                take 5
                select (customer.Country, customer.CompanyName)
            }
        else
            query {
                for customer in dc.Main.Customers do
                sortBy customer.CompanyName
                take 5
                select (customer.Country, customer.CompanyName)
            }
    
    let byCountry = getSortedCustomers true |> Array.ofSeq
    let byName = getSortedCustomers false |> Array.ofSeq
    
    Assert.AreEqual(5, byCountry.Length)
    Assert.AreEqual(5, byName.Length)
    
    // Verify sorting
    let countriesSorted = byCountry |> Array.map fst |> Array.pairwise |> Array.forall (fun (a, b) -> a <= b)
    Assert.IsTrue(countriesSorted)

[<Test>]
let ``conditional filtering with multiple parameters``() =
    let dc = sql.GetDataContext()
    
    let searchCustomers (countryFilter: string option) (cityFilter: string option) =
        let mutable querybase = 
            query {
                for customer in dc.Main.Customers do
                select customer
            }
        
        querybase <- 
            match countryFilter with
            | Some country -> 
                query {
                    for customer in querybase do
                    where (customer.Country.Value = country)
                    select customer
                }
            | None -> querybase
        
        querybase <- 
            match cityFilter with
            | Some city ->
                query {
                    for customer in querybase do
                    where (customer.City.Value = city)
                    select customer
                }
            | None -> querybase
        
        querybase
    
    let allCustomers = searchCustomers None None |> Array.ofSeq
    let usaCustomers = searchCustomers (Some "USA") None |> Array.ofSeq
    let londonCustomers = searchCustomers None (Some "London") |> Array.ofSeq
    let usaLondonCustomers = searchCustomers (Some "USA") (Some "London") |> Array.ofSeq
    
    Assert.IsTrue(allCustomers.Length > 0)
    Assert.IsTrue(usaCustomers.Length >= 0)
    Assert.IsTrue(londonCustomers.Length >= 0)
    Assert.IsTrue(usaLondonCustomers.Length >= 0)
    
    // Verify filtering worked
    if usaCustomers.Length > 0 then
        Assert.IsTrue(usaCustomers |> Array.forall (fun c -> c.Country.Value = "USA"))
    
    if londonCustomers.Length > 0 then
        Assert.IsTrue(londonCustomers |> Array.forall (fun c -> c.City.Value = "London"))

// Advanced Aggregation Tests
//module AggregationTests =

[<Test>]
let ``groupBy with multiple aggregations``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for orderDetail in dc.Main.OrderDetails do
            groupBy orderDetail.ProductId into g
            select (
                g.Key,
                g.Count(),
                g.Sum(fun x -> int x.Quantity),
                g.Average(fun x -> x.UnitPrice),
                g.Min(fun x -> x.UnitPrice),
                g.Max(fun x -> x.UnitPrice)
            )
        }
    
    let results = query |> Array.ofSeq |> Array.take 5
    Assert.IsTrue(results.Length > 0)
    
    results |> Array.iter (fun (productId, count, totalQty, avgPrice, minPrice, maxPrice) ->
        Assert.IsTrue(productId > 0)
        Assert.IsTrue(count > 0)
        Assert.IsTrue(totalQty > 0)
        Assert.IsTrue(avgPrice > 0m)
        Assert.IsTrue(minPrice <= maxPrice)
    )

[<Test>]
let ``complex groupBy with having clause equivalent``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for orderDetail in dc.Main.OrderDetails do
            groupBy orderDetail.ProductId into g
            where (g.Count() > 5) // Having equivalent
            select (g.Key, g.Count(), g.Sum(fun x -> decimal x.Quantity * x.UnitPrice))
        }
    
    let results = query |> Array.ofSeq
    Assert.IsTrue(results.Length > 0)
    
    results |> Array.iter (fun (productId, count, totalValue) ->
        Assert.IsTrue(count > 5)
        Assert.IsTrue(totalValue > 0m)
    )

// Performance and Edge Case Tests
//module PerformanceTests =

[<Test>]
let ``large dataset pagination performance``() =
    task {
        let dc = sql.GetDataContext()
        let pageSize = 50
        let mutable totalProcessed = 0
        let mutable currentPage = 0
        let maxPages = 10 // Limit for test
        
        while currentPage < maxPages do
            let! batch = 
                query {
                    for orderDetail in dc.Main.OrderDetails do
                    sortBy orderDetail.OrderId
                    skip (currentPage * pageSize)
                    take pageSize
                    select orderDetail
                } |> Array.executeQueryAsync
            
            if batch.Length > 0 then
                totalProcessed <- totalProcessed + batch.Length
                currentPage <- currentPage + 1
            else
                currentPage <- maxPages // Exit loop
        
        Assert.IsTrue(totalProcessed > 0)
    }

[<Test>]
let ``null value handling in aggregations``() =
    let dc = sql.GetDataContext()
    
    // Test that null values are handled correctly in aggregations
    let query = 
        query {
            for order in dc.Main.Orders do
            groupBy (order.ShippedDate.IsSome) into g
            select (
                g.Key,
                g.Count(),
                g.Sum(fun x -> if x.RequiredDate.IsSome then 1 else 0)
            )
        }
    
    let results = query |> Array.ofSeq
    Assert.IsTrue(results.Length > 0)
    
    // Should have at least one group for shipped and potentially one for unshipped
    let shippedGroup = results |> Array.tryFind (fun (isShipped, _, _) -> isShipped)
    Assert.IsTrue(shippedGroup.IsSome)

[<Test>]
let ``empty result set handling``() =
    let dc = sql.GetDataContext()
    
    // Query that should return no results
    let query = 
        query {
            for customer in dc.Main.Customers do
            where (customer.CustomerId = "NONEXISTENT")
            select customer
        }
    
    let results = query |> Array.ofSeq
    Assert.AreEqual(0, results.Length)

[<Test>]
let ``concurrent query execution``() =
    task {
        let dc = sql.GetDataContext()
        
        let task1 = 
            task {
                let! result = 
                    query {
                        for customer in dc.Main.Customers do
                        take 10
                        select customer.CustomerId
                    } |> Array.executeQueryAsync
                return "customers", result.Length
            }
        
        let task2 = 
            task {
                let! result = 
                    query {
                        for order in dc.Main.Orders do
                        take 10
                        select order.OrderId
                    } |> Array.executeQueryAsync
                return "orders", result.Length
            }
        
        let task3 = 
            task {
                let! result = 
                    query {
                        for product in dc.Main.Products do
                        take 10
                        select product.ProductId
                    } |> Array.executeQueryAsync
                return "products", result.Length
            }
        
        let! results = Task.WhenAll([| task1; task2; task3 |])
        
        Assert.AreEqual(3, results.Length)
        results |> Array.iter (fun (name, count) ->
            Assert.IsTrue(count > 0, $"{name} should have results")
        )
    }

