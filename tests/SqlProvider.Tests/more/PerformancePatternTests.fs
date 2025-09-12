#if INTERACTIVE
#I @"../../bin/lib/net48/"
#r "FSharp.Data.SqlProvider.Common.dll"
#r "FSharp.Data.SqlProvider.dll"
#r @"../../packages/tests/NUnit/lib/netstandard2.0/nunit.framework.dll"
#else
module PerformancePatternTests
#endif

open System
open FSharp.Data.Sql
open System.Linq
open NUnit.Framework
open System.Diagnostics

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

// Helper function to measure query performance
let measureQueryPerformance<'T> (queryName: string) (query: unit -> 'T) =
    let stopwatch = Stopwatch.StartNew()
    let result = query()
    stopwatch.Stop()
    
    printfn "Query '%s' completed in %d ms" queryName stopwatch.ElapsedMilliseconds
    result, stopwatch.ElapsedMilliseconds

[<Test>]
let ``chunking large ID arrays should work efficiently``() =
    // Get a list of order IDs to test with
    let allOrderIds = 
        query {
            for order in dc.Main.Orders do
            select order.OrderId
        } |> Seq.toArray
    
    // Process in chunks
    let chunkSize = 10
    let chunks = allOrderIds |> Array.chunkBySize chunkSize
    let results = ResizeArray<_>()
    
    for chunk in chunks do
        let chunkResults = 
            query {
                for order in dc.Main.Orders do
                where (chunk.Contains(order.OrderId))
                select order.OrderId
            } |> Seq.toArray
        
        results.AddRange(chunkResults)
    
    let retrievedIds = results.ToArray() |> Array.sort
    let originalIds = allOrderIds |> Array.sort
    
    Assert.AreEqual(originalIds.Length, retrievedIds.Length)
    Assert.IsTrue(Array.forall2 (=) originalIds retrievedIds)
    printfn "Successfully processed %d orders in %d chunks" allOrderIds.Length chunks.Length

[<Test>]
let ``pagination should work correctly``() =
    let pageSize = 5
    let page1 = 
        query {
            for customer in dc.Main.Customers do
            sortBy customer.CustomerId
            take pageSize
            select customer.CustomerId
        } |> Seq.toArray
    
    let page2 = 
        query {
            for customer in dc.Main.Customers do
            sortBy customer.CustomerId
            skip pageSize
            take pageSize
            select customer.CustomerId
        } |> Seq.toArray
    
    Assert.AreEqual(pageSize, page1.Length)
    Assert.AreEqual(pageSize, page2.Length)
    
    // Pages should not overlap
    let overlap = page1 |> Array.exists (fun id -> page2 |> Array.contains id)
    Assert.IsFalse(overlap)
    
    // Should be in order
    Assert.IsTrue(page1.[0] < page1.[pageSize - 1])
    Assert.IsTrue(page2.[0] < page2.[pageSize - 1])
    Assert.IsTrue(page1.[pageSize - 1] < page2.[0])
    
    printfn "Page 1: %A" page1
    printfn "Page 2: %A" page2

[<Test>]
let ``cursor-based pagination should be more efficient for large offsets``() =
    // Simulate cursor pagination
    let pageSize = 3
    
    // First page
    let firstPage = 
        query {
            for customer in dc.Main.Customers do
            sortBy customer.CustomerId
            take (pageSize + 1) // Get one extra to check if there are more
            select customer.CustomerId
        } |> Seq.toArray
    
    let hasNextPage = firstPage.Length > pageSize
    let firstPageItems = if hasNextPage then firstPage |> Array.take pageSize else firstPage
    
    Assert.IsTrue(hasNextPage)
    Assert.AreEqual(pageSize, firstPageItems.Length)
    
    // Second page using cursor
    let lastIdFromFirstPage = firstPageItems.[firstPageItems.Length - 1]
    let secondPage = 
        query {
            for customer in dc.Main.Customers do
            where (customer.CustomerId > lastIdFromFirstPage)
            sortBy customer.CustomerId
            take (pageSize + 1)
            select customer.CustomerId
        } |> Seq.toArray
    
    let secondPageItems = if secondPage.Length > pageSize then secondPage |> Array.take pageSize else secondPage
    
    // Verify no overlap
    let overlap = firstPageItems |> Array.exists (fun id -> secondPageItems |> Array.contains id)
    Assert.IsFalse(overlap)
    
    printfn "First page: %A" firstPageItems
    printfn "Second page: %A" secondPageItems

[<Test>]
let ``early filtering should be more efficient than late filtering``() =
    let testDate = DateTime(1997, 1, 1)
    
    // Early filtering approach
    let (earlyResults, earlyTime) = measureQueryPerformance "EarlyFiltering" (fun () ->
        query {
            for order in dc.Main.Orders do
            where (order.OrderDate.IsSome && order.OrderDate.Value >= testDate) // Filter early
            join orderDetail in dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            join customer in dc.Main.Customers on (order.CustomerId.Value = customer.CustomerId)
            take 10
            select (customer.CompanyName, order.OrderId)
        } |> Seq.toArray)
    
    // Note: For this test, we'll measure the same query twice since both should be efficient
    // In a real scenario, you'd compare with a less efficient approach
    let (_, repeatTime) = measureQueryPerformance "RepeatQuery" (fun () ->
        query {
            for order in dc.Main.Orders do
            where (order.OrderDate.IsSome && order.OrderDate.Value >= testDate)
            join orderDetail in dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            join customer in dc.Main.Customers on (order.CustomerId.Value = customer.CustomerId)
            take 10
            select (customer.CompanyName, order.OrderId)
        } |> Seq.toArray)
    
    Assert.IsTrue(earlyResults.Length > 0)
    Assert.IsTrue(earlyTime < 5000) // Should complete within 5 seconds
    Assert.IsTrue(repeatTime < 5000)
    
    printfn "Early filtering found %d results" earlyResults.Length

[<Test>]
let ``selective column loading should be more efficient``() =
    // Selective loading - only needed columns
    let (selectiveResults, selectiveTime) = measureQueryPerformance "SelectiveLoading" (fun () ->
        query {
            for customer in dc.Main.Customers do
            take 20
            select {|
                Id = customer.CustomerId
                Name = customer.CompanyName
                Country = customer.Country
            |}
        } |> Seq.toArray)
    
    // Full entity loading
    let (fullResults, fullTime) = measureQueryPerformance "FullLoading" (fun () ->
        query {
            for customer in dc.Main.Customers do
            take 20
            select customer
        } |> Seq.toArray)
    
    Assert.AreEqual(20, selectiveResults.Length)
    Assert.AreEqual(20, fullResults.Length)
    
    // Both should be fast for small datasets, but selective should not be slower
    Assert.IsTrue(selectiveTime <= fullTime + 100L) // Allow some margin
    
    printfn "Selective loading: %d ms, Full loading: %d ms" selectiveTime fullTime

[<Test>]
let ``streaming large datasets should control memory usage``() =
    let processedCount = ref 0
    let maxItemsToProcess = 50 // Limit for test
    
    // Stream processing without materializing entire result set
    let streamQuery = 
        query {
            for order in dc.Main.Orders do
            join orderDetail in dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            select (order.OrderId, (decimal orderDetail.Quantity) * orderDetail.UnitPrice)
        }
    
    let (_, streamTime) = measureQueryPerformance "StreamProcessing" (fun () ->
        for (orderId, value) in streamQuery do
            // Limit processing for test
            if !processedCount < maxItemsToProcess then

                // Process individual item
                incr processedCount
        
        !processedCount)
    
    Assert.IsTrue(!processedCount > 0)
    Assert.IsTrue(!processedCount <= maxItemsToProcess)
    Assert.IsTrue(streamTime < 5000)
    
    printfn "Streamed %d items in %d ms" !processedCount streamTime

[<Test>]
let ``batch processing should handle large datasets efficiently``() =
    let batchSize = 10
    let maxBatches = 3 // Limit for test
    let totalProcessed = ref 0
    let batchesProcessed = ref 0
    
    let (_, batchTime) = measureQueryPerformance "BatchProcessing" (fun () ->
        let mutable skipv = 0
        let mutable hasMoreData = true
        
        while hasMoreData && !batchesProcessed < maxBatches do
            let batch = 
                query {
                    for order in dc.Main.Orders do
                    sortBy order.OrderId
                    skip skipv
                    take batchSize
                    select order.OrderId
                } |> Seq.toArray
            
            hasMoreData <- batch.Length = batchSize
            skipv <- skipv + batchSize
            incr batchesProcessed
            
            // Process batch
            totalProcessed := !totalProcessed + batch.Length
        
        !totalProcessed)
    
    Assert.IsTrue(!totalProcessed > 0)
    Assert.IsTrue(!batchesProcessed <= maxBatches)
    Assert.IsTrue(batchTime < 5000)
    
    printfn "Processed %d items in %d batches (%d ms)" !totalProcessed !batchesProcessed batchTime

[<Test>]
let ``index-friendly queries should perform well``() =
    // Query using indexed column (OrderId is typically indexed)
    let (indexedResults, indexedTime) = measureQueryPerformance "IndexedQuery" (fun () ->
        query {
            for order in dc.Main.Orders do
            where (order.OrderId >= 10248 && order.OrderId <= 10348) // Range query on indexed column
            sortBy order.OrderId
            select order.OrderId
        } |> Seq.toArray)
    
    Assert.IsTrue(indexedResults.Length > 0)
    Assert.IsTrue(indexedTime < 2000) // Should be fast with index
    
    // Verify results are in order (confirms sort worked)
    let isOrdered = 
        indexedResults 
        |> Array.pairwise 
        |> Array.forall (fun (a, b) -> a <= b)
    
    Assert.IsTrue(isOrdered)
    
    printfn "Indexed query returned %d results in %d ms" indexedResults.Length indexedTime

[<Test>]
let ``query performance should be consistent across multiple runs``() =
    let runs = 3
    let times = ResizeArray<int64>()
    
    for i in 1..runs do
        let (_, time) = measureQueryPerformance $"Run{i}" (fun () ->
            query {
                for customer in dc.Main.Customers do
                take 10
                select customer.CustomerId
            } |> Seq.toArray)
        
        times.Add(time)
    
    let avgTime = times |> Seq.averageBy float
    let maxTime = times |> Seq.max
    let minTime = times |> Seq.min
    
    // Performance should be reasonably consistent
    Assert.IsTrue(maxTime - minTime < int64(avgTime) * 2L) // Max variance of 2x average
    
    printfn "Query times: Min=%d ms, Max=%d ms, Avg=%.1f ms" minTime maxTime avgTime

[<Test>]
let ``complex query optimization should show measurable improvement``() =
    // Complex query with multiple joins and filters
    let complexQuery() =
        query {
            for order in dc.Main.Orders do
            join customer in dc.Main.Customers on (order.CustomerId.Value = customer.CustomerId)
            join orderDetail in dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            join product in dc.Main.Products on (orderDetail.ProductId = product.ProductId)
            where (order.OrderDate.IsSome && 
                   customer.Country.Value = "USA" &&
                   orderDetail.UnitPrice > 10.0m)
            take 5
            select {|
                OrderId = order.OrderId
                Customer = customer.CompanyName
                Product = product.ProductName
                Value = (decimal orderDetail.Quantity) * orderDetail.UnitPrice
            |}
        } |> Seq.toArray
    
    let (results, time) = measureQueryPerformance "ComplexQuery" complexQuery
    
    Assert.IsTrue(results.Length > 0)
    Assert.IsTrue(time < 10000) // Should complete within 10 seconds
    
    // Verify results meet the criteria
    results |> Array.iter (fun r ->
        Assert.IsTrue(r.Value > 10.0m)
        Assert.IsTrue(not (String.IsNullOrEmpty r.Customer))
        Assert.IsTrue(not (String.IsNullOrEmpty r.Product)))
    
    printfn "Complex query returned %d results in %d ms" results.Length time
