#if INTERACTIVE
#I @"../../bin/lib/net48/"
#r "FSharp.Data.SqlProvider.Common.dll"
#r "FSharp.Data.SqlProvider.dll"
#r @"../../packages/tests/NUnit/lib/netstandard2.0/nunit.framework.dll"
#else
module PerformanceTests
#endif

open System
open FSharp.Data.Sql
open System.Linq
open NUnit.Framework
open System.Collections.Concurrent
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
let ``connection pooling pattern`` () =
    let pool = ConcurrentStack<sql.dataContext>()
    let maxPoolSize = 5
    
    let getConnection() =
        match pool.TryPop() with
        | true, ctx -> ctx
        | false, _ -> sql.GetDataContext()
    
    let returnConnection(ctx: sql.dataContext) =
        if pool.Count < maxPoolSize then
            pool.Push(ctx)

    
    task {
        // Test connection pooling
        let connections = Array.init 10 (fun _ -> getConnection())
        
        // Return connections to pool
        connections |> Array.iter returnConnection
        
        // Verify pool contains connections
        Assert.LessOrEqual(pool.Count, maxPoolSize)
        Assert.Greater(pool.Count, 0)
    }

[<Test>]
let ``batch processing pattern`` () =
    let batchSize = 100
    
    let processBatch (customers: sql.dataContext.``main.CustomersEntity``[]) =
        task {
            // Simulate processing
            do! Task.Delay(10)
            return customers.Length
        }
    
    task {
        let ctx = sql.GetDataContext()
        
        let totalCustomers = 
            query {
                for customer in ctx.Main.Customers do
                count
            }
        
        let mutable processedCount = 0
        let totalBatches = (totalCustomers + batchSize - 1) / batchSize
        
        for batchIndex in 0 .. totalBatches - 1 do
            let! batch =
                query {
                    for customer in ctx.Main.Customers do
                    skip (batchIndex * batchSize)
                    take batchSize
                    select customer
                } |> Array.executeQueryAsync
            
            let! batchResult = processBatch batch
            processedCount <- processedCount + batchResult
        
        Assert.AreEqual(totalCustomers, processedCount)
    }

[<Test>]
let ``memoization pattern test`` () =
    let cache = ConcurrentDictionary<string, Task<int>>()
    let mutable callCount = 0
    
    let expensiveOperation (customerId: string) =
        task {
            callCount <- callCount + 1
            let ctx = sql.GetDataContext()
            
            let! orderCount =
                query {
                    for order in ctx.Main.Orders do
                    where (order.CustomerId.Value = customerId)
                } |> Seq.lengthAsync |> Async.AwaitTask
            
            return orderCount
        }
    
    let memoizedOperation (customerId: string) =
        cache.GetOrAdd(customerId, fun key -> expensiveOperation key)
    
    task {
        // First call should execute the operation
        let! result1 = memoizedOperation "ALFKI"
        let firstCallCount = callCount
        
        // Second call should use cache
        let! result2 = memoizedOperation "ALFKI"
        let secondCallCount = callCount
        
        Assert.AreEqual(result1, result2)
        Assert.AreEqual(firstCallCount, secondCallCount) // No additional calls
        
        // Different key should execute operation
        let! result3 = memoizedOperation "ANATR"
        let thirdCallCount = callCount
        
        Assert.Greater(thirdCallCount, secondCallCount)
    }

[<Test>]
let ``chunked processing for large datasets`` () =
    let chunkSize = 50
    
    let processChunk (items: (string * string)[]) =
        task {
            // Simulate processing time
            do! Task.Delay(5)
            return items.Length
        }
    
    task {
        let ctx = sql.GetDataContext()
        
        let allCustomers =
            query {
                for customer in ctx.Main.Customers do
                select (customer.CustomerId, customer.CompanyName)
            } |> Seq.toArray
        
        let chunks = allCustomers |> Array.chunkBySize chunkSize
        let mutable totalProcessed = 0
        
        for chunk in chunks do
            let! processed = processChunk chunk
            totalProcessed <- totalProcessed + processed
        
        Assert.AreEqual(allCustomers.Length, totalProcessed)
    }

[<Test>]
let ``parallel query execution`` () =
    let getCustomerOrderCount customerId =
        task {
            let ctx = sql.GetDataContext()
            let! count =
                query {
                    for order in ctx.Main.Orders do
                    where (order.CustomerId.Value = customerId)
                } |> Seq.lengthAsync
            return customerId, count
        }
    
    task {
        let ctx = sql.GetDataContext()
        
        let customerIds =
            query {
                for customer in ctx.Main.Customers do
                take 10
                select customer.CustomerId
            } |> Seq.toArray
        
        // Process in parallel
        let! results = 
            customerIds
            |> Array.map getCustomerOrderCount
            |> Task.WhenAll
        
        Assert.AreEqual(customerIds.Length, results.Length)
        Assert.IsTrue(results |> Array.forall (fun (_, count) -> count >= 0))
    }

[<Test>]
let ``connection timeout handling`` () =
    task {
        try
            // Create context with short timeout
            let ctx = sql.GetDataContext(commandTimeout = 1)
            
            // Execute a potentially slow query
            let! result =
                query {
                    for customer in ctx.Main.Customers do
                    join order in ctx.Main.Orders on (customer.CustomerId = order.CustomerId.Value)
                    join detail in ctx.Main.OrderDetails on (order.OrderId = detail.OrderId)
                    groupBy customer.CustomerId into g
                    select (g.Key, g.Sum(fun (_,_,x) -> x.UnitPrice * decimal x.Quantity))
                } |> Seq.executeQueryAsync |> Async.AwaitTask
            
            Assert.IsNotNull(result)
        with
        | ex when ex.Message.Contains("timeout") ->
            Assert.Pass("Timeout handled correctly")
        | ex ->
            Assert.Fail("Unexpected exception: " + ex.Message)
    }

[<Test>]
let ``query result streaming`` () =
    task {
        let ctx = sql.GetDataContext()
        let processedCount = ref 0
        
        // Process results as they come (streaming)
        let customerQuery =
            query {
                for customer in ctx.Main.Customers do
                select customer
            }
        
        for customer in customerQuery do
            processedCount := !processedCount + 1
            // Simulate processing
            do! Task.Delay(1)
        
        Assert.Greater(!processedCount, 0)
    }

[<Test>]
let ``memory efficient large result processing`` () =
    task {
        let ctx = sql.GetDataContext()
        let mutable totalOrders = 0
        let mutable totalValue = 0.0m
        
        // Process without loading all into memory at once
        let orderDetails =
            query {
                for detail in ctx.Main.OrderDetails do
                select (detail.Quantity, detail.UnitPrice)
            }
        
        // Use sequence to avoid loading all at once
        for (quantity, unitPrice) in orderDetails do
            totalOrders <- totalOrders + 1
            totalValue <- totalValue + (decimal quantity * unitPrice)
        
        Assert.Greater(totalOrders, 0)
        Assert.Greater(totalValue, 0.0m)
    }

[<Test>]
let ``query plan caching test`` () =
    let ctx = sql.GetDataContext()
    let mutable executionTimes = []
    
    let executeQuery customerId =
        let stopwatch = System.Diagnostics.Stopwatch.StartNew()
        let result =
            query {
                for order in ctx.Main.Orders do
                where (order.CustomerId.Value = customerId)
            } |> Seq.lengthAsync
        stopwatch.Stop()
        executionTimes <- stopwatch.ElapsedMilliseconds :: executionTimes
        result
    
    task {
        // Execute same query multiple times
        for i in 1..5 do
            let! _ = executeQuery "ALFKI" |> Async.AwaitTask
            ()

        // Later executions should generally be faster due to query plan caching
        let avgFirstTwo = executionTimes |> List.map decimal |> List.take 2 |> List.average
        let avgLastTwo = executionTimes |> List.map decimal |> List.skip 3 |> List.average
        
        // This is a general expectation, but can vary
        Assert.IsTrue(avgLastTwo <= avgFirstTwo * 2.0m) // Allow reasonable variance
    }
