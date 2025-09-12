#if INTERACTIVE
#I @"../../bin/lib/net48/"
#r "FSharp.Data.SqlProvider.Common.dll"
#r "FSharp.Data.SqlProvider.dll"
#r @"../../packages/tests/NUnit/lib/netstandard2.0/nunit.framework.dll"
#else
module ValueOptionQueryTests
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
let ``valueOption column should filter correctly with IsSome``() =
    let result = 
        query {
            for customer in dc.Main.Customers do
            where (customer.Region.IsSome)
            select customer.CustomerId
        } |> Seq.toList
    
    Assert.IsTrue(result.Length > 0)
    printfn "Found %d customers with region" result.Length

[<Test>]
let ``valueOption column should filter by value correctly``() =
    let result = 
        query {
            for customer in dc.Main.Customers do
            where (customer.Region.IsSome && customer.Region.Value = "WA")
            select (customer.CustomerId, customer.Region.Value)
        } |> Seq.toList
    
    Assert.IsTrue(result.Length > 0)
    result |> List.iter (fun (id, region) -> 
        Assert.AreEqual("WA", region)
        printfn "Customer %s in region %s" id region)

[<Test>]
let ``valueOption should work with pattern matching in select``() =
    let result = 
        query {
            for customer in dc.Main.Customers do
            take 5
            select (customer.CustomerId, 
                   match customer.Region with 
                   | ValueSome region -> region 
                   | ValueNone -> "No Region")
        } |> Seq.toList
    
    Assert.AreEqual(5, result.Length)
    result |> List.iter (fun (id, region) -> 
        Assert.IsTrue(not (String.IsNullOrEmpty region))
        printfn "Customer %s: %s" id region)

[<Test>]
let ``valueOption should work with defaultValue``() =
    let result = 
        query {
            for customer in dc.Main.Customers do
            take 5
            select (customer.CustomerId, customer.Region |> ValueOption.defaultValue "Unknown")
        } |> Seq.toList
    
    Assert.AreEqual(5, result.Length)
    result |> List.iter (fun (id, region) -> 
        Assert.IsTrue(not (String.IsNullOrEmpty region))
        printfn "Customer %s: %s" id region)

[<Test>]
let ``valueOption nullable foreign key join should work``() =
    let result = 
        query {
            for order in dc.Main.Orders do
            join employee in (!!) dc.Main.Employees on (order.EmployeeId.Value = employee.EmployeeId)
            where (order.EmployeeId.IsSome)
            take 5
            select (order.OrderId, employee.FirstName, employee.LastName)
        } |> Seq.toList
    
    Assert.IsTrue(result.Length > 0)
    result |> List.iter (fun (orderId, firstName, lastName) -> 
        Assert.IsTrue(orderId > 0)
        Assert.IsTrue(not (String.IsNullOrEmpty firstName))
        printfn "Order %d handled by %s %s" orderId firstName lastName)

[<Test>]
let ``valueOption date comparison should work``() =
    let testDate = DateTime(1996, 7, 1)
    let result = 
        query {
            for order in dc.Main.Orders do
            where (order.OrderDate.IsSome && order.OrderDate.Value >= testDate)
            take 5
            select (order.OrderId, order.OrderDate.Value)
        } |> Seq.toList
    
    Assert.IsTrue(result.Length > 0)
    result |> List.iter (fun (orderId, orderDate) -> 
        Assert.IsTrue(orderDate >= testDate)
        printfn "Order %d on %A" orderId orderDate)

[<Test>]
let ``valueOption complex where clause should work``() =
    let result = 
        query {
            for order in dc.Main.Orders do
            where (order.ShippedDate.IsSome && 
                   order.RequiredDate.IsSome &&
                   order.ShippedDate.Value > order.RequiredDate.Value)
            take 3
            select (order.OrderId, order.RequiredDate.Value, order.ShippedDate.Value)
        } |> Seq.toList
    
    // Note: This might return 0 results if no orders were shipped late
    result |> List.iter (fun (orderId, required, shipped) -> 
        Assert.IsTrue(shipped > required)
        printfn "Late order %d: required %A, shipped %A" orderId required shipped)

[<Test>]
let ``valueOption count with filter should work``() =
    let countWithRegion = 
        query {
            for customer in dc.Main.Customers do
            where (customer.Region.IsSome)
            select customer.CustomerId
            count
        }
    
    let totalCount = 
        query {
            for customer in dc.Main.Customers do
            select customer.CustomerId
            count
        }
    
    Assert.IsTrue(countWithRegion > 0)
    Assert.IsTrue(countWithRegion <= totalCount)
    printfn "Customers with region: %d out of %d total" countWithRegion totalCount

[<Test>]
let ``valueOption computed values should work``() =
    let result = 
        query {
            for order in dc.Main.Orders do
            take 5
            select {|
                OrderId = order.OrderId
                DaysToShip = 
                    match order.OrderDate, order.ShippedDate with
                    | ValueSome orderDate, ValueSome shippedDate -> 
                        ValueSome (shippedDate - orderDate).Days
                    | _ -> ValueNone
                IsLate = 
                    match order.RequiredDate, order.ShippedDate with
                    | ValueSome required, ValueSome shipped -> shipped > required
                    | _ -> false
            |}
        } |> Seq.toList
    
    Assert.AreEqual(5, result.Length)
    result |> List.iter (fun order -> 
        Assert.IsTrue(order.OrderId > 0)
        match order.DaysToShip with
        | ValueSome days -> printfn "Order %d took %d days to ship, late: %b" order.OrderId days order.IsLate
        | ValueNone -> printfn "Order %d shipping time unknown, late: %b" order.OrderId order.IsLate)


[<Test>]
let ``valueOption aggregation should handle nulls correctly``() =
    // Count of non-null regions
    let regionCount = 
        query {
            for customer in dc.Main.Customers do
            where (customer.Region.IsSome)
            select customer.Region
            count
        }
    
    // Total customer count
    let totalCount = 
        query {
            for customer in dc.Main.Customers do
            count
        }
    
    Assert.IsTrue(regionCount > 0)
    Assert.IsTrue(regionCount <= totalCount)
    
    // Customers without region
    let noRegionCount = totalCount - regionCount
    Assert.IsTrue(noRegionCount >= 0)
    
    printfn "Customers: %d total, %d with region, %d without region" totalCount regionCount noRegionCount
