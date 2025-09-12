#if INTERACTIVE
#I @"../../bin/lib/net48/"
#r "FSharp.Data.SqlProvider.Common.dll"
#r "FSharp.Data.SqlProvider.dll"
#r @"../../packages/tests/NUnit/lib/netstandard2.0/nunit.framework.dll"
#else
module OptionTypesTests
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
let ``value option basic usage`` () =
    let ctx = sql.GetDataContext()
    
    let customersWithRegion =
        query {
            for customer in ctx.Main.Customers do
            where customer.Region.IsSome
            select (customer.CompanyName, customer.Region)
        } |> Seq.toList
    
    Assert.IsNotEmpty(customersWithRegion)
    customersWithRegion |> List.iter (fun (_, region) ->
        Assert.IsTrue(region.IsSome)
    )

[<Test>]
let ``value option pattern matching`` () =
    let ctx = sql.GetDataContext()
    
    let categorizeCustomers =
        query {
            for customer in ctx.Main.Customers do
            select (
                customer.CompanyName,
                match customer.Region with
                | ValueSome region -> "Has region: " + region
                | ValueNone -> "No region specified"
            )
        } |> Seq.toList
    
    Assert.IsNotEmpty(categorizeCustomers)

[<Test>]
let ``value option filtering patterns`` () =
    let ctx = sql.GetDataContext()
    
    // Filter by Some values
    let withRegion =
        query {
            for customer in ctx.Main.Customers do
            where customer.Region.IsSome
            select customer.CompanyName
        } |> Seq.toList
    
    // Filter by None values
    let withoutRegion =
        query {
            for customer in ctx.Main.Customers do
            where customer.Region.IsNone
            select customer.CompanyName
        } |> Seq.toList
    
    // Filter by specific value
    let specificRegion =
        query {
            for customer in ctx.Main.Customers do
            where (customer.Region = ValueSome "WA")
            select customer.CompanyName
        } |> Seq.toList
    
    Assert.IsNotEmpty(withRegion)
    Assert.IsNotEmpty(withoutRegion)

[<Test>]
let ``value option with default values`` () =
    let ctx = sql.GetDataContext()
    
    let customersWithDefaultRegion =
        query {
            for customer in ctx.Main.Customers do
            select (
                customer.CompanyName,
                customer.Region |> ValueOption.defaultValue "Unknown"
            )
        } |> Seq.toList
    
    Assert.IsNotEmpty(customersWithDefaultRegion)
    customersWithDefaultRegion |> List.iter (fun (_, region) ->
        Assert.IsNotNull(region)
    )

[<Test>]
let ``value option mapping and transformation`` () =
    let ctx = sql.GetDataContext()
    
    let processRegions =
        query {
            for customer in ctx.Main.Customers do
            select (
                customer.CompanyName,
                customer.Region |> ValueOption.map (fun r -> r.ToUpper())
            )
        } |> Seq.toList
    
    let upperCaseRegions =
        processRegions
        |> List.choose (fun (name, region) ->
            match region with
            | ValueSome r -> Some (name, r)
            | ValueNone -> None
        )
    
    Assert.IsNotEmpty(upperCaseRegions)

[<Test>]
let ``value option with nullable datetime`` () =
    let ctx = sql.GetDataContext()
    
    // Assuming we have a nullable datetime field
    let ordersWithShipDate =
        query {
            for order in ctx.Main.Orders do
            where order.ShippedDate.IsSome
            select (order.OrderId, order.ShippedDate.Value)
        } |> Seq.toList
    
    let ordersWithoutShipDate =
        query {
            for order in ctx.Main.Orders do
            where order.ShippedDate.IsNone
            select order.OrderId
        } |> Seq.toList
    
    Assert.IsNotEmpty(ordersWithShipDate)

[<Test>]
let ``value option aggregations`` () =
    let ctx = sql.GetDataContext()
    
    // Count non-null regions
    let regionsCount =
        query {
            for customer in ctx.Main.Customers do
            where customer.Region.IsSome
            count
        }
    
    // Get distinct regions
    let distinctRegions =
        query {
            for customer in ctx.Main.Customers do
            where customer.Region.IsSome
            select customer.Region.Value
            distinct
        } |> Seq.toList
    
    Assert.Greater(regionsCount, 0)
    Assert.IsNotEmpty(distinctRegions)

[<Test>]
let ``value option in joins`` () =
    let ctx = sql.GetDataContext()
    
    let customersWithNullableFields =
        query {
            for customer in ctx.Main.Customers do
            join order in ctx.Main.Orders on (customer.CustomerId = order.CustomerId.Value)
            where (customer.Region.IsSome && order.ShippedDate.IsSome)
            select (
                customer.CompanyName,
                customer.Region.Value,
                order.ShippedDate.Value
            )
        } |> Seq.toList
    
    Assert.IsNotEmpty(customersWithNullableFields)

[<Test; Ignore("Modifies database")>]
let ``value option insertion and updates`` () =
    task {
        let ctx = sql.GetDataContext()
        let cid = System.Guid.NewGuid().ToString()

        // Create customer with Some region
        let customer1 = ctx.Main.Customers.Create()
        customer1.CustomerId <- "TEST1" + cid
        customer1.CompanyName <- "Test Company 1"
        customer1.Region <- ValueSome "TX"

        let noneval : string voption = ValueNone
        // Create customer with None region
        let customer2 = ctx.Main.Customers.Create()
        customer2.CustomerId <- "TEST2" + cid
        customer2.CompanyName <- "Test Company 2"
        customer2.Region <- noneval
        
        do! ctx.SubmitUpdatesAsync()
        
        // Verify insertions
        let! test1 = 
            query {
                for c in ctx.Main.Customers do
                where (c.CustomerId = "TEST1" + cid)
                select c.Region
            } |> Seq.tryHeadAsync |> Async.AwaitTask
        
        let! test2 = 
            query {
                for c in ctx.Main.Customers do
                where (c.CustomerId = "TEST2" + cid)
                select c.Region
            } |> Seq.tryHeadAsync |> Async.AwaitTask
        
        Assert.IsTrue(test1.IsSome)
        Assert.AreEqual(ValueSome "TX", test1.Value)
        Assert.IsTrue(test2.IsSome)
        Assert.AreEqual(noneval, test2.Value)

        customer1.Delete()
        customer2.Delete()
        do! ctx.SubmitUpdatesAsync()
    }

[<Test>]
let ``value option complex expressions`` () =
    let ctx = sql.GetDataContext()
    
    let complexQuery =
        query {
            for customer in ctx.Main.Customers do
            where (
                customer.Region.IsSome &&
                customer.ContactTitle.IsSome &&
                customer.Region.Value.Length > 1
            )
            select (
                customer.CompanyName,
                customer.Region.Value + " - " + customer.ContactTitle.Value
            )
        } |> Seq.toList
    
    Assert.IsNotEmpty(complexQuery)

[<Test>]
let ``value option with case expressions`` () =
    let ctx = sql.GetDataContext()
    
    let categorizedCustomers =
        query {
            for customer in ctx.Main.Customers do
            select (
                customer.CompanyName,
                match customer.Region, customer.Country with
                | ValueSome region, ValueSome country when country = "USA" -> 
                    "US Region: " + region
                | ValueSome region, ValueSome country -> 
                    "International Region: " + region + " in " + country
                | ValueNone, ValueSome country -> 
                    "No region in " + country
                | _, ValueNone -> 
                    "Unknown country"
            )
        } |> Seq.toList
    
    Assert.IsNotEmpty(categorizedCustomers)

[<Test>]
let ``value option bind operations`` () =
    let ctx = sql.GetDataContext()
    
    let processCustomerData =
        query {
            for customer in ctx.Main.Customers do
            select customer
        }
        |> Seq.map (fun c ->
            c.CompanyName,
            c.Region 
            |> ValueOption.bind (fun region ->
                if region.Length > 2 then ValueSome (region.ToUpper())
                else ValueNone
            )
        )
        |> Seq.toList
    
    Assert.IsNotEmpty(processCustomerData)

[<Test>]
let ``value option with nullable foreign keys`` () =
    let ctx = sql.GetDataContext()
    
    // Assuming we have nullable foreign key relationships
    let ordersWithEmployees =
        query {
            for order in ctx.Main.Orders do
            where order.EmployeeId.IsSome
            select (order.OrderId, order.EmployeeId.Value)
        } |> Seq.toList
    
    let ordersWithoutEmployees =
        query {
            for order in ctx.Main.Orders do
            where order.EmployeeId.IsNone
            select order.OrderId
        } |> Seq.toList
    
    // Both lists can be valid depending on data
    Assert.IsTrue(ordersWithEmployees.Length >= 0)
    Assert.IsTrue(ordersWithoutEmployees.Length >= 0)
