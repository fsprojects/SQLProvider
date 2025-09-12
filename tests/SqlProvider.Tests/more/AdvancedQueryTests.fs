#if INTERACTIVE
#I @"../../bin/lib/net48/"
#r "FSharp.Data.SqlProvider.Common.dll"
#r "FSharp.Data.SqlProvider.dll"
#r @"../../packages/tests/NUnit/lib/netstandard2.0/nunit.framework.dll"
#else
module AdvancedQueryTests
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

type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, CaseSensitivityChange=Common.CaseSensitivityChange.ORIGINAL, ResolutionPath = resolutionPath, SQLiteLibrary=Common.SQLiteLibrary.SystemDataSQLite>
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %O")
type sqlOpt = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, CaseSensitivityChange=Common.CaseSensitivityChange.ORIGINAL, ResolutionPath = resolutionPath, SQLiteLibrary=Common.SQLiteLibrary.SystemDataSQLite, UseOptionTypes = Common.NullableColumnType.VALUE_OPTION>

//[<SetUp>]
//let setUp() =
//    let ctx = sql.GetDataContext()
//    // Setup test data
//    ()

[<Test>]
let ``complex join with navigation properties`` () =
    let ctx = sql.GetDataContext()
    let query = 
        query {
            for customer in ctx.Main.Customers do
            for order in customer.``main.Orders by CustomerID`` do
            for detail in order.``main.OrderDetails by OrderID`` do
            where (detail.Quantity > (int16 10))
            select (customer.CompanyName, order.OrderDate, detail.Quantity)
        }
    
    let results = query |> Seq.toList
    Assert.IsNotEmpty(results)

[<Test>]
let ``subquery with exists pattern`` () =
    let ctx = sql.GetDataContext()
    
    // Find customers who have orders with expensive items
    let expensiveOrderCustomers =
        query {
            for customer in ctx.Main.Customers do
            where (query {
                for order in ctx.Main.Orders do
                join detail in ctx.Main.OrderDetails on (order.OrderId = detail.OrderId)
                exists (order.CustomerId = customer.CustomerId && detail.UnitPrice > 50m)
            })
            select customer.CustomerId
        }
    
    let results = expensiveOrderCustomers |> Seq.toList
    Assert.IsNotEmpty(results)

[<Test>]
let ``complex aggregation with grouping`` () =
    let ctx = sql.GetDataContext()
    
    let orderStats =
        query {
            for order in ctx.Main.Orders do
            join detail in ctx.Main.OrderDetails on (order.OrderId = detail.OrderId)
            groupBy order.CustomerId into g
            select (g.Key, 
                    g.Count(), 
                    g.Sum(fun (_,x) -> x.UnitPrice * decimal x.Quantity),
                    g.Average(fun (_,x) -> x.UnitPrice))
        }
    
    let results = orderStats |> Seq.toList
    Assert.IsNotEmpty(results)

[<Test>]
let ``option type handling with ValueSome pattern`` () =
    let ctx = sqlOpt.GetDataContext()
    
    let customersWithRegion =
        query {
            for customer in ctx.Main.Customers do
            where (customer.Region.IsSome)
            select (customer.CompanyName, customer.Region.Value)
        }
    
    let results = customersWithRegion |> Seq.toList
    Assert.IsNotEmpty(results)

[<Test>]
let ``pagination with complex sorting`` () =
    let ctx = sql.GetDataContext()
    let pageSize = 10
    let pageNumber = 1
    
    let pagedResults =
        query {
            for customer in ctx.Main.Customers do
            sortBy customer.Country
            thenBy customer.City
            thenByDescending customer.CompanyName
            skip ((pageNumber - 1) * pageSize)
            take pageSize
            select customer
        }
    
    let results = pagedResults |> Seq.toList
    Assert.LessOrEqual(results.Length, pageSize)


[<Test>]
let ``dynamic filtering with multiple optional parameters`` () =
    let ctx = sql.GetDataContext()
    
    let searchCustomers (country: string option) (city: string option) (hasOrders: bool) =
        let noCountry, country = match country with | None -> true, "" | Some x -> false, x
        let noCity, city = match city with | None -> true, "" | Some x -> false, x

        query {
            for customer in ctx.Main.Customers do
            where (
                (noCountry || customer.Country = country) &&
                (noCity || customer.City = city) &&
                (hasOrders || 
                 (query {
                     for order in ctx.Main.Orders do
                     exists (order.CustomerId = customer.CustomerId)
                 }))
            )
            select customer
        }
    
    let results1 = searchCustomers (Some "USA") None false |> Seq.toList
    let results2 = searchCustomers None (Some "London") true |> Seq.toList
    
    Assert.IsNotEmpty(results1)
    Assert.IsNotEmpty(results2)

[<Test>]
let ``complex case when expression`` () =
    let ctx = sql.GetDataContext()
    
    let customerCategories =
        query {
            for customer in ctx.Main.Customers do
            select (
                customer.CompanyName,
                (query {
                    for order in ctx.Main.Orders do
                    where (order.CustomerId = customer.CustomerId)
                    count
                } |> fun count ->
                    if count > 10 then "High Volume"
                    elif count > 5 then "Medium Volume"
                    else "Low Volume")
            )
        }
    
    let results = customerCategories |> Seq.toList
    Assert.IsNotEmpty(results)

