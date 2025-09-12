#if INTERACTIVE
#I @"../../bin/lib/net48/"
#r "FSharp.Data.SqlProvider.Common.dll"
#r "FSharp.Data.SqlProvider.dll"
#r @"../../packages/tests/NUnit/lib/netstandard2.0/nunit.framework.dll"
#else
module SubqueryCompositionTests
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
let ``subquery as filter should work correctly``() =
    // Find customers who have placed large orders
    let largeOrderCustomers =
        query {
            for order in dc.Main.Orders do
            join orderDetail in dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            where (order.CustomerId.IsSome && (float orderDetail.Quantity) * (float orderDetail.UnitPrice) > 500.0)
            distinct
            select order.CustomerId.Value
        }
    
    let result =
        query {
            for customer in dc.Main.Customers do
            where (largeOrderCustomers.Contains(customer.CustomerId))
            take 5
            select (customer.CustomerId, customer.CompanyName)
        } |> Seq.toList
    
    Assert.IsTrue(result.Length > 0)
    result |> List.iter (fun (id, company) -> 
        Assert.IsTrue(not (String.IsNullOrEmpty id))
        Assert.IsTrue(not (String.IsNullOrEmpty company))
        printfn "Large order customer: %s (%s)" company id)

[<Test>]
let ``multiple subqueries combined should work``() =
    // Find customers who have placed recent orders
    let recentOrderCustomers =
        query {
            for order in dc.Main.Orders do
            where (order.OrderDate.IsSome && order.OrderDate.Value >= DateTime(1998, 1, 1) && order.CustomerId.IsSome)
            distinct
            select order.CustomerId.Value
        }
    
    // Find customers who have ordered expensive items
    let expensiveOrderCustomers =
        query {
            for order in dc.Main.Orders do
            join orderDetail in dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            where (orderDetail.UnitPrice > 30.0m && order.CustomerId.IsSome)
            distinct
            select order.CustomerId.Value
        }
    
    // Find customers who are both recent AND have expensive orders
    let result =
        query {
            for customer in dc.Main.Customers do
            where (recentOrderCustomers.Contains(customer.CustomerId) &&
                   expensiveOrderCustomers.Contains(customer.CustomerId))
            select (customer.CustomerId, customer.CompanyName)
        } |> Seq.toList
    
    Assert.IsTrue(result.Length > 0)
    result |> List.iter (fun (id, company) -> 
        printfn "Recent + expensive customer: %s (%s)" company id)

[<Test>]
let ``exists pattern using contains should work``() =
    // Find customers who have ordered seafood products (CategoryId = 8)
    let seafoodCustomerIds =
        query {
            for order in dc.Main.Orders do
            join orderDetail in dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            join product in dc.Main.Products on (orderDetail.ProductId = product.ProductId)
            where (order.CustomerId.IsSome && product.CategoryId.IsSome && product.CategoryId.Value = 8)
            distinct
            select order.CustomerId.Value
        }
    
    let result =
        query {
            for customer in dc.Main.Customers do
            where (seafoodCustomerIds.Contains(customer.CustomerId))
            select (customer.CustomerId, customer.CompanyName)
        } |> Seq.toList
    
    result |> List.iter (fun (id, company) -> 
        printfn "Seafood customer: %s (%s)" company id)

[<Test>]
let ``not exists pattern should work``() =
    // Find customers who have never placed an order
    let customerIdsWithOrders =
        query {
            for order in dc.Main.Orders do
            where (order.CustomerId.IsSome)
            distinct
            select order.CustomerId.Value
        }
    
    let result =
        query {
            for customer in dc.Main.Customers do
            where (not (customerIdsWithOrders.Contains(customer.CustomerId)))
            select (customer.CustomerId, customer.CompanyName)
        } |> Seq.toList
    
    // Print results even if empty (some databases might have all customers with orders)
    printfn "Customers with no orders: %d" result.Length
    result |> List.iter (fun (id, company) -> 
        printfn "No orders: %s (%s)" company id)

[<Test>]
let ``correlated subquery pattern should work``() =
    // Find customers with above-average order counts for their country
    let result =
        query {
            for customer in dc.Main.Customers do
            join order in dc.Main.Orders on (customer.CustomerId = order.CustomerId.Value)
            groupBy (customer.CustomerId, customer.CompanyName, customer.Country) into customerGroup
            let customerOrderCount = customerGroup.Count()
            // This simulates a correlated subquery by getting country average separately
            select (customerGroup.Key, customerOrderCount)
        } |> Seq.toList
    
    // Group by country to find averages
    let countryAverages =
        result
        |> List.groupBy (fun ((_, _, country), _) -> country)
        |> List.filter (fun (country, customers) -> country.IsSome)
        |> List.map (fun (country, customers) -> 
            let avgOrderCount = customers |> List.averageBy (fun (_, count) -> float count)
            country.Value, avgOrderCount)
        |> Map.ofList
    
    let aboveAverageCustomers =
        result
        |> List.filter (fun ((_, _, country), orderCount) ->
            if country.IsNone then false
            else
            match countryAverages.TryFind country.Value with
            | Some avg -> float orderCount > avg
            | None -> false)
        |> List.take 5
    
    aboveAverageCustomers |> List.iter (fun ((customerId, company, country), orderCount) -> 
        let countryAvg = countryAverages.[country.Value]
        printfn "Above average customer: %s (%s) in %s - %d orders (avg: %.1f)" 
                company customerId country.Value orderCount countryAvg)

type CustomerFilter = IQueryable<sql.dataContext.``main.CustomersEntity``> -> IQueryable<sql.dataContext.``main.CustomersEntity``>

[<Test>]
let ``query composition with filter functions should work``() =
    // Define reusable query filters
    
    let byCountry country : CustomerFilter =
        fun customers -> customers.Where(fun c -> c.Country.Value = country)
    
    let byCity city : CustomerFilter =
        fun customers -> customers.Where(fun c -> c.City.Value = city)
    
    let hasOrders : CustomerFilter =
        fun customers ->
            let customerIds =
                query {
                    for order in dc.Main.Orders do
                    distinct
                    select order.CustomerId.Value
                }
            customers.Where(fun c -> customerIds.Contains(c.CustomerId))
    
    // Compose filters
    let filteredCustomers =
        dc.Main.Customers.AsQueryable()
        |> byCountry "USA"
        |> hasOrders
        |> fun q -> q.Take(3)
        |> Seq.toList
    
    filteredCustomers |> List.iter (fun customer -> 
        Assert.AreEqual("USA", customer.Country.Value)
        printfn "USA customer with orders: %s (%s)" customer.CompanyName customer.CustomerId)

type SearchCriteria = {
    Country: string option
    City: string option
    HasOrders: bool
    MinOrderValue: float option
}

[<Test>]
let ``dynamic query building should work``() =
   
    let buildCustomerQuery (criteria: SearchCriteria) =
        let baseQuery = dc.Main.Customers.AsQueryable()
        
        let withCountryFilter =
            match criteria.Country with
            | Some country -> baseQuery.Where(fun c -> c.Country.Value = country)
            | None -> baseQuery
        
        let withCityFilter =
            match criteria.City with
            | Some city -> withCountryFilter.Where(fun c -> c.City.Value = city)
            | None -> withCountryFilter
        
        let withOrdersFilter =
            if criteria.HasOrders then
                let customerIds =
                    query {
                        for order in dc.Main.Orders do
                        distinct
                        select order.CustomerId.Value
                    }
                withCityFilter.Where(fun c -> customerIds.Contains(c.CustomerId))
            else
                withCityFilter
        
        let withValueFilter =
            match criteria.MinOrderValue with
            | Some minValue ->
                let highValueCustomers =
                    query {
                        for order in dc.Main.Orders do
                        join orderDetail in dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
                        where (order.CustomerId.IsSome && (float orderDetail.Quantity) * (float orderDetail.UnitPrice) >= minValue)
                        distinct
                        select order.CustomerId.Value
                    }
                withOrdersFilter.Where(fun c -> highValueCustomers.Contains(c.CustomerId))
            | None ->
                withOrdersFilter
        
        withValueFilter
    
    // Test different search criteria
    let criteria1 = { Country = Some "Germany"; City = None; HasOrders = true; MinOrderValue = Some 100.0 }
    let result1 = buildCustomerQuery criteria1 |> Seq.take 3 |> Seq.toList
    
    result1 |> List.iter (fun customer -> 
        Assert.AreEqual("Germany", customer.Country.Value)
        printfn "German customer with valuable orders: %s" customer.CompanyName)
    
    let criteria2 = { Country = None; City = Some "London"; HasOrders = true; MinOrderValue = None }
    let result2 = buildCustomerQuery criteria2 |> Seq.take 3 |> Seq.toList
    
    result2 |> List.iter (fun customer -> 
        Assert.AreEqual("London", customer.City.Value)
        printfn "London customer with orders: %s" customer.CompanyName)

[<Test>]
let ``complex subquery with multiple levels should work``() =
    // Find products that were ordered by customers from specific regions
    let europeanCustomerIds =
        query {
            for customer in dc.Main.Customers do
            where (customer.Country.IsSome && (customer.Country.Value = "Germany" || customer.Country.Value = "France" || customer.Country.Value = "UK"))
            select customer.CustomerId
        }
    
    let europeanOrderIds =
        query {
            for order in dc.Main.Orders do
            where (europeanCustomerIds.Contains(order.CustomerId.Value))
            select order.OrderId
        }
    
    let popularProductIds =
        query {
            for orderDetail in dc.Main.OrderDetails do
            where (europeanOrderIds.Contains(orderDetail.OrderId))
            groupBy orderDetail.ProductId into productGroup
            where (productGroup.Count() > 2) // Ordered by at least 3 European customers
            select productGroup.Key
        }
    
    let result =
        query {
            for product in dc.Main.Products do
            where (popularProductIds.Contains(product.ProductId))
            take 5
            select (product.ProductName, product.ProductId)
        } |> Seq.toList
    
    Assert.IsTrue(result.Length > 0)
    result |> List.iter (fun (name, id) -> 
        printfn "Popular European product: %s (ID: %d)" name id)

[<Test>]
let ``subquery result caching should work``() =
    // Simulate caching subquery results for reuse
    let expensiveProductIds =
        query {
            for product in dc.Main.Products do
            where (product.UnitPrice.IsSome && product.UnitPrice.Value > 20.0m)
            select product.ProductId
        } |> Seq.toList // Materialize the query
    
    // Use cached result multiple times
    let ordersWithExpensiveProducts =
        query {
            for orderDetail in dc.Main.OrderDetails do
            where (expensiveProductIds.Contains(orderDetail.ProductId))
            select orderDetail.OrderId
        } |> Seq.distinct |> Seq.toList
    
    let customersOrderingExpensiveProducts =
        query {
            for order in dc.Main.Orders do
            where (order.CustomerId.IsSome && ordersWithExpensiveProducts.Contains(order.OrderId))
            distinct
            select order.CustomerId.Value
        } |> Seq.toList
    
    let result =
        query {
            for customer in dc.Main.Customers do
            where (customersOrderingExpensiveProducts.Contains(customer.CustomerId))
            take 5
            select (customer.CompanyName, customer.CustomerId)
        } |> Seq.toList
    
    Assert.IsTrue(result.Length > 0)
    printfn "Found %d expensive products, %d orders, %d unique customers" 
            expensiveProductIds.Length ordersWithExpensiveProducts.Length result.Length
    
    result |> List.iter (fun (company, id) -> 
        printfn "Customer ordering expensive products: %s (%s)" company id)
