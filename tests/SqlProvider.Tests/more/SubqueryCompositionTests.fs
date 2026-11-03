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

#if NETFRAMEWORK
[<Literal>]
let connectionString =  @"Data Source=" + __SOURCE_DIRECTORY__ + @"/../db/northwindEF.db;Version=3;Read Only=false;FailIfMissing=True;"
[<Literal>]
let sqliteLib = Common.SQLiteLibrary.SystemDataSQLite
#else
[<Literal>]
let connectionString =  @"Data Source=" + __SOURCE_DIRECTORY__ + @"/../db/northwindEF.db"
[<Literal>]
let sqliteLib = Common.SQLiteLibrary.MicrosoftDataSqlite
#endif

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + "/../libs"

// If you want to run these in Visual Studio Test Explorer, please install:
// Tools -> Extensions and Updates... -> Online -> NUnit Test Adapter for Visual Studio
// http://nunit.org/index.php?p=vsTestAdapter&r=2.6.4

type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, CaseSensitivityChange=Common.CaseSensitivityChange.ORIGINAL, ResolutionPath = resolutionPath, SQLiteLibrary=sqliteLib, UseOptionTypes=Common.NullableColumnType.VALUE_OPTION>
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %O")


let dc = sql.GetDataContext()

[<Test; SqlCheck.ExpectSql("WHERE", "LIMIT 5|TOP 5|FETCH NEXT 5", "JOIN", "DISTINCT")>]
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
            where (largeOrderCustomers.Contains customer.CustomerId)
            take 5
            select (customer.CustomerId, customer.CompanyName)
        } |> Seq.toList
    
    Assert.IsTrue(result.Length > 0)
    result |> List.iter (fun (id, company) -> 
        Assert.IsTrue(not (String.IsNullOrEmpty id))
        Assert.IsTrue(not (String.IsNullOrEmpty company))
        printfn "Large order customer: %s (%s)" company id)

[<Test; SqlCheck.ExpectSql("WHERE", "JOIN", "DISTINCT")>]
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
                   expensiveOrderCustomers.Contains customer.CustomerId)
            select (customer.CustomerId, customer.CompanyName)
        } |> Seq.toList
    
    Assert.IsTrue(result.Length > 0)
    result |> List.iter (fun (pid, company) -> 
        printfn "Recent + expensive customer: %s (%s)" company pid)

[<Test; SqlCheck.ExpectSql("WHERE", "JOIN", "DISTINCT")>]
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
            where (seafoodCustomerIds.Contains customer.CustomerId)
            select (customer.CustomerId, customer.CompanyName)
        } |> Seq.toList
    
    result |> List.iter (fun (pid, company) -> 
        printfn "Seafood customer: %s (%s)" company pid)

[<Test; SqlCheck.ExpectSql("WHERE", "DISTINCT")>]
let ``not exists pattern should work``() =
    // Find customers who have never placed an order
    let customerIdsWithOrders =
        query {
            for order in dc.Main.Orders do
            where order.CustomerId.IsSome
            distinct
            select order.CustomerId.Value
        }
    
    let result =
        query {
            for customer in dc.Main.Customers do
            where (not (customerIdsWithOrders.Contains customer.CustomerId))
            select (customer.CustomerId, customer.CompanyName)
        } |> Seq.toList
    
    // Print results even if empty (some databases might have all customers with orders)
    printfn "Customers with no orders: %d" result.Length
    result |> List.iter (fun (pid, company) -> 
        printfn "No orders: %s (%s)" company pid)

[<Test; SqlCheck.ExpectSql("GROUP BY", "JOIN")>]
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

[<Test; SqlCheck.ExpectSql("DISTINCT")>]
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
            customers.Where(fun c -> customerIds.Contains c.CustomerId)
    
    // Compose filters
    let filteredCustomers =
        dc.Main.Customers.AsQueryable()
        |> byCountry "USA"
        |> hasOrders
        |> fun q -> q.Take 3
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

[<Test; SqlCheck.ExpectSql("WHERE", "JOIN", "DISTINCT")>]
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
                withCityFilter.Where(fun c -> customerIds.Contains c.CustomerId)
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
                withOrdersFilter.Where(fun c -> highValueCustomers.Contains c.CustomerId)
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

[<Test; SqlCheck.ExpectSql("WHERE|HAVING", "LIMIT 5|TOP 5|FETCH NEXT 5", "GROUP BY")>]
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
            where (europeanCustomerIds.Contains order.CustomerId.Value)
            select order.OrderId
        }
    
    let popularProductIds =
        query {
            for orderDetail in dc.Main.OrderDetails do
            where (europeanOrderIds.Contains orderDetail.OrderId)
            groupBy orderDetail.ProductId into productGroup
            where (productGroup.Count() > 2) // Ordered by at least 3 European customers
            select productGroup.Key
        }
    
    let result =
        query {
            for product in dc.Main.Products do
            where (popularProductIds.Contains product.ProductId)
            take 5
            select (product.ProductName, product.ProductId)
        } |> Seq.toList
    
    Assert.IsTrue(result.Length > 0)
    result |> List.iter (fun (name, pid) -> 
        printfn "Popular European product: %s (ID: %d)" name pid)

[<Test; SqlCheck.ExpectSql("WHERE", "LIMIT 5|TOP 5|FETCH NEXT 5", "DISTINCT")>]
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
            where (expensiveProductIds.Contains orderDetail.ProductId)
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
            where (customersOrderingExpensiveProducts.Contains customer.CustomerId)
            take 5
            select (customer.CompanyName, customer.CustomerId)
        } |> Seq.toList
    
    Assert.IsTrue(result.Length > 0)
    printfn "Found %d expensive products, %d orders, %d unique customers" 
            expensiveProductIds.Length ordersWithExpensiveProducts.Length result.Length
    
    result |> List.iter (fun (company, pid) -> 
        printfn "Customer ordering expensive products: %s (%s)" company pid)

[<Test; SqlCheck.ExpectSql("WHERE")>]
let ``parameters of a subquery nested in another subquery are bound``() =
    let minQty = 50s
    let allCases = false
    let onlyCase = "ERNSH"
    let openOnly = true
    let bigOrders =
        query {
            for detail in dc.Main.OrderDetails do
            where (detail.Quantity > minQty)
            select detail.OrderId
        }
    let chosen =
        query {
            for order in dc.Main.Orders do
            where ((allCases || order.CustomerId.Value = onlyCase) && (not openOnly || not (bigOrders.Contains order.OrderId)))
            select order.OrderId
        }
    let runQuery () =
        query {
            for order in dc.Main.Orders do
            where (chosen.Contains order.OrderId)
            select order.OrderId
        } |> Seq.toArray |> Array.sort

    let bigIds = query { for d in dc.Main.OrderDetails do select (d.OrderId, d.Quantity) } |> Seq.filter (fun (_, q) -> q > minQty) |> Seq.map fst |> Set.ofSeq
    let expected =
        query { for o in dc.Main.Orders do select (o.OrderId, o.CustomerId) }
        |> Seq.filter (fun (id, c) -> c = ValueSome onlyCase && not (bigIds.Contains id))
        |> Seq.map fst |> Seq.toArray |> Array.sort

    Assert.IsNotEmpty expected
    Assert.AreEqual(expected, runQuery ())
    Assert.AreEqual(expected, runQuery ())

[<Test; SqlCheck.ExpectSql("WHERE")>]
let ``parameters of three nested subquery levels are bound``() =
    let minQty = 50s
    let country = "Germany"
    let bigOrders =
        query {
            for detail in dc.Main.OrderDetails do
            where (detail.Quantity > minQty)
            select detail.OrderId
        }
    let germanBigOrders =
        query {
            for order in dc.Main.Orders do
            where (order.ShipCountry.Value = country && bigOrders.Contains order.OrderId)
            select order.OrderId
        }
    let customersOfThose =
        query {
            for order in dc.Main.Orders do
            where (order.OrderId > 0L && germanBigOrders.Contains order.OrderId)
            select order.CustomerId.Value
        }
    let result =
        query {
            for customer in dc.Main.Customers do
            where (customer.CustomerId <> "" && customersOfThose.Contains customer.CustomerId)
            select customer.CustomerId
        } |> Seq.toArray |> Array.sort

    let bigIds = query { for d in dc.Main.OrderDetails do select (d.OrderId, d.Quantity) } |> Seq.filter (fun (_, q) -> q > minQty) |> Seq.map fst |> Set.ofSeq
    let expected =
        query { for o in dc.Main.Orders do select (o.OrderId, o.ShipCountry, o.CustomerId) }
        |> Seq.filter (fun (id, c, _) -> c = ValueSome country && bigIds.Contains id)
        |> Seq.map (fun (_, _, cust) -> cust.Value) |> Seq.distinct |> Seq.toArray |> Array.sort

    Assert.IsNotEmpty expected
    Assert.AreEqual(expected, result)

[<Test; SqlCheck.ExpectSql("GROUP BY", "MAX(")>]
let ``a grouped sub-query selecting only an aggregate gives one column to IN``() =
    let latestOrderIds =
        query {
            for order in dc.Main.Orders do
            groupBy order.CustomerId into g
            select (g.Max(fun order -> order.OrderId))
        }

    let result =
        query {
            for order in dc.Main.Orders do
            where (latestOrderIds.Contains order.OrderId)
            select order.OrderId
        } |> Seq.toArray |> Array.sort

    let expected =
        query { for o in dc.Main.Orders do select (o.CustomerId, o.OrderId) }
        |> Seq.groupBy fst |> Seq.map (fun (_, rows) -> rows |> Seq.map snd |> Seq.max) |> Seq.toArray |> Array.sort

    Assert.IsNotEmpty expected
    Assert.AreEqual(expected, result)

[<Test; SqlCheck.ExpectSql("GROUP BY", "MAX(", "NOT IN")>]
let ``a grouped sub-query selecting only an aggregate gives one column to NOT IN``() =
    let latestOrderIds =
        query {
            for order in dc.Main.Orders do
            groupBy order.CustomerId into g
            select (g.Max(fun order -> order.OrderId))
        }

    let result =
        query {
            for order in dc.Main.Orders do
            where (not (latestOrderIds.Contains order.OrderId))
            select order.OrderId
        } |> Seq.toArray |> Array.sort

    let latest =
        query { for o in dc.Main.Orders do select (o.CustomerId, o.OrderId) }
        |> Seq.groupBy fst |> Seq.map (fun (_, rows) -> rows |> Seq.map snd |> Seq.max) |> Set.ofSeq

    let expected =
        query { for o in dc.Main.Orders do select o.OrderId }
        |> Seq.filter (latest.Contains >> not) |> Seq.toArray |> Array.sort

    Assert.IsNotEmpty expected
    Assert.AreEqual(expected, result)

[<Test; SqlCheck.NoSql>]
let ``a grouped query keeps its key in the select outside IN sub-queries``() =
    let latestOrderIds =
        query {
            for order in dc.Main.Orders do
            groupBy order.CustomerId into g
            select (g.Max(fun order -> order.OrderId))
        }

    let sql, _ = FSharp.Data.Sql.Runtime.QueryInspection.toQueryString latestOrderIds
    StringAssert.Contains("[CustomerID]", sql.Substring(0, sql.IndexOf "FROM"))

[<Test; SqlCheck.ExpectSql("GROUP BY", "UNION", "MAX(")>]
let ``a union of grouped sub-queries gives one column to IN on both sides``() =
    let latestByCustomer =
        query {
            for order in dc.Main.Orders do
            groupBy order.CustomerId into g
            select (g.Max(fun order -> order.OrderId))
        }

    let latestByEmployee =
        query {
            for order in dc.Main.Orders do
            groupBy order.EmployeeId into g
            select (g.Max(fun order -> order.OrderId))
        }

    let result =
        query {
            for order in dc.Main.Orders do
            where (latestByCustomer.Union(latestByEmployee).Contains order.OrderId)
            select order.OrderId
        } |> Seq.toArray |> Array.sort

    let latestBy key =
        query { for o in dc.Main.Orders do select (o.CustomerId, o.EmployeeId, o.OrderId) }
        |> Seq.groupBy key |> Seq.map (fun (_, rows) -> rows |> Seq.map (fun (_, _, id) -> id) |> Seq.max)

    let expected =
        Seq.append (latestBy (fun (customer, _, _) -> box customer)) (latestBy (fun (_, employee, _) -> box employee))
        |> Seq.distinct |> Seq.toArray |> Array.sort

    Assert.IsNotEmpty expected
    Assert.AreEqual(expected, result)

[<Test; SqlCheck.NoSql>]
let ``an IN sub-query computing on an aggregate in .NET is refused``() =
    let shifted =
        query {
            for order in dc.Main.Orders do
            groupBy order.CustomerId into g
            select (g.Max(fun order -> order.OrderId) + 1L)
        }

    let error =
        Assert.Throws<Exception>(fun () ->
            query {
                for order in dc.Main.Orders do
                where (shifted.Contains order.OrderId)
                select order.OrderId
            } |> Seq.toArray |> ignore)

    StringAssert.StartsWith("IN / NOT IN needs a sub-query that selects one column", error.Message)

let private doubled (id: int64) = id * 2L

[<Test; SqlCheck.NoSql>]
let ``an IN sub-query calling an F# function the database cannot run is refused``() =
    let doubledIds =
        query {
            for order in dc.Main.Orders do
            select (doubled order.OrderId)
        }

    let error =
        Assert.Throws<Exception>(fun () ->
            query {
                for order in dc.Main.Orders do
                where (not (doubledIds.Contains order.OrderId))
                select order.OrderId
            } |> Seq.toArray |> ignore)

    StringAssert.StartsWith("IN / NOT IN needs a sub-query that selects one column", error.Message)

[<Test; SqlCheck.NoSql>]
let ``an IN sub-query converting an aggregate to a narrower number in .NET is refused``() =
    let roundedAverages =
        query {
            for detail in dc.Main.OrderDetails do
            groupBy detail.ProductId into g
            select (int64 (g.Average(fun detail -> detail.UnitPrice)))
        }

    let error =
        Assert.Throws<Exception>(fun () ->
            query {
                for product in dc.Main.Products do
                where (roundedAverages.Contains product.ProductId)
                select product.ProductId
            } |> Seq.toArray |> ignore)

    StringAssert.StartsWith("IN / NOT IN needs a sub-query that selects one column", error.Message)

let private GetColumnDoubled (id: int64) = id * 2L

[<Test; SqlCheck.NoSql>]
let ``an IN sub-query calling a function named like a column read is refused``() =
    let doubledIds =
        query {
            for order in dc.Main.Orders do
            select (GetColumnDoubled order.OrderId)
        }

    let error =
        Assert.Throws<Exception>(fun () ->
            query {
                for order in dc.Main.Orders do
                where (doubledIds.Contains order.OrderId)
                select order.OrderId
            } |> Seq.toArray |> ignore)

    StringAssert.StartsWith("IN / NOT IN needs a sub-query that selects one column", error.Message)

type private DoubledId(id: int64) =
    member _.Key = id * 2L

[<Test; SqlCheck.NoSql>]
let ``an IN sub-query reading a Key that is not a group key is refused``() =
    let doubledIds =
        query {
            for order in dc.Main.Orders do
            select (DoubledId(order.OrderId).Key)
        }

    let error =
        Assert.Throws<Exception>(fun () ->
            query {
                for order in dc.Main.Orders do
                where (doubledIds.Contains order.OrderId)
                select order.OrderId
            } |> Seq.toArray |> ignore)

    StringAssert.StartsWith("IN / NOT IN needs a sub-query that selects one column", error.Message)

[<Test; SqlCheck.ExpectSql("GROUP BY", "IN")>]
let ``an IN sub-query selecting a group key still runs``() =
    let popular =
        query {
            for detail in dc.Main.OrderDetails do
            groupBy detail.ProductId into g
            where (g.Count() > 40)
            select g.Key
        }

    let result =
        query {
            for product in dc.Main.Products do
            where (popular.Contains product.ProductId)
            select product.ProductId
        } |> Seq.toArray |> Array.sort

    let expected =
        query { for d in dc.Main.OrderDetails do select d.ProductId }
        |> Seq.countBy id |> Seq.filter (fun (_, n) -> n > 40) |> Seq.map fst |> Seq.toArray |> Array.sort

    Assert.IsNotEmpty expected
    Assert.AreEqual(expected, result)

[<Test; SqlCheck.ExpectSql("GROUP BY", "HAVING", "MAX(", "IN")>]
let ``a grouped IN sub-query with a having filter selects only its aggregate``() =
    let latestOfPopular =
        query {
            for detail in dc.Main.OrderDetails do
            groupBy detail.ProductId into g
            where (g.Count() > 40)
            select (g.Max(fun detail -> detail.OrderId))
        }

    let result =
        query {
            for order in dc.Main.Orders do
            where (latestOfPopular.Contains order.OrderId)
            select order.OrderId
        } |> Seq.toArray |> Array.sort

    let expected =
        query { for d in dc.Main.OrderDetails do select (d.ProductId, d.OrderId) }
        |> Seq.groupBy fst |> Seq.filter (fun (_, rows) -> Seq.length rows > 40)
        |> Seq.map (fun (_, rows) -> rows |> Seq.map snd |> Seq.max) |> Seq.distinct |> Seq.toArray |> Array.sort

    Assert.IsNotEmpty expected
    Assert.AreEqual(expected, result)

[<Test; SqlCheck.ExpectSql("IN", "+ 1")>]
let ``an IN sub-query with arithmetic the database computes still runs``() =
    let nextIds =
        query {
            for order in dc.Main.Orders do
            select (order.OrderId + 1L)
        }

    let result =
        query {
            for order in dc.Main.Orders do
            where (nextIds.Contains order.OrderId)
            select order.OrderId
        } |> Seq.toArray |> Array.sort

    let ids = query { for o in dc.Main.Orders do select o.OrderId } |> Seq.toArray
    let next = ids |> Array.map (fun id -> id + 1L) |> Set.ofArray
    let expected = ids |> Array.filter next.Contains |> Array.sort

    Assert.IsNotEmpty expected
    Assert.AreEqual(expected, result)
