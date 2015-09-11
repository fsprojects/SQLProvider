module QueryTests

open System
open FSharp.Data.Sql
open System.Linq
open NUnit.Framework

[<Literal>]
let connectionString = @"Data Source=./db/northwindEF.db; Version = 3; Read Only=false; FailIfMissing=True;"


type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, CaseSensitivityChange=Common.CaseSensitivityChange.ORIGINAL>
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %s")
   
[<Test; Ignore("Not Supported")>]
let ``simple select with contains query``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            select cust.CustomerId
            contains "ALFKI"
        }
    Assert.IsTrue(query)    

[<Test>]
let ``simple select with count``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            select cust.CustomerId
            count
        }
    Assert.AreEqual(91, query)   

[<Test; Ignore("Not Supported")>]
let ``simple select with last``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            sortBy cust.CustomerId
            select cust.CustomerId
            last
        }
    Assert.AreEqual("WOLZA", query)   

[<Test; Ignore("Not Supported")>]
let ``simple select with last or default when not exists``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "ZZZZ")
            select cust.CustomerId
            lastOrDefault
        }
    Assert.AreEqual(null, query)   

[<Test; Ignore("Not Supported")>]
let ``simple select with last or default when exists``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "WOLZA")
            select cust.CustomerId
            lastOrDefault
        }
    Assert.AreEqual("WOLZA", query)  

[<Test>]
let ``simple select with exactly one``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "ALFKI")
            select cust.CustomerId
            exactlyOne
        }
    Assert.AreEqual("ALFKI", query)  

[<Test; Ignore("Not Supported")>]
let ``simple select with exactly one when not exists``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "ZZZZ")
            select cust.CustomerId
            exactlyOneOrDefault
        }
    Assert.AreEqual(null, query)  

[<Test>]
let ``simple select with head``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "ALFKI")
            select cust.CustomerId
            head
        }
    Assert.AreEqual("ALFKI", query)  

[<Test; Ignore("Not Supported")>]
let ``simple select with head or Default``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "ALFKI")
            select cust.CustomerId
            headOrDefault
        }
    Assert.AreEqual("ALFKI", query)  

[<Test; Ignore("Not Supported")>]
let ``simple select with head or Default when not exists``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "ZZZZ")
            select cust.CustomerId
            headOrDefault
        }
    Assert.AreEqual(null, query)  

[<Test>]
let ``simple select query``() = 
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            select cust
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty query

[<Test>]
let ``simple select where query``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "ALFKI")
            select cust
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty query
    Assert.AreEqual(1, query.Length)
    Assert.AreEqual("Berlin", query.[0].City)

[<Test; Ignore("Not Supported")>]
let ``simple select query with minBy``() = 
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            minBy cust.City
        }   
    Assert.AreEqual("Berlin", query)

[<Test; Ignore("Not Supported")>]
let ``simple select query with maxBy``() = 
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            maxBy cust.City
        }   
    Assert.AreEqual("Århus", query)

[<Test; Ignore("Not Supported")>]
let ``simple select query with groupBy``() = 
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            groupBy cust.City into c
            select (c.Key, c.Count())
        } |> dict  
    Assert.AreEqual(6, query.["London"])

[<Test>]
let ``simple select and sort query``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            sortBy cust.City
            select cust.City
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty query    
    CollectionAssert.AreEquivalent([|"Aachen"; "Albuquerque"; "Anchorage"|], query.[0..2])

[<Test>]
let ``simple select and sort desc query``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            sortByDescending cust.City
            select cust.City
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty query    
    CollectionAssert.AreEquivalent([|"Århus"; "Warszawa"; "Walla Walla"|], query.[0..2])

[<Test; Ignore("Not Supported")>]
let ``simple select and sort query with then by query``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            sortBy cust.Country
            thenBy cust.City
            select cust.City
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty query    
    CollectionAssert.AreEquivalent([|"Buenos Aires"; "Buenos Aires"; "Buenos Aires"; "Graz"|], query.[0..3])

[<Test; Ignore("Not Supported")>]
let ``simple select and sort query with then by desc query``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            sortBy cust.Country
            thenByDescending cust.City
            select cust.City
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty query    
    CollectionAssert.AreEquivalent([|"Buenos Aires"; "Buenos Aires"; "Buenos Aires"; "Salzburg"|], query.[0..3])

[<Test>]
let ``simple select query with join``() = 
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            join order in dc.Main.Orders on (cust.CustomerId = order.CustomerId)
            select (cust.CustomerId, order.OrderDate)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty query
    CollectionAssert.AreEquivalent(
        [|
            "VINET", new DateTime(1996,7,4)
            "TOMSP", new DateTime(1996,7,5)
            "HANAR", new DateTime(1996,7,8)
            "VICTE", new DateTime(1996,7,8)
        |], query.[0..3])

[<Test>]
let ``simple select query with join using relationships``() = 
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            for order in cust.``main.Orders by CustomerID`` do
            select (cust.CustomerId, order.OrderId)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty query
    CollectionAssert.AreEquivalent(
        [|
            "VINET", new DateTime(1996,7,4)
            "TOMSP", new DateTime(1996,7,5)
            "HANAR", new DateTime(1996,7,8)
            "VICTE", new DateTime(1996,7,8)
        |], query.[0..3])

[<Test; Ignore("Not Supported")>]
let ``simple select query with group join``() = 
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            groupJoin ord in dc.Main.Orders on (cust.CustomerId = ord.CustomerId) into g
            for order in g do
            join orderDetail in dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            select (cust.CustomerId, orderDetail.ProductId, orderDetail.Quantity)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty query
    CollectionAssert.AreEquivalent(
        [|
            "VINET", 11L, 12
            "VINET", 42L, 10
            "VINET", 72L, 5
            "TOMSP", 14L, 9
            "TOMSP", 51L, 40
            "HANAR", 41L, 10
            "HANAR", 51L, 35
            "HANAR", 65L, 15
        |], query.[0..7])

[<Test>]
let ``simple select query with multiple joins on relationships``() = 
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            for order in cust.``main.Orders by CustomerID`` do
            for orderDetail in order.``main.OrderDetails by OrderID`` do
            select (cust.CustomerId, orderDetail.ProductId, orderDetail.Quantity)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty query
    CollectionAssert.AreEquivalent(
        [|
            "VINET", 11L, 12s
            "VINET", 42L, 10s
            "VINET", 72L, 5s
            "TOMSP", 14L, 9s
            "TOMSP", 51L, 40s
            "HANAR", 41L, 10s
            "HANAR", 51L, 35s
            "HANAR", 65L, 15s
        |], query.[0..7])

[<Test; Ignore("Not Supported")>]
let ``simple select query with left outer join``() = 
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            leftOuterJoin order in dc.Main.Orders on (cust.CustomerId = order.CustomerId) into result
            for order in result.DefaultIfEmpty() do
            select (cust.CustomerId, order.OrderDate)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty query
    CollectionAssert.AreEquivalent(
        [|
            "VINET", new DateTime(1996,7,4)
            "TOMSP", new DateTime(1996,7,5)
            "HANAR", new DateTime(1996,7,8)
            "VICTE", new DateTime(1996,7,8)
        |], query.[0..3])

[<Test; Ignore("Not Supported")>]
let ``simple sumBy``() = 
    let dc = sql.GetDataContext()
    let query = 
        query {
            for od in dc.Main.OrderDetails do
            sumBy od.UnitPrice
        }
    Assert.That(query, Is.EqualTo(56500.91M).Within(0.001M))

[<Test; Ignore("Not Supported")>]
let ``simple averageBy``() = 
    let dc = sql.GetDataContext()
    let query = 
        query {
            for od in dc.Main.OrderDetails do
            averageBy od.UnitPrice
        }
    Assert.That(query, Is.EqualTo(56500.91M).Within(0.001M))

[<Test>]
let ``simple select with distinct``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            select cust.City
            distinct
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty query   
    Assert.AreEqual(69, query.Length) 
    CollectionAssert.AreEquivalent([|"Aachen"; "Albuquerque"; "Anchorage"|], query.[0..2])

[<Test; Ignore("Query supported but providers implement badly.")>]
let ``simple select with skip``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            select cust.City
            skip 5
        } 
        |> Seq.toArray

    CollectionAssert.IsNotEmpty query   
    Assert.AreEqual(86, query.Length) 
    CollectionAssert.AreEquivalent([|"Bergamo"; "Berlin"; "Bern"|], query.[0..2])

[<Test>]
let ``simple select with take``() =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            select cust.City
            take 5
        } 
        |> Seq.toArray

    CollectionAssert.IsNotEmpty query   
    Assert.AreEqual(5, query.Length) 
    CollectionAssert.AreEquivalent([|"Aachen"; "Albuquerque"; "Anchorage"; "Barcelona"; "Barquisimeto"|], query)  
    

type Simple = {First : string}

type Dummy<'t> = D of 't

[<Test>]
let ``simple select into a generic type`` () =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for emp in dc.Main.Customers do
            select (D {First=emp.ContactName})
        } |> Seq.toList

    CollectionAssert.IsNotEmpty query

[<Test>]
let ``simple select into a generic type with pipe`` () =
    let dc = sql.GetDataContext()
    let query = 
        query {
            for emp in dc.Main.Customers do
            select ({First=emp.ContactName} |> D)
        } |> Seq.toList

    CollectionAssert.IsNotEmpty query