#if INTERACTIVE
#r @"../../bin/net451/FSharp.Data.SqlProvider.dll"
#r @"../../packages/NUnit/lib/nunit.framework.dll"
#else
module QueryTests
#endif

open System
open FSharp.Data.Sql
open System.Linq
open NUnit.Framework

[<Literal>]
let connectionString = @"Data Source=./db/northwindEF.db;Version=3;Read Only=false;FailIfMissing=True;"

// If you want to run these in Visual Studio Test Explorer, please install:
// Tools -> Extensions and Updates... -> Online -> NUnit Test Adapter for Visual Studio
// http://nunit.org/index.php?p=vsTestAdapter&r=2.6.4

type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, CaseSensitivityChange=Common.CaseSensitivityChange.ORIGINAL>
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %O")
let inline isNull (x:^T when ^T : not struct) = obj.ReferenceEquals (x, null)   

let isMono = Type.GetType ("Mono.Runtime") <> null

[<Test>]
let ``simple select with contains query``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            select cust.CustomerId
            contains "ALFKI"
        }
    Assert.IsTrue(qry)    

[<Test>]
let ``simple select with contains query with where``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.City <> "")
            select cust.CustomerId
            contains "ALFKI"
        }
    Assert.IsTrue(qry)    

[<Test>]
let ``simple select with contains query when not exists``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            select cust.CustomerId
            contains "ALFKI2"
        }
    Assert.IsFalse(qry)    

[<Test >]
let ``simple select with count``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            select cust.CustomerId
            count
        }
    Assert.AreEqual(91, qry)   

[<Test >]
let ``simple select with distinct count``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            select cust.CustomerId
            distinct
            count
        }
    Assert.AreEqual(91, qry)   

[<Test; Ignore("Not Supported")>]
let ``simple select with last``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            sortBy cust.CustomerId
            select cust.CustomerId
            last
        }
    Assert.AreEqual("WOLZA", qry)   

[<Test; Ignore("Not Supported")>]
let ``simple select with last or default when not exists``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "ZZZZ")
            select cust.CustomerId
            lastOrDefault
        }
    Assert.AreEqual(null, qry)   

[<Test>]
let ``simple exists when exists``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            exists (cust.CustomerId = "WOLZA")
        }
    Assert.AreEqual(true, qry)  

[<Test; Ignore("Not Supported")>]
let ``simple select with last or default when exists``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "WOLZA")
            select cust.CustomerId
            lastOrDefault
        }
    Assert.AreEqual("WOLZA", qry)  

[<Test >]
let ``simple select with exactly one``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "ALFKI")
            select cust.CustomerId
            exactlyOne
        }
    Assert.AreEqual("ALFKI", qry)  

[<Test >]
let ``option from join select with exactly one``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            join ord in dc.Main.Orders on (cust.CustomerId = ord.CustomerId)
            where (cust.CustomerId = "ALFKI" && ord.OrderId = (int64 10643))
            select (Some (cust, ord))
            exactlyOneOrDefault
        } 

    match qry with
    | Some(cust, ord) ->
        let id = cust.CustomerId
        let ordId = ord.OrderId
        Assert.AreEqual("ALFKI",id)
        Assert.AreEqual(10643,ordId)
    | None ->
        Assert.Fail()

[<Test >]
let ``option from simple select with exactly one``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "ALFKI")
            select (Some cust)
            exactlyOneOrDefault
        } 

    match qry with
    | Some cust ->
        let id = cust.CustomerId
        Assert.AreEqual("ALFKI",id)
        let city = cust.City
        cust.City <- city + " upd "
        dc.SubmitUpdates()
        cust.City <- city
        dc.SubmitUpdates()
    | None ->
        Assert.Fail()
     

[<Test>]
let ``simple select with exactly one when not exists``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "ZZZZ")
            select cust.CustomerId
            exactlyOneOrDefault
        }
    Assert.IsTrue(isNull(qry))
    Assert.AreEqual(null, qry)  


[<Test >]
let ``simple select with head``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "ALFKI")
            select cust.CustomerId
            head
        }
    Assert.AreEqual("ALFKI", qry)  

[<Test>]
let ``simple select with head or Default``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "ALFKI")
            select cust.CustomerId
            headOrDefault
        }
    Assert.AreEqual("ALFKI", qry)  

[<Test>]
let ``simple select with head or Default when not exists``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "ZZZZ")
            select cust.CustomerId
            headOrDefault
        }
    Assert.AreEqual(null, qry)  

[<Test >]
let ``simple select query``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            select cust
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry


[<Test>]
let ``simplest select query into temp``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            select (cust.City, cust.Country) into y
            select (snd y, fst y)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry

[<Test>]
let ``simplest select query let temp``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            let y = cust.City
            select cust.Address
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry

[<Test>]
let ``simple select query let temp nested``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            let y1 = cust.Address
            let y2 = cust.City
            select (y1, y2)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry

[<Test>]
let ``simple select query let temp``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            let y = cust.City + "test"
            select y
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry
    //Assert.AreEqual("Aachentest", qry.[0])
    //Assert.AreEqual("Berlintest", qry.[0])


[<Test>]
let ``simple select query let where``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            let y = cust.City + "test"
            where (cust.Address <> "")
            select y
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry
    Assert.AreEqual("Berlintest", qry.[0])

[<Test; Ignore("Not supported")>]
let ``simple select query let temp used in where``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            let y = cust.City + "test"
            where (y <> "")
            select y
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry
    Assert.AreEqual("Berlintest", qry.[0])


[<Test >]
let ``simple select query with operations in select``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            select (cust.Country + " " + cust.Address + (1).ToString())
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry

[<Test >]
let ``simple select where query``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "ALFKI")
            select cust
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry
    Assert.AreEqual(1, qry.Length)
    Assert.AreEqual("Berlin", qry.[0].City)


[<Test >]
let ``simple select where null query``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId <> null)
            select cust
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry
    Assert.AreEqual(91, qry.Length)
    Assert.AreEqual("Berlin", qry.[0].City)


[<Test >]
let ``simple select where query right side``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where ("ALFKI" = cust.CustomerId)
            select cust
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry
    Assert.AreEqual(1, qry.Length)
    Assert.AreEqual("Berlin", qry.[0].City)

[<Test >]
let ``simple select where query between col properties``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for order in dc.Main.Orders do
            where (order.ShippedDate > order.RequiredDate)
            select (order.ShippedDate, order.RequiredDate)
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry
    Assert.AreEqual(37, qry.Length)

[<Test >]
let ``simple nth query``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            select cust
            nth 4
        }
    Assert.AreEqual("London", qry.City)


[<Test >]
let ``simple select where not query``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (not(cust.CustomerId = "ALFKI"))
            select cust
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry
    Assert.AreEqual(90, qry.Length)

[<Test >]
let ``simple select where in query``() =
    let dc = sql.GetDataContext()
    let arr = ["ALFKI"; "ANATR"; "AROUT"]
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (arr.Contains(cust.CustomerId))
            select cust.CustomerId
        } |> Seq.toArray
    let res = query

    CollectionAssert.IsNotEmpty qry
    Assert.AreEqual(3, qry.Length)
    Assert.IsTrue(qry.Contains("ANATR"))

    
[<Test >]
let ``simple select where not-in query``() =
    let dc = sql.GetDataContext()
    let arr = ["ALFKI"; "ANATR"; "AROUT"]
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (not(arr.Contains(cust.CustomerId)))
            select cust.CustomerId
        } |> Seq.toArray
    let res = query

    CollectionAssert.IsNotEmpty qry
    Assert.AreEqual(88, qry.Length)
    Assert.IsFalse(qry.Contains("ANATR"))

[<Test >]
let ``simple select where in queryable query``() =

    let dc = sql.GetDataContext()
    let query1 = 
        query {
            for cust in dc.Main.Customers do
            where (cust.City="London")
            select cust.CustomerId
        }

    let query2 = 
        query {
            for cust in dc.Main.Customers do
            where (query1.Contains(cust.CustomerId))
            select cust.CustomerId
        } |> Seq.toArray
    let res = query

    CollectionAssert.IsNotEmpty query2
    Assert.AreEqual(6, query2.Length)
    Assert.IsTrue(query2.Contains("EASTC"))

[<Test >]
let ``simple select where inner-join box-check and not in queryable query``() =

    let dc = sql.GetDataContext()
    let query1 = 
        query {
            for cust in dc.Main.Customers do
            where (cust.City="London")
            select cust.CustomerId
        }

    let query2 = 
        query {
            for cust in dc.Main.Customers do
            for ord in (!!) cust.``main.Orders by CustomerID`` do
            where (box(ord.OrderDate) = null &&
               not(query1.Contains(cust.CustomerId)))
            select cust.CustomerId
        } |> Seq.toArray
    let res = query

    CollectionAssert.IsNotEmpty query2
    Assert.AreEqual(3, query2.Length)

[<Test >]
let ``simple select where in query custom syntax``() =
    let dc = sql.GetDataContext()
    let arr = ["ALFKI"; "ANATR"; "AROUT"]
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId |=| arr)
            select cust.CustomerId
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry
    Assert.AreEqual(3, qry.Length)
    Assert.IsTrue(qry.Contains("ANATR"))

[<Test >]
let ``simple select where like query``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId.Contains("a"))
            select cust.CustomerId
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry

[<Test >]
let ``simple select where query with operations in where``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId = "ALFKI" && (cust.City.StartsWith("B")))
            select cust
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry
    Assert.AreEqual(1, qry.Length)
    Assert.AreEqual("Berlin", qry.[0].City)

[<Test>]
let ``simple select query with minBy``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for ord in dc.Main.OrderDetails do
            minBy (decimal ord.Discount)
        }   
    Assert.AreEqual(0m, qry)

[<Test>]
let ``simple select query with minBy2``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for ord in dc.Main.OrderDetails do
            minBy (ord.Discount)
        }   
    Assert.AreEqual(0., qry)


[<Test>]
let ``simple select query with minBy DateTime``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for emp in dc.Main.Employees do
            minBy (emp.BirthDate)
        }   
    Assert.AreEqual(DateTime(1937, 09, 19), qry)

[<Test>]
let ``simple select query with sumBy``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            sumBy od.UnitPrice
        }
    Assert.Greater(56501m, qry)
    Assert.Less(56499m, qry)

[<Test>]
let ``simple select query with sumBy times two``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            sumBy (od.UnitPrice*od.UnitPrice-2m)
        }
    Assert.Greater(3393420m, qry)
    Assert.Less(3393418m, qry)


[<Test>]
let ``simple select query with averageBy``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            averageBy od.UnitPrice
        }
    Assert.Greater(27m, qry)
    Assert.Less(26m, qry)

let ``simple select query with averageBy length``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for c in dc.Main.Customers do
            averageBy (decimal(c.ContactName.Length))
        }
    Assert.Greater(14m, qry)
    Assert.Less(13m, qry)


[<Test>]
let ``simplest select query with groupBy``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            groupBy cust.City
        } |> Seq.toArray

    Assert.IsNotEmpty(qry)

[<Test>]
let ``simplest select query with groupBy constant``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for p in dc.Main.Products do
            groupBy 1 into g
            select (g.Max(fun p -> p.CategoryId), g.Average(fun p -> p.CategoryId))
        } |> Seq.head

    Assert.AreEqual(fst(qry), 8)
    Assert.Greater(fst(qry), snd(qry))

[<Test>]
let ``simplest select query with groupBy constant with operation``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for p in dc.Main.Products do
            groupBy 1 into g
            select (g.Max(fun p -> p.CategoryId+p.CategoryId+p.CategoryId))
        } |> Seq.head

    Assert.AreEqual(qry, 24)

[<Test; Ignore("Not Supported")>]
let ``simple select query with join groupBy constant``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for o in dc.Main.Orders do
            join od in dc.Main.OrderDetails on (o.OrderId = od.OrderId)
            groupBy 1 into g
            select (g.Max(fun (_,od) -> od.Discount), g.Average(fun (o,_) -> o.Freight),
                    g.Max(fun (o,od) -> (decimal od.Discount) + o.Freight + 1m),
                    g.Sum(fun (o,od)-> (if o.OrderDate < DateTime.Now then 0.0 else (float od.Discount))
                   )
            )
        } |> Seq.toArray

    Assert.IsNotEmpty(qry)

[<Test>]
let ``simple select query with groupBy``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            groupBy cust.City into c
            select (c.Key, c.Count())
        }
    let res = qry |> dict  
    Assert.IsNotEmpty(res)
    Assert.AreEqual(6, res.["London"])

[<Test>]
let ``simple select query with groupBy and then sort``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for order in dc.Main.Orders do
            groupBy (order.ShipCity) into ts
            where (ts.Count() > 1)
            sortBy (ts.Key)
            thenBy (ts.Key)
            select (ts.Key, ts.Average(fun o -> o.Freight))
        }
    let res = qry |> dict  
    Assert.IsNotEmpty(res)
    let lonAvg = res.["London"]
    Assert.Less(50, lonAvg)
    Assert.Greater(100, lonAvg)

[<Test>]
let ``simple select query with groupBy multiple columns``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for p in dc.Main.Products do
            groupBy (p.ReorderLevel, p.CategoryId) into c
            select (c.Key, c.Sum(fun i -> i.UnitPrice))
        }
    let res = qry |> dict  
    Assert.IsNotEmpty(res)

[<Test>]
let ``simple select query with groupBy sum``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            groupBy od.ProductId into p
            select (p.Key, p.Sum(fun f -> f.UnitPrice), p.Sum(fun f -> f.Discount), p.Sum(fun f -> f.UnitPrice+1m))
        } |> Seq.toList

    let _,fstUnitPrice, fstDiscount, plusones = qry.[0]
    Assert.Greater(652m, fstUnitPrice)
    Assert.Less(651m, fstUnitPrice)
    Assert.Greater(2.96m, fstDiscount)
    Assert.Less(2.95m, fstDiscount)
    Assert.Greater(699m, plusones)
    Assert.Less(689m, plusones)
        
[<Test>]
let ``simple select query with groupBy having count``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            groupBy cust.City into c
            where (c.Count() > 1)
            where (c.Count() < 6)
            select (c.Key, c.Count())
        }
    let res = qry |> dict  
    Assert.IsNotEmpty(res)
    Assert.AreEqual(3, res.["Buenos Aires"])

[<Test>]
let ``simple select query with groupBy where and having``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for emp in dc.Main.Employees do
            where (emp.Country = "USA")
            groupBy emp.City into grp
            where (grp.Count() > 0)
            select (grp.Key, grp.Sum(fun e -> e.EmployeeId))
        }
    let res = qry |> dict
    Assert.IsFalse(res.ContainsKey("London"))
    Assert.IsNotEmpty(res)
    Assert.AreEqual(4, res |> Seq.length)
    Assert.AreEqual(9L, res.["Seattle"])


[<Test>]
let ``simple select query with groupBy having key``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            groupBy cust.City into c
            where (c.Key = "London") 
            select (c.Key, c.Count()) 
        } |> Seq.toList
    let res = qry |> dict  
    Assert.IsNotEmpty(res)
    Assert.AreEqual(6, res.["London"])

[<Test>]
let ``simple select query with groupBy date``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for emp in dc.Main.Employees do
            groupBy emp.BirthDate into e
            select (e.Key, e.Count())
        }
    let res = qry |> dict  
    Assert.IsNotEmpty(res)

[<Test>]
let ``simple select query with groupBy2``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.City = "London")
            groupBy (cust.Country, cust.City) into c
            select (c.Key, c.Count()+1)
        } |> dict  
    Assert.IsNotNull(qry)
    Assert.AreEqual(7, qry.Sum(fun k -> k.Value))

[<Test; Ignore("Not Supported")>]
let ``simple select query with groupValBy``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            groupValBy cust.ContactTitle cust.City into g
            select (g.Key, g.Count())
        } |> dict  
    Assert.IsNotEmpty(qry)

[<Test; Ignore("Not Supported")>]
let ``complex select query with groupValBy``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            groupValBy (cust.ContactTitle, Int32.Parse(cust.PostalCode)) (cust.PostalCode, cust.City) into g
            let maxPlusOne = 1 + query {for i in g do sumBy (snd i) }
            select (snd(g.Key), maxPlusOne)
        } |> dict  
    Assert.IsNotEmpty(qry)

[<Test;>]
let ``simple if query``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            if cust.Country = "UK" then select cust.City
        } |> Seq.toArray
    CollectionAssert.IsNotEmpty qry
    CollectionAssert.Contains(qry, "London")
    CollectionAssert.DoesNotContain(qry, "Helsinki")

[<Test;>]
let ``simple select query with case``() = 
    // SELECT CASE WHEN ([cust].[Country] = @param1) THEN [cust].[City] ELSE @param2 END as [result] FROM main.Customers as [cust]
    let dc = sql.GetDataContext(SelectOperations.DatabaseSide)
    let qry = 
        query {
            for cust in dc.Main.Customers do
            select (if cust.Country = "UK" then (cust.City)
                else ("Outside UK"))
        } |> Seq.toArray
    CollectionAssert.IsNotEmpty qry

[<Test;>]
let ``simple select query with case on client``() = 
    // SELECT [Customers].[Country] as 'Country',[Customers].[City] as 'City' FROM main.Customers as [Customers]
    let dc = sql.GetDataContext(SelectOperations.DotNetSide)
    let qry = 
        query {
            for cust in dc.Main.Customers do
            select (if cust.Country = "UK" then (cust.City)
                else ("Outside UK"))
        } |> Seq.toArray
    CollectionAssert.IsNotEmpty qry

[<Test >]
let ``simple select and sort query``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            sortBy cust.City
            select cust.City
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry    
    CollectionAssert.AreEquivalent([|"Aachen"; "Albuquerque"; "Anchorage"|], qry.[0..2])

[<Test >]
let ``simple select and sort desc query``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            sortByDescending cust.City
            select cust.City
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry    
    CollectionAssert.AreEquivalent([|"Århus"; "Warszawa"; "Walla Walla"|], qry.[0..2])

[<Test>]
let ``simple select and sort query with then by query``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            sortBy cust.Country
            thenBy cust.City
            select cust.City
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry    
    CollectionAssert.AreEquivalent([|"Buenos Aires"; "Buenos Aires"; "Buenos Aires"; "Graz"|], qry.[0..3])

[<Test>]
let ``simple select and sort query with then by desc query``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            sortBy cust.Country
            thenByDescending cust.City
            select cust.City
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry    
    CollectionAssert.AreEquivalent([|"Buenos Aires"; "Buenos Aires"; "Buenos Aires"; "Salzburg"|], qry.[0..3])

[<Test>]
let ``simple sort query with lambda cast to IComparable``() =
    let dc = sql.GetDataContext()
    let qry =
        query {
            for cust in dc.Main.Customers do
            sortBy ((fun (c : sql.dataContext.``main.CustomersEntity``) -> c.CustomerId :> IComparable) cust)
            select cust.City
            take 1
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry
    CollectionAssert.AreEquivalent([|"Berlin"|], qry)

[<Test>]
let ``simple sort query with then by desc query with lambda cast to IComparable``() =
    let dc = sql.GetDataContext()
    let qry =
        query {
            for cust in dc.Main.Customers do
            sortBy cust.CompanyName
            thenByDescending ((fun (c : sql.dataContext.``main.CustomersEntity``) -> c.CustomerId :> IComparable) cust)
            select cust.City
            take 1
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry
    CollectionAssert.AreEquivalent([|"Berlin"|], qry)

[<Test >]
let ``simple select query with join``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            join order in dc.Main.Orders on (cust.CustomerId = order.CustomerId)
            select (cust.CustomerId, order.OrderDate)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry
    CollectionAssert.AreEquivalent(
        [|
            "VINET", new DateTime(1996,7,4)
            "TOMSP", new DateTime(1996,7,5)
            "HANAR", new DateTime(1996,7,8)
            "VICTE", new DateTime(1996,7,8)
        |], qry.[0..3])


[<Test; Ignore("Grouping over multiple tables is not supported yet")>]
let ``simple select query with join and then groupBy``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            join order in dc.Main.Orders on (cust.CustomerId = order.CustomerId)
            groupBy cust.City into c
            select (c.Key, c.Count())
        }
    let res = qry |> dict  
    Assert.IsNotEmpty(res)
    Assert.AreEqual(6, res.["London"])

[<Test; Ignore("Joining over grouping is not supported yet")>]
let ``simple select query with groupBy and then join``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            groupBy cust.City into c
            join order in dc.Main.Orders on (c.Key = order.ShipCity)
            select (c.Key, c.Count())
        }
    let res = qry |> dict  
    Assert.IsNotEmpty(res)
    Assert.AreEqual(6, res.["London"])


[<Test >]
let ``simple select query with join multi columns``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            join order in dc.Main.Orders on ((cust.CustomerId, cust.CustomerId) = (order.CustomerId, order.CustomerId))
            select (cust.CustomerId, order.OrderDate)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry
    CollectionAssert.AreEquivalent(
        [|
            "VINET", new DateTime(1996,7,4)
            "TOMSP", new DateTime(1996,7,5)
            "HANAR", new DateTime(1996,7,8)
            "VICTE", new DateTime(1996,7,8)
        |], qry.[0..3])


[<Test >]
let ``simple select query with join using relationships``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            for order in cust.``main.Orders by CustomerID`` do
            select (cust.CustomerId, order.OrderDate)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry
    CollectionAssert.AreEquivalent(
        [|
            "VINET", new DateTime(1996,7,4)
            "TOMSP", new DateTime(1996,7,5)
            "HANAR", new DateTime(1996,7,8)
            "VICTE", new DateTime(1996,7,8)
        |], qry.[0..3])

[<Test; Ignore("Not Supported")>]
let ``simple select query with group join``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            groupJoin ord in dc.Main.Orders on (cust.CustomerId = ord.CustomerId) into g
            for order in g do
            join orderDetail in dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            select (cust.CustomerId, orderDetail.ProductId, orderDetail.Quantity)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry
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
        |], qry.[0..7])

[<Test >]
let ``simple select query with multiple joins on relationships``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            for order in cust.``main.Orders by CustomerID`` do
            for orderDetail in order.``main.OrderDetails by OrderID`` do
            select (cust.CustomerId, orderDetail.ProductId, orderDetail.Quantity)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry
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
        |], qry.[0..7])

[<Test; Ignore("Not Supported")>]
let ``simple select query with left outer join``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            leftOuterJoin order in dc.Main.Orders on (cust.CustomerId = order.CustomerId) into result
            for order in result.DefaultIfEmpty() do
            select (cust.CustomerId, order.OrderDate)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry
    CollectionAssert.AreEquivalent(
        [|
            "VINET", new DateTime(1996,7,4)
            "TOMSP", new DateTime(1996,7,5)
            "HANAR", new DateTime(1996,7,8)
            "VICTE", new DateTime(1996,7,8)
        |], qry.[0..3])

[<Test>]
let ``simple sumBy``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            sumBy od.UnitPrice
        }
    Assert.That(qry, Is.EqualTo(56500.91M).Within(0.001M))

[<Test>]
let ``simple async sum``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            select od.UnitPrice
        } |> Seq.sumAsync |> Async.RunSynchronously
    Assert.That(qry, Is.EqualTo(56500.91M).Within(0.001M))

[<Test>]
let ``simple async sum with operations``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            select ((od.UnitPrice+1m)*od.UnitPrice)
        } |> Seq.sumAsync |> Async.RunSynchronously
    Assert.That(qry, Is.EqualTo(3454230.7769M).Within(0.1M))

[<Test>]
let ``simple async sum with operations 2``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for emp in dc.Main.Employees do
            select (decimal(emp.HireDate.Year)*2m*Math.Min(
                        2m, if emp.HireDate.Subtract(emp.BirthDate.AddYears(1)).Days>0 then
                                Math.Abs(
                                    decimal(emp.HireDate.Subtract(emp.BirthDate).Days)/decimal(emp.HireDate.Subtract(emp.BirthDate.AddYears(1)).Days))
                            else 1m
                    ))
        } |> Seq.sumAsync |> Async.RunSynchronously
    Assert.That(qry, Is.EqualTo(31886.0M).Within(1.0M))

[<Test>]
let ``simple averageBy``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            averageBy od.UnitPrice
        }
    Assert.That(qry, Is.EqualTo(26.2185m).Within(0.001M))

[<Test>]
let ``simple averageByNullable``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            averageByNullable (System.Nullable(od.UnitPrice))
        }
    Assert.That(qry, Is.EqualTo(26.2185m).Within(0.001M))

[<Test >]
let ``simple select with distinct``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            select cust.City
            distinct
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry   
    Assert.AreEqual(69, qry.Length) 
    CollectionAssert.AreEquivalent([|"Aachen"; "Albuquerque"; "Anchorage"|], qry.[0..2])

[<Test>]
let ``simple select with skip``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            select cust.City
            skip 5
        } 
        |> Seq.toArray

    CollectionAssert.IsNotEmpty qry   
    Assert.AreEqual(86, qry.Length) 
    CollectionAssert.AreEquivalent([|"Bergamo"; "Berlin"; "Bern"|], qry.[0..2])

[<Test >]
let ``simple select with take``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            select cust.City
            take 5
        } 
        |> Seq.toArray

    CollectionAssert.IsNotEmpty qry   
    Assert.AreEqual(5, qry.Length) 
    CollectionAssert.AreEquivalent([|"Aachen"; "Albuquerque"; "Anchorage"; "Barcelona"; "Barquisimeto"|], qry)  

[<Test>]
let ``simple select query with all``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for ord in dc.Main.OrderDetails do
            all (ord.UnitPrice > 0m)
        }   
    Assert.IsTrue(qry)

[<Test>]
let ``simple select query with all false``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for ord in dc.Main.OrderDetails do
            all (ord.UnitPrice > 10m)
        }   
    Assert.IsFalse(qry)

[<Test>]
let ``simple select query with find``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for ord in dc.Main.OrderDetails do
            find (ord.UnitPrice > 10m)
        }   
    Assert.AreEqual(14m, qry.UnitPrice)

type Simple = {First : string}

type Dummy<'t> = D of 't

[<Test >]
let ``simple select into a generic type`` () =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for emp in dc.Main.Customers do
            select (D {First=emp.ContactName})
        } |> Seq.toList

    CollectionAssert.IsNotEmpty qry

[<Test >]
let ``simple select into a generic type with pipe`` () =
    if isMono then 
        Console.WriteLine "This is not supported in Mono: simple select into a generic type with pipe"
        Assert.IsFalse false
    else
        let dc = sql.GetDataContext()
        let qry = 
            query {
                for emp in dc.Main.Customers do
                select ({First=emp.ContactName} |> D)
            } |> Seq.toList

        CollectionAssert.IsNotEmpty qry

[<Test >]
let ``simple select with bool outside query``() = 
    let dc = sql.GetDataContext()
    let rnd = System.Random()
    // Direct booleans outside LINQ:
    let myCond1 = true
    let myCond2 = false
    let myCond3 = rnd.NextDouble() > 0.5
    let myCond4 = 4

    let qry = 
        query {
            for cust in dc.Main.Customers do
            // Simple booleans outside queries are supported:
            where (((myCond1 && myCond1=true) && cust.City="Helsinki" || myCond1) || cust.City="London")
            // Boolean in select fetches just either country or address, not both:
            select (if not(myCond3) then cust.Country else cust.Address)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry


[<Test >]
let ``simple select with bool outside query2``() = 
    let dc = sql.GetDataContext()
    let rnd = System.Random()
    // Direct booleans outside LINQ:
    let myCond1 = true
    let myCond2 = false
    let myCond3 = rnd.NextDouble() > 0.5
    let myCond4 = 4

    let qry = 
        query {
            for cust in dc.Main.Customers do
            // Simple booleans outside queries are supported:
            where (myCond4 > 3 || (myCond2 && cust.Address="test" && not(myCond2)))
            // Boolean in select fetches just either country or address, not both:
            select (if not(myCond4=8) then cust.Country else cust.Address)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry

[<Test >]
let ``simple select cross-join test``() = 
    //crossjoin: SELECT ... FROM Customers, Employees
    let dc = sql.GetDataContext()
    let qry = 
        query { 
            for cust in dc.Main.Customers do
            for emp in dc.Main.Employees do
            select (cust.ContactName, emp.LastName)
            take 3
        } |> Seq.toList
    Assert.IsNotNull(qry) 

[<Test >]
let ``simple select cross-join test 3 tables``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query { 
            for cust in dc.Main.Customers do
            for emp in dc.Main.Employees do
            for ter in dc.Main.Territories do
            where (cust.City=emp.City && ter.RegionId > 3L)
            select (cust.ContactName, emp.LastName, ter.RegionId)
            take 3
        } |> Seq.toList
    Assert.IsNotNull(qry) 

[<Test >]
let ``simple select join and cross-join test``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query { 
            for cust in dc.Main.Customers do
            for x in cust.``main.Orders by CustomerID`` do
            for emp in dc.Main.Employees do
            select (x.Freight, cust.ContactName, emp.LastName)
            head
        } 
    Assert.IsNotNull(qry) 


[<Test >]
let ``simple select nested query``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for c3 in (query {
                for b2 in (query {
                    for a1 in (query {
                        for emp in dc.Main.Employees do
                        select emp
                    }) do
                    select a1
                }) do
                select b2.LastName
            }) do
            select c3
        } |> Seq.toList
    Assert.IsNotNull(qry)    

[<Test >]
let ``simple select nested emp query``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            for a1 in (query {
                for emp in dc.Main.Employees do
                select (emp)
            }) do
            where(a1.FirstName = cust.ContactName)
            select (a1.FirstName)
        } |> Seq.toList
    Assert.IsNotNull(qry)    

[<Test; Ignore("Not supported, issue #405")>]
let ``simple select nested query sort``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for emp2 in (query {
                for ter in dc.Main.EmployeesTerritories do
                for emp in ter.``main.Employees by EmployeeID`` do
                select emp
            }) do
            sortBy emp2.EmployeeId
            select emp2.Address
        } |> Seq.toList
    Assert.IsNotNull(qry)    


[<Test>]
let ``simple select join twice to same table``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for emp in dc.Main.Employees do
            join cust1 in dc.Main.Customers on (emp.Country = cust1.Country)
            join cust2 in dc.Main.Customers on (emp.Country = cust2.Country)
            where (cust1.City = "Butte" && cust2.City = "Kirkland")
            select (emp.EmployeeId, cust1.City, cust2.City)
        } |> Seq.toList
    Assert.AreEqual(5,qry.Length)    
    let _,c1,c2 = qry.[0]
    Assert.AreEqual("Butte",c1)    
    Assert.AreEqual("Kirkland",c2)    

[<Test >]
let ``simple select query async``() = 
    let dc = sql.GetDataContext() 
    let task = 
        async {
            let! asyncquery =
                query {
                    for cust in dc.Main.Customers do
                    select cust
                } |> Seq.executeQueryAsync 
            return asyncquery |> Seq.toList
        } |> Async.StartAsTask
    task.Wait()
    CollectionAssert.IsNotEmpty task.Result


type sqlOption = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, CaseSensitivityChange=Common.CaseSensitivityChange.ORIGINAL, UseOptionTypes=true>
[<Test>]
let ``simple select with contains query with where boolean option type``() =
    let dc = sqlOption.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.City.IsSome)
            select cust.CustomerId
            contains "ALFKI"
        }
    Assert.IsTrue(qry)    

type MyOpt = {MyItem: string option}

[<Test>]
let ``simple select with custom option types in where``() =
    let dc = sqlOption.GetDataContext()
    let someItem = {MyItem = Some "London"}
    let noneItem = {MyItem = None}
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.City = someItem.MyItem && not(cust.City = noneItem.MyItem))
            select cust.CustomerId
            headOrDefault
        }
    Assert.AreEqual("AROUT",qry)    


[<Test>]
let ``simple async sum with option operations``() = 
    let dc = sqlOption.GetDataContext()
    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            where (od.UnitPrice>0m)
            select ((od.UnitPrice)*(decimal)od.OrderId)
        } |> Seq.sumAsync |> Async.RunSynchronously
    Assert.That(qry, Is.EqualTo(603221955M).Within(10M))


[<Test>]
let ``simple math operations query``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for p in dc.Main.Products do
            where (p.ProductId - 21L = 23L)
            select p.ProductId
        } |> Seq.toList

    Assert.AreEqual(44L, qry.Head)

let ``simple math operations query2``() =
    let dc = sql.GetDataContext()
    let qry2 = 
        query {
            for p in dc.Main.Products do
            where (p.UnitPrice <> 30m && ((p.UnitPrice - 30m) = (30m - p.UnitPrice)))
            select p.UnitPrice
        } |> Seq.toList
    CollectionAssert.IsEmpty qry2

[<Test >]
let ``simple canonical operation substing query``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            // Database spesific warning here: Substring of SQLite starts from 1.
            where (cust.CustomerId.Substring(2,3)+"F"="NATF")
            select cust.CustomerId
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry
    Assert.AreEqual(1, qry.Length)
    Assert.IsTrue(qry.Contains("ANATR"))

[<Test >]
let ``simple canonical operations query``() =
    let dc = sql.GetDataContext()

    let qry = 
        let L = "L"
        query {
            // Silly query not hitting indexes, so testing purposes only...
            for cust in dc.Main.Customers do
            join emp in dc.Main.Employees on (cust.City.Trim() + "x" + cust.Country = emp.City.Trim() + "x" + emp.Country)
            join secondCust in dc.Main.Customers on (cust.City + emp.City + "A" = secondCust.City + secondCust.City + "A")
            where (
                cust.City + emp.City + cust.City + emp.City + cust.City = cust.City + emp.City + cust.City + emp.City + cust.City
                && abs(emp.EmployeeId)+1L > 4L 
                && cust.City.Length + secondCust.City.Length + emp.City.Length = 3 * cust.City.Length
                && (cust.City.Replace("on","xx") + L).Replace("xx","on") + ("O" + L) = "London" + "LOL" 
                && cust.City.IndexOf("n")>0 && cust.City.IndexOf(cust.City.Substring(1,cust.City.Length-1))>0
                && Math.Max(emp.BirthDate.Date.AddYears(3).Month + 1, 0) > 3
                && emp.BirthDate.AddDays(1.).Subtract(emp.BirthDate).Days=1
            )
            sortBy (abs(abs(emp.BirthDate.Day * emp.BirthDate.Day)))
            select (cust.CustomerId, cust.City, emp.BirthDate)
            distinct
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry
    Assert.AreEqual(12, qry.Length)
    Assert.IsTrue(qry.[0] |> fun (id,c,b) -> c="London" && b.Month>2)

[<Test>]
let ``simple canonical operations case-when-elses``() =
    let dc = sql.GetDataContext()

    let qry1 = 
        query {
            for cust in dc.Main.Customers do
            join emp in dc.Main.Employees on (cust.City.Trim() + "_" + cust.Country = emp.City.Trim() + "_" + emp.Country)
            where ((if box(emp.BirthDate)=null then 200 else 100) = 100) 
            where ((if emp.EmployeeId > 1L then 200 else 100) = 100) 
            where ((if emp.BirthDate > emp.BirthDate then 200 else 100) = 100)
            select (cust.CustomerId, cust.City, emp.BirthDate)
            distinct
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry1

    let qry2 = 
        query {
            for cust in dc.Main.Customers do
            where ((if cust.City=cust.ContactName then cust.City else cust.Address)<>"x") 
            where ( (if cust.City.Substring(0,3)<>"Lond" then cust.City else cust.Address) = "London")
            select (cust.City)
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry2

[<Test >]
let ``simple operations in select query``() =
    let dc = sql.GetDataContext()

    let qry = 
        let L = "L"
        query {
            for cust in dc.Main.Customers do
            join emp in dc.Main.Employees on (cust.City + "_" + cust.Country = emp.City + "_" + emp.Country)
            join secondCust in dc.Main.Customers on (cust.City = secondCust.City)
            select (
                cust.City + emp.City + cust.City + emp.City + cust.City = cust.City + emp.City + cust.City + emp.City + cust.City
                && abs(emp.EmployeeId)+1L > 4L 
                && cust.City.Length + secondCust.City.Length + emp.City.Length = 3 * cust.City.Length
                && (cust.City.Replace("on","xx") + L).Replace("xx","on") + ("O" + L) = "London" + "LOL" 
                && cust.City.IndexOf("n")>0 && cust.City.IndexOf(cust.City.Substring(1,cust.City.Length-1))>0
                && Math.Max(emp.BirthDate.Date.AddYears(3).Month + 1, 0) > 3
            )
            distinct
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry
    Assert.AreEqual(6, qry.Length)

[<Test>]
let ``simple canonical operations in nested select query``() =
    let dc = sql.GetDataContext()

    let qry1 = 
        let L = "L"
        query {
            for cust in dc.Main.Customers do
            join emp in dc.Main.Employees on (cust.City + "_" + cust.Country = emp.City + "_" + emp.Country)
            join secondCust in dc.Main.Customers on (cust.City = secondCust.City)
            select (cust.City.Replace("on","xx") + secondCust.City.Length.ToString())
            distinct
        } 
    let qry2 = 
        query {
            for cust in dc.Main.Customers do
            where (qry1.Contains(cust.City.Replace("on","xx") + "6"))
            select cust
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry2
    Assert.AreEqual(6, qry2.Length)

[<Test>]
let ``simple union query test``() = 
    let dc = sql.GetDataContext()
    let query1 = 
        query {
            for cus in dc.Main.Customers do
            where (cus.City <> "Atlantis1")
            select (cus.City)
        }
    let query2 = 
        query {
            for emp in dc.Main.Employees do
            where (emp.City <> "Atlantis2")
            select (emp.City)
        } 

    // Union: query1 contains 69 distinct values, query2 distinct 5 and res1 is 71 distinct values
    let res1 = query1.Union(query2) |> Seq.toArray
    Assert.IsNotEmpty(res1)
    // Intersect contains 3 values:
    let res2 = query1.Intersect(query2) |> Seq.toArray
    Assert.IsNotEmpty(res2)
    // Except contains 2 values:
    let res3 = query2.Except(query1) |> Seq.toArray
    Assert.IsNotEmpty(res3)
    

[<Test>]
let ``simple union all query test``() = 
    let dc = sql.GetDataContext()
    let query1 = 
        query {
            for cus in dc.Main.Customers do
            select (cus.City)
        }
    let query2 = 
        query {
            for emp in dc.Main.Employees do
            select (emp.City)
        } 

    // Union all:
    // query1 contains 91 values and query2 contains 8 so res2 contains 99 values.
    let res2 = query1.Concat(query2) |> Seq.toArray
    Assert.IsNotEmpty(res2)
    
[<Test>]
let ``verify groupBy results``() = 
    let dc = sql.GetDataContext()
    let enumtest =
        query {
            for cust in dc.Main.Customers do
            select (cust)
        } |> Seq.toList
    let inlogics = 
        query {
            for cust in enumtest do
            groupBy cust.City into c
            select (c.Key, c.Count())
        } |> Seq.toArray |> Array.sortBy (fun (k,v) -> k )

    let groupqry = 
        query {
            for cust in dc.Main.Customers do
            groupBy cust.City into c
            select (c.Key, c.Count())
        } |> Seq.toArray |> Array.sortBy (fun (k,v) -> k )
    let res = groupqry |> dict  

    CollectionAssert.AreEqual(inlogics,groupqry)

[<Test >]
let ``simple delete where query``() =
    let dc = sql.GetDataContext()
    query {
        for cust in dc.Main.Customers do
        where (cust.City = "Atlantis" || cust.CompanyName = "Home")
    } |> Seq.``delete all items from single table`` 
    |> Async.RunSynchronously |> ignore
    ()

[<Test>]
let ``simple left join``() = 
    let dc = sqlOption.GetDataContext()
    let qry = 
        query {
            for o in dc.Main.Orders do
            for c in (!!) o.``main.Customers by CustomerID`` do
            select (o.CustomerId, c.CustomerId)
        } |> Seq.toArray
    
    let hasNulls = qry |> Seq.map(fst) |> Seq.filter(Option.isNone) |> Seq.isEmpty |> not
    Assert.IsTrue hasNulls


[<Test>]
let ``simple quert sproc result``() = 
    let dc = sql.GetDataContext()
    let pragmaSchemav = dc.Pragma.Get.Invoke("schema_version")
    let res = pragmaSchemav.ResultSet |> Array.map(fun i -> i.ColumnValues |> Map.ofSeq)
    let ver = (res |> Seq.head).["schema_version"] :?> Int64
    Assert.IsTrue(ver > 1L)

    let pragmaFk = dc.Pragma.GetOf.Invoke("foreign_key_list", "EmployeesTerritories")
    let res = pragmaFk.ResultSet |> Array.map(fun i -> i.ColumnValues |> Map.ofSeq)
    Assert.IsNotNull(res)

    let pragmaSchemaAsync = 
        dc.Pragma.Get.InvokeAsync("schema_version")
        |> Async.RunSynchronously
    Assert.IsNotNull(pragmaSchemaAsync.ResultSet)
