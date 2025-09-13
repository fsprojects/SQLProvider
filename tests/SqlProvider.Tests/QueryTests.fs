#if INTERACTIVE
#I @"../../bin/lib/net48/"
#r "FSharp.Data.SqlProvider.Common.dll"
#r "FSharp.Data.SqlProvider.dll"
#r @"../../packages/tests/NUnit/lib/netstandard2.0/nunit.framework.dll"
#else
module QueryTests
#endif

open System
open FSharp.Data.Sql
open System.Linq
open NUnit.Framework

// System.Data.Sqlite connection string:
[<Literal>]
let connectionString =  @"Data Source=" + __SOURCE_DIRECTORY__ + @"/db/northwindEF.db;Version=3;Read Only=false;FailIfMissing=True;"

// Microsoft.Data.Sqlite connection string:
//[<Literal>]
//let connectionString =  @"Data Source=" + __SOURCE_DIRECTORY__ + @"/db/northwindEF.db"

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + "/libs"

// If you want to run these in Visual Studio Test Explorer, please install:
// Tools -> Extensions and Updates... -> Online -> NUnit Test Adapter for Visual Studio
// http://nunit.org/index.php?p=vsTestAdapter&r=2.6.4

type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE,
                           connectionString,
                           CaseSensitivityChange=Common.CaseSensitivityChange.ORIGINAL,
                           ResolutionPath = resolutionPath,
                           //SQLiteLibrary=Common.SQLiteLibrary.MicrosoftDataSqlite
                           SQLiteLibrary=Common.SQLiteLibrary.SystemDataSQLite>

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

[<Test >] // Generates COUNT(DISTINCT CustomerId)
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

[<Test; Ignore("AVG-DISTINCT not supported. Generates 'DISTINCT AVG(UnitPrice)' where maybe should generate 'AVG(DISTINCT UnitPrice)' ")>]
let ``simple select with distinct avg``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for o in dc.Main.OrderDetails do
            distinct
            averageBy(o.UnitPrice)
        }
    Assert.AreEqual(91, qry)

[<Test >]
let ``simple select with read only context``() =

    let dc2 = sql.GetReadOnlyDataContext()
    // dc2 doesn't contain SubmitUpdates()
    // This helps you to segregate read and write operations

    let res = 
        query {
            for cust in dc2.Main.Customers do
            take 1
            select cust
        } |> Seq.head

    try
        // Trying to modify will throw an exception
        res.City <- "modified"
    with
    | _ ->
        Assert.True true

[<Test >]
let ``simple select with read only context should contain the same types for easier code share``() =
    let getCust customers =
        let res = 
            query {
                for cust in customers do
                take 1
                select cust
            } |> Seq.toArray
        res

    let dc1 = sql.GetDataContext()
    let dc2 = sql.GetReadOnlyDataContext()

    let c1 = getCust dc1.Main.Customers
    let c2 = getCust dc2.Main.Customers

    CollectionAssert.IsNotEmpty c1
    CollectionAssert.IsNotEmpty c2
    let sameTypes = c1.[0].GetType() = c2.[0].GetType()
    Assert.IsTrue sameTypes

    // Also can be passed as readonly
    let dc3 = dc1.AsReadOnly()
    let res2 = 
        query {
            for cust in dc3.Main.Customers do
            take 1
            select cust
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty res2

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

[<Test>]
let ``over 8 joins test``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            join ord1 in dc.Main.Orders on (cust.CustomerId = ord1.CustomerId)
            join ord2 in dc.Main.Orders on (cust.CustomerId = ord2.CustomerId)
            join ord3 in dc.Main.Orders on (cust.CustomerId = ord3.CustomerId)
            join ord4 in dc.Main.Orders on (cust.CustomerId = ord4.CustomerId)
            join ord5 in dc.Main.Orders on (cust.CustomerId = ord5.CustomerId)
            join ord6 in dc.Main.Orders on (cust.CustomerId = ord6.CustomerId)
            join ord7 in dc.Main.Orders on (cust.CustomerId = ord7.CustomerId)
            join ord8 in dc.Main.Orders on (cust.CustomerId = ord8.CustomerId)
            join ord9 in dc.Main.Orders on (cust.CustomerId = ord9.CustomerId)
            where (cust.CustomerId = "ALFKI"
                && ord1.OrderId = 10643L && ord2.OrderId = 10643L && ord3.OrderId = 10643L
                && ord4.OrderId = 10643L && ord5.OrderId = 10643L && ord6.OrderId = 10643L
                && ord7.OrderId = 10643L && ord8.OrderId = 10643L && ord9.OrderId = 10643L
                )
            select (Some (cust, ord9))
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

[<Test>]
let ``outer join with inner join test``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            join ord1 in dc.Main.Orders on (cust.CustomerId = ord1.CustomerId)
            join ord2 in (!!) dc.Main.Orders on (cust.CustomerId = ord2.CustomerId)
            for ord3 in ord2.``main.OrderDetails by OrderID`` do
            join ord4 in (!!) dc.Main.Orders on (ord2.CustomerId = ord4.CustomerId)
            where (cust.CustomerId = "ALFKI"
                && ord1.OrderId = 10643L && ord2.OrderId = 10643L && ord3.OrderId = 10643L
                && ord4.OrderId = 10643L
                )
            select (Some (cust, ord1))
            head
        } 

    Assert.AreNotEqual(None,qry)

[<Test>]
let ``simple select two queries test``() =
    let dc = sql.GetDataContext(SelectOperations.DatabaseSide)
    // Works also with: let dc = sql.GetDataContext(SelectOperations.DotNetSide)
    let itm1 = 
        query {
            for cust in dc.Main.Customers do
            select (cust)
            head
        } 
    let itm2, isOk = 
        query {
            for o in dc.Main.Orders do
            where(o.CustomerId = itm1.CustomerId)
            select (o, (o.CustomerId = itm1.CustomerId))
            head
        } 
    Assert.AreEqual(itm1.CustomerId, itm2.CustomerId)
    Assert.IsTrue isOk

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
    Assert.AreEqual("Obere Str. 57", qry.[0].Address)  

[<Test >]
let ``simple select query with different return types``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for ord in dc.Main.Orders do
            select (ord.Freight, ord.CustomerId, ord.OrderDate, ord.EmployeeId)
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
    Assert.AreEqual("Germany", fst qry.[0])

[<Test>]
let ``simplest select into where``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            select (cust.City + "te") into y
            select (y+"st") into y2
            where (y2 <> "Helsinktest")
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry
    CollectionAssert.Contains(qry, "Aachentest")

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
    CollectionAssert.Contains(qry, "Obere Str. 57")

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
    Assert.AreEqual("Obere Str. 57", fst qry.[0])

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
    CollectionAssert.Contains(qry, "Berlintest")
    
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
    CollectionAssert.Contains(qry, "Berlintest")

[<Test>]
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
    CollectionAssert.Contains(qry, "Berlintest")

[<Test>]
let ``simple select query let temp used in select database``() = 
    let dc = sql.GetDataContext(SelectOperations.DatabaseSide)
    let qry = 
        query {
            for cust in dc.Main.Customers do
            let t1 = cust.City + "te"
            select (t1 + "st", cust.PostalCode)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry
    Assert.AreEqual("Berlintest", fst qry.[0])

[<Test >]
let ``simple select query with operations in select``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            select (cust.Country + " " + cust.Address + (1).ToString())
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry
    CollectionAssert.Contains(qry, "Germany Berliner Platz 431")

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
let ``simple select where in set query``() =
    let dc = sql.GetDataContext()
    let itmSet = ["ALFKI"; "ANATR"; "AROUT"] |> Set.ofList
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (itmSet.Contains(cust.CustomerId))
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
    CollectionAssert.Contains(query2, "FISSA")

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
    CollectionAssert.Contains(qry, "FRANR")

[<Test >]
let ``simple select where like query2``() =
    let dc = sql.GetDataContext()

    let x = {| Test = "a" |}
    let itm1 = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId.Contains x.Test && cust.CustomerId > (string x.Test) )
            select cust
        } |> Seq.head

    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (cust.CustomerId.Contains itm1.CustomerId && int(cust.CustomerId) >= int(itm1.CustomerId))
            select cust.CustomerId
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry
    CollectionAssert.Contains(qry, "ALFKI")


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

[<Test; Ignore("Not supported, but you can do this via: query { ... select od.UnitPrice } |> Seq.sumAsync or |> Seq.sumQuery")>]
let ``simple select query with sumBy join``() = 
    let dc = sql.GetDataContext()

    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            join o in dc.Main.Orders on (od.OrderId=o.OrderId)
            sumBy od.UnitPrice
        }

    Assert.Greater(56501m, qry)
    Assert.Less(56499m, qry)

[<Test>]
let ``subquery with 1 tables``() = 
    let dc = sql.GetDataContext()

    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            where (od.UnitPrice <> 0m)
        }

    let qry2 =
        qry.Select(fun od -> od.UnitPrice)
    let qry2 = qry2 |> Seq.toList

    Assert.IsNotEmpty(qry2)


[<Test; Ignore("Not supported")>]
let ``subquery with 2 tables``() = 
    let dc = sql.GetDataContext()

    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            join o in dc.Main.Orders on (od.OrderId=o.OrderId)
            where (od.UnitPrice <> 0m)
        }

    let qry2 =
        qry.Select(fun (od, o) -> od.UnitPrice)
    let qry2 = qry2 |> Seq.toList

    Assert.IsNotEmpty(qry2)

[<Test>]
let ``simple where before join test``() = 
    let dc = sql.GetDataContext()

    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            where(od.OrderId > 0L)
            join o in dc.Main.Orders on (od.OrderId=o.OrderId)
            sortBy od.OrderId
            select (od.OrderId, o.OrderId)
        } |> Seq.toArray

    Assert.AreEqual(qry.Length, 2155)
    Assert.LessOrEqual((fst qry.[0]), (fst qry.[1000]))
    CollectionAssert.Contains(qry, (10257L, 10257L))

[<Test>]
let ``simple where before join test2``() = 
    let dc = sql.GetDataContext()

    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            where(od.UnitPrice > 100m)
            for ord in od.``main.Orders by OrderID`` do
            sortBy ord.ShipCity
            select (ord)
        } |> Seq.toArray

    Assert.AreEqual(qry.Length, 46)
    Assert.LessOrEqual(qry.[0].ShipCity, qry.[40].ShipCity)


[<Test>]
let ``simple navigation sum async``() = 
    let dc = sql.GetDataContext()

    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            for ord in od.``main.Orders by OrderID`` do
            select (ord.Freight)
        } |> Seq.sumAsync

    let res = 
        qry |> Async.AwaitTask |> Async.RunSynchronously

    Assert.GreaterOrEqual(res, 0m)

[<Test>]
let ``simple where before join test3``() = 
    let dc = sql.GetDataContext()

    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            for prod in od.``main.Products by ProductID`` do
            where(od.UnitPrice > 100m && prod.Discontinued = false)
            for ord in od.``main.Orders by OrderID`` do
            for cust in ord.``main.Customers by CustomerID`` do
            where (cust.CustomerId <> "ALFKI")
            sortBy ord.ShipCity
            select (ord)
        } |> Seq.toArray

    Assert.AreEqual(qry.Length, 24)
    Assert.LessOrEqual(qry.[0].ShipCity, qry.[20].ShipCity)

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

[<Test>]
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
let ``simplest select query with groupBy 2 columns``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            groupBy (cust.City, cust.Address)
        } |> Seq.toArray

    Assert.IsNotEmpty(qry)

[<Test>]
let ``simplest select query with groupBy key aggregate``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            groupBy (cust.City+"1") into g
            select g.Key
        } |> Seq.toArray

    Assert.IsNotEmpty(qry)


[<Test>]
let ``simplest select query with groupBy aggregate``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for o in dc.Main.Orders do
            groupBy o.OrderDate.Date into g
            select (g.Key, g.Sum(fun p -> if p.Freight > 1m then p.Freight else 0m))
        } |> Seq.toArray

    Assert.IsNotEmpty(qry)

[<Test>]
let ``simplest select query with groupBy aggregate via select``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for o in dc.Main.Orders do
            groupBy o.OrderDate.Date into g
            select (g.Key, g.Select(fun p -> if p.Freight > 1m then p.Freight else 0m).Sum())
        } |> Seq.toArray

    Assert.IsNotEmpty(qry)

[<Test>]
let ``simplest select query with groupBy aggregate temp total``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for o in dc.Main.Orders do
            groupBy o.OrderDate.Date into g
            let total = query {
                for s in g do
                sumBy s.Freight
            }
            select (g.Key, total)
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

[<Test>]
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
            select (
                p.Key,
                p.Sum(fun f -> f.UnitPrice),
                p.Sum(fun f -> f.Discount),
                p.Sum(fun f -> f.UnitPrice+1m),
                p.Sum(fun f -> f.UnitPrice+2m)
                )
        } |> Seq.toList

    let mapped = qry |> List.map(fun (k,v1,v2,v3,v4) -> k, (v1,v2,v3,v4)) |> Map.ofList
    let fstUnitPrice, fstDiscount, plusones, plustwos = mapped.[1L]
    Assert.Greater(652m, fstUnitPrice)
    Assert.Less(651m, fstUnitPrice)
    Assert.Greater(2.96m, fstDiscount)
    Assert.Less(2.95m, fstDiscount)
    Assert.Greater(699m, plusones)
    Assert.Less(689m, plusones)
    Assert.Greater(plustwos, plusones)
        
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
let ``simple select query with groupBy having nested``() = 
    let dc = sql.GetDataContext()
    let subQry = 
        query {
            for cust in dc.Main.Customers do
            groupBy cust.City into c
            where (c.Key = "London") 
            select (c.Key) 
        }
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (subQry.Contains(cust.City))
            select (cust.CustomerId) 
        } |> Seq.toArray

    Assert.IsNotEmpty(qry)
    Assert.Contains("AROUT", qry)

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

[<Test>]
let ``simple select query with groupBy complex operations``() = 
    let dc = sql.GetDataContext(SelectOperations.DatabaseSide)
    let old = System.DateTime(1990,01,01)
    let qry = 
        query {
            for o in dc.Main.Orders do
            where (o.OrderDate > old)
            groupBy (o.ShipCountry, o.ShipCity) into c
            select (
                c.Key,
                c.Sum(fun (ord) -> if ord.ShipCity = "London" then ord.Freight+1m else ord.Freight))
        } |> dict
    Assert.IsNotNull(qry)
    Assert.AreEqual(458.91m, qry.["Belgium", "Bruxelles"])
    Assert.AreEqual(2151.67m, qry.["UK", "London"])

[<Test>]
let ``simple select query with groupBy join complex operations``() = 
    let dc = sql.GetDataContext()
    let old = System.DateTime(1990,01,01)
    let qry = 
        query {
            for o in dc.Main.Orders do
            join od in dc.Main.OrderDetails on (o.OrderId = od.OrderId)
            where (o.OrderDate > old)
            groupBy (o.ShipCountry, o.ShipCity) into c
            select (
                c.Key,
                c.Sum(fun (ord,od) -> if ord.ShipCity = "London" then ord.Freight+1m else ord.Freight))
        } |> Seq.toArray
    Assert.IsNotNull(qry)

[<Test; Ignore("Not Supported")>]
let ``simple groupValBy item``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            groupValBy cust.ContactTitle cust.City 
        } |> Seq.toList
    Assert.IsNotEmpty(qry)

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

[<Test>]
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

[<Test>]
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
    CollectionAssert.Contains(qry, "London")
    CollectionAssert.Contains(qry, "Outside UK")

[<Test>]
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
    CollectionAssert.Contains(qry, "London")
    CollectionAssert.Contains(qry, "Outside UK")

[<Test>]
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

[<Test>]
let ``simple select and sort query2``() =
    let dc = sql.GetDataContext()
    let sortbyCity=true
    let qry = 
        query {
            for cust in dc.Main.Customers do
            sortBy (if sortbyCity then "1" else cust.Address)
            thenBy (cust.City)
            select cust.City
        }
    let qry = qry |> Seq.toArray

    CollectionAssert.IsNotEmpty qry    
    CollectionAssert.AreEquivalent([|"Aachen"; "Albuquerque"; "Anchorage"|], qry.[0..2])

[<Test>]
let ``simple select and sort query3``() =
    let dc = sql.GetDataContext()
    let sortbyCity="asdf"
    let qry = 
        query {
            for cust in dc.Main.Customers do
            sortBy (
                match sortbyCity with
                | "a" -> (string) cust.Address
                | "b" -> (string) cust.Address
                | _ -> (string) cust.City)
            select cust.City
        }
    let qry = qry |> Seq.toArray

    CollectionAssert.IsNotEmpty qry    
    CollectionAssert.AreEquivalent([|"Aachen"; "Albuquerque"; "Anchorage"|], qry.[0..2])


[<Test>]
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
let ``simple date opearations with implicit convert``() = 
    let dc = sql.GetDataContext()
    let today = DateTime.Today
    let qry = 
        query {
            for emp in dc.Main.Employees do
            where (emp.BirthDate.AddHours 5 < today)
            select (emp.BirthDate.AddDays 5)
        } |> Seq.toList
    CollectionAssert.IsNotEmpty qry

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

[<Test>]
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


[<Test>]
let ``simple select query with join and then groupBy``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
                for cust in dc.Main.Customers do
                join order in dc.Main.Orders on (cust.CustomerId = order.CustomerId)
                where(cust.Address <> "Road" && order.ShipName <> "mcboatface")
                groupBy (cust.City, order.ShipCity) into g
                select (g.Key, g.Max(fun (c,o) -> c.PostalCode))
            } 
    let res = qry |> dict  
    Assert.IsNotEmpty(res)
    Assert.AreEqual("WX3 6FW", res.["London","London"])

[<Test>]
let ``simple select query with join and then groupBy 2``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
                for cust in dc.Main.Customers do
                join order in dc.Main.Orders on (cust.CustomerId = order.CustomerId)
                join order2 in dc.Main.Orders on (order.OrderId = order2.OrderId)
                where(order2.ShipCity = "London")
                groupBy (order.ShipName, order.ShipCity, order2.ShipCity, order.ShipCountry, order2.ShippedDate) into g
                select (g.Key, g.Max(fun (c,o1,o2) -> c.PostalCode))
            }
    let res = qry |> Seq.toList  
    Assert.IsNotEmpty(res)
    Assert.AreEqual(32, res.Length)

[<Test>]
let ``simple select query with joins and then groupBy``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
                for cust in dc.Main.Customers do
                join order in dc.Main.Orders on (cust.CustomerId = order.CustomerId)
                join order2 in dc.Main.Orders on (cust.CustomerId = order2.CustomerId)
                groupBy (cust.City, order.ShipCity) into g
                select (g.Key, g.Max(fun (c,o,o2) -> c.PostalCode))
            }
    let res = qry |> dict  
    Assert.IsNotEmpty(res)
    Assert.AreEqual("WX3 6FW", res.["London","London"])

[<Test; Ignore("Grouping over 4 tables is not supported yet")>]
let ``simple select query with many joins and then groupBy``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
                for cust in dc.Main.Customers do
                join order in dc.Main.Orders on (cust.CustomerId = order.CustomerId)
                join order2 in dc.Main.Orders on (cust.CustomerId = order2.CustomerId)
                join order3 in dc.Main.Orders on (cust.CustomerId = order3.CustomerId)
                groupBy (cust.City, order.ShipCity) into g
                select (g.Key, g.Max(fun (c,o,o2,o3) -> c.PostalCode))
            }
    let res = qry |> dict  
    Assert.IsNotEmpty(res)
    Assert.AreEqual("WX3 6FW", res.["London","London"])

[<Test; Ignore("Joining over grouping is not supported yet")>]
let ``simple select query with groupBy and then join``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            groupBy cust.City into c
            join order in dc.Main.Orders on (c.Key = order.ShipCity)
            select (c.Key, c.Count(), order.ShipCity)
        }
    let res = qry |> Seq.toList  
    Assert.IsNotEmpty(res)

[<Test>]
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


[<Test>]
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

[<Test>]
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

[<Test; Ignore("Not Supported, but you can use (!!) operator for left join")>]
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
        } |> Seq.sumAsync |> Async.AwaitTask |> Async.RunSynchronously
    Assert.That(qry, Is.EqualTo(56500.91M).Within(0.001M))

[<Test>]
let ``simple async sum with operations``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            select ((od.UnitPrice+1m)*od.UnitPrice)
        } |> Seq.sumAsync |> Async.AwaitTask |> Async.RunSynchronously
    Assert.That(qry, Is.EqualTo(3454230.7769M).Within(0.1M))

[<Test>]
let ``simple async sum with join and operations``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            join o in dc.Main.Orders on (od.OrderId = o.OrderId)
            select ((od.UnitPrice+1m)*od.UnitPrice)
        } |> Seq.sumAsync |> Async.AwaitTask |> Async.RunSynchronously

    Assert.That(qry, Is.EqualTo(3454230.7769M).Within(0.1M))

    let qry2 = 
        query {
            for o in dc.Main.Orders do
            join od in dc.Main.OrderDetails on (o.OrderId = od.OrderId)
            select ((od.UnitPrice+1m)*od.UnitPrice)
        } |> Seq.sumAsync |> Async.AwaitTask |> Async.RunSynchronously

    Assert.That(qry2, Is.EqualTo(3454230.7769M).Within(0.1M))

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
        } |> Seq.sumAsync |> Async.AwaitTask |> Async.RunSynchronously
    Assert.That(qry, Is.EqualTo(31886.0M).Within(1.0M))

[<Test>] 
// Note: 
// Use float if you try to do any math, don't use decimal.
// Decimal is by intention of SQL-drivers handled as text to be non-lossy,
// but that's why you cant do math on SQLite decimals.
let ``simple math op``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for od in dc.Main.OrderDetails do
            where ((float od.UnitPrice) * (float od.Quantity) > 100.)
            select (od.OrderId, (float od.UnitPrice) * (float od.Quantity) > 100.)
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry

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
    CollectionAssert.Contains(qry, ("Maria Anders", "Buchanan"))


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
    CollectionAssert.Contains(qry, ("Karl Jablonski", "Davolio", 4L))

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

[<Test>]
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
    CollectionAssert.Contains(qry, "Fuller")

[<Test>]
let ``simple select nested emp query``() = 
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            for a1 in (query {
                for emp in dc.Main.Employees do
                select (emp)
            }) do
            where(a1.FirstName = cust.ContactName || a1.City = cust.City)
            select (a1.FirstName)
        } |> Seq.toList
    Assert.IsNotNull(qry)    
    CollectionAssert.Contains(qry, "Anne")

[<Test >]
let ``simple select entityValue form another query``() = 
    let dc = sql.GetDataContext()
    let ent1 = 
        query {
            for cust in dc.Main.Customers do
            select (cust)
        } |> Seq.head

    let ent2 = 
        query {
            for c in dc.Main.Customers do
            where (c.CustomerId = ent1.CustomerId)
            select (c)
        } |> Seq.head

    Assert.IsNotNull(ent2)    
    Assert.AreEqual(ent1.CustomerId, ent2.CustomerId)    

[<Test>]
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

[<Test>]
let ``simple select with multiple table joins with 4 tables``() =
    let dc = sql.GetDataContext()
    let results =
        query {
            for customer in dc.Main.Customers do
            join order in dc.Main.Orders on (customer.CustomerId = order.CustomerId)
            join orderDetail in dc.Main.OrderDetails on (order.OrderId = orderDetail.OrderId)
            join product in dc.Main.Products on (orderDetail.ProductId = product.ProductId)
            where (customer.Country = "USA")
            select (customer.ContactName, product.ProductName)
        } |> Seq.toArray
    Assert.IsTrue(results.Length >= 0)

[<Test >]
let ``simple select query async``() = 
    let dc = sql.GetDataContext() 
    let task = 
        task {
            let! asyncquery =
                query {
                    for cust in dc.Main.Customers do
                    select cust
                } |> Seq.executeQueryAsync 
            return asyncquery |> Seq.toList
        }
    task.Wait()
    CollectionAssert.IsNotEmpty task.Result

[<Test >]
let ``simple select query async2``() = 
    let dc = sql.GetDataContext() 
    let res = 
        task {
            let! asyncquery =
                query {
                    for cust in dc.Main.Customers do
                    where (cust.City <> "")
                    select (cust.Address, cust.City, cust.ContactName)
                } |> Seq.executeQueryAsync 
            return asyncquery
        } |> Async.AwaitTask |> Async.RunSynchronously
    CollectionAssert.IsNotEmpty res
    let r = res |> Seq.toArray
    CollectionAssert.Contains(r, ("55 Grizzly Peak Rd.", "Butte", "Liu Wong"))

[<Test>]
let ``simple select query async3``() =
    let dc = sql.GetDataContext() 
    let res = 
        task {
            let asyncquery =
                query {
                    for cust in dc.Main.Customers do
                    where (cust.City <> "")
                }
            // Let's mix some good old LINQ. (Not recommended!) Note: the query above didn't have Select, it's returning cust.
            let res = asyncquery.Where(fun cust -> cust.City = "London").Select(fun cust -> (cust.Address, cust.City, cust.ContactName)).Distinct()
            let! d = res |> Seq.lengthAsync
            return d
        } |> Async.AwaitTask |> Async.RunSynchronously
    Assert.IsTrue(res > 0)
    ()

[<Test>]
let ``simple select query async4``() =
    let dc = sql.GetDataContext() 
    let res = 
        task {
            let asyncquery =
                query {
                    for cust in dc.Main.Customers do
                    where (cust.City <> "")
                    select cust
                }

            let! res = asyncquery |> Seq.headAsync
            return res
        } |> Async.AwaitTask |> Async.RunSynchronously
    Assert.IsNotNull(res)
    ()


[<Test>]
let ``simple select query async5``() =
    let dc = sql.GetDataContext()
    async {
        let! city, country = 
            task {
                let asyncquery =
                    query {
                        for cust in dc.Main.Customers do
                        where (cust.City <> "")
                        select (cust.City, cust.Country)
                    }

                let! res = asyncquery |> Seq.headAsync
                return res
            } |> Async.AwaitTask
        Assert.IsNotNull(city)
        Assert.IsNotNull(country)
     } |> Async.RunSynchronously
    ()

type CustomType = {
    Location : String;
    Country : String;
}

[<Test>]
let ``simple select query async6``() =
    let dc = sql.GetDataContext()
    async {
        let! customRec = 
            task {
                let asyncquery =
                    query {
                        for cust in dc.Main.Customers do
                        where (cust.City <> "")
                        select { Location = cust.City; Country = cust.Country }
                    }

                let! res = asyncquery |> Seq.headAsync
                return res
            } |> Async.AwaitTask
        Assert.IsNotNull(customRec)
        Assert.IsNotNull(customRec.Location)
     } |> Async.RunSynchronously
    ()

[<Test>]
let ``simple select query lengthAsync``() =
    task {
        let dc = sql.GetDataContext()
        let! count =
            query {
                for customer in dc.Main.Customers do
                where (customer.Country = "USA")
                select customer.CustomerId
            } |> Seq.lengthAsync
        Assert.IsTrue(count >= 0)
    }

[<Test >] // Generates COUNT(DISTINCT CustomerId)
let ``simple select with distinct count async``() =
    async {
        let dc = sql.GetDataContext()
        let! res =
            task {
                let qry = 
                    query {
                        for cust in dc.Main.Customers do
                        where (cust.City <> "Helsinki")
                        distinct
                        select(cust.City, cust.CustomerId)
                    }
                let! leng = qry |> Seq.lengthAsync
                return leng
            } |> Async.AwaitTask
        Assert.AreEqual(90, res)
     } |> Async.RunSynchronously
    ()


type sqlOption = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, CaseSensitivityChange=Common.CaseSensitivityChange.ORIGINAL, UseOptionTypes=FSharp.Data.Sql.Common.NullableColumnType.OPTION, ResolutionPath = resolutionPath, SQLiteLibrary=Common.SQLiteLibrary.SystemDataSQLite>

[<Test>]
let ``simple select query with different return types options``() = 
    let dc = sqlOption.GetDataContext()
    let qry = 
        query {
            for ord in dc.Main.Orders do
            select (ord.Freight, ord.CustomerId, ord.OrderDate, ord.EmployeeId, ord.OrderId, ord.ShipCity.IsSome)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry

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

[<Test>]
let ``simple select with contains query with where not boolean option type``() =
    let dc = sqlOption.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where (not(cust.City.IsNone))
            select cust.CustomerId
            contains "ALFKI"
        }
    Assert.IsTrue(qry)

[<Test>]
let ``simple select with where boolean option types``() =
    let dc = sqlOption.GetDataContext()
    let getOptionFilter (city : string option) = 
        query {
            for c in dc.Main.Customers do
            where (c.City = city)
            select (c.CustomerId)
        } |> Seq.toList

    let nullCase = getOptionFilter None //[City] IS NULL
    let someCase = getOptionFilter (Some "London") //[City] = @param1

    Assert.IsEmpty nullCase
    Assert.IsNotEmpty someCase

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
        } |> Seq.sumAsync |> Async.AwaitTask |> Async.RunSynchronously
    Assert.That(qry, Is.EqualTo(603221955M).Within(10M))

[<Test >]
let ``simple select query with left join``() = 
    let dc = sqlOption.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            join order in (!!) dc.Main.Orders on (cust.CustomerId = order.OrderId.ToString())
            select (cust.CustomerId, order.OrderDate)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry
    Assert.AreEqual(91, qry.Length)

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

[<Test>]
let ``simple math operations query2``() =
    let dc = sql.GetDataContext()
    let qry2 = 
        query {
            for p in dc.Main.Products do
            where (p.UnitPrice <> 30m && ((p.UnitPrice - 30m) = (30m - p.UnitPrice)))
            select p.UnitPrice
        } |> Seq.toList
    CollectionAssert.IsEmpty qry2

[<Test>]
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

[<Test>]
let ``simple canonical operation inverted operations query``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for o in dc.Main.Orders do
            where (500m < o.Freight)
            select o.OrderId
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry
    Assert.AreEqual(13, qry.Length)
    CollectionAssert.Contains(qry, 10514L)

[<Test>]
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
    CollectionAssert.Contains(qry2, "London")

[<Test >]
let ``simple operations in select query``() =
    let dc = sql.GetDataContext()
    //let dc = sql.GetDataContext(SelectOperations.DatabaseSide) // same results, maybe different order

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
let ``simple canonical join query``() =
    let dc = sql.GetDataContext()

    let qry1 = 
        query {
            for cust in dc.Main.Customers do
            join emp in dc.Main.Employees on (cust.City = if emp.City = "" then "" else emp.City)
            sortBy (cust.ContactName)
            select (cust.ContactName)
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry1
    CollectionAssert.Contains(qry1, "Simon Crowther")
    Assert.LessOrEqual(qry1.[0], qry1.[10])

    let qry2 = 
        query {
            for emp in dc.Main.Employees do
            join cust in dc.Main.Customers on ((if emp.City = "" then "" else emp.City) = cust.City)
            sortBy (cust.ContactName)
            select (cust.ContactName)
        } |> Seq.toArray

    CollectionAssert.IsNotEmpty qry2
    CollectionAssert.AreEqual(qry1, qry2)

[<Test>]
let ``simple query yield test``() = 
    let dc = sql.GetDataContext()
    let query1 = 
        query {
            for cus in dc.Main.Customers do
            where (cus.City = "London")
            yield (cus.City + "1")
        }

    let res = query1 |> Seq.toList
    CollectionAssert.IsNotEmpty res
    CollectionAssert.Contains(query1, "London1")

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
    CollectionAssert.Contains(res1, "Portland")
    // Intersect contains 3 values:
    let res2 = query1.Intersect(query2) |> Seq.toArray
    Assert.IsNotEmpty(res2)
    CollectionAssert.Contains(res2, "Seattle")
    // Except contains 2 values:
    let res3 = query2.Except(query1) |> Seq.toArray
    Assert.IsNotEmpty(res3)
    CollectionAssert.Contains(res3, "Redmond")
    

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
    CollectionAssert.Contains(res2, "Sevilla")
    
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
    |> Async.AwaitTask |> Async.RunSynchronously |> ignore
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
let ``simple query sproc result``() = 
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
        |> Async.AwaitTask |> Async.RunSynchronously
    Assert.IsNotNull(pragmaSchemaAsync.ResultSet)

[<Test>]
let ``simple select with subquery exists subquery``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where(query {
                    for cust2 in dc.Main.Customers do
                    exists(cust2.CustomerId = "ALFKI")
                })
            select cust.CustomerId
        } |> Seq.toList
    Assert.IsNotEmpty(qry)
    Assert.AreEqual(91, qry.Count())
    CollectionAssert.Contains(qry, "QUEEN")

[<Test; Ignore("Not supported")>]
let ``simple select with join subqry``() =
    let dc = sql.GetDataContext()
    let subqry =
        query {
            for cust2 in dc.Main.Customers do
            select cust2
        }
    let qry = 
        query {
            for cust in dc.Main.Customers do
            join cust2 in subqry on (cust.CustomerId = cust2.CustomerId)
            select (cust, cust2)
        } |> Seq.toList
    Assert.IsNotEmpty(qry)
    Assert.AreEqual(91, qry.Count())
    CollectionAssert.Contains(qry, "QUEEN")

[<Test>]
let ``simple select with subquery exists parameter from main query``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for o in dc.Main.Orders do
            where(query {
                    for od in dc.Main.OrderDetails do
                    where (od.Quantity > (int16 10))
                    exists(od.OrderId = o.OrderId)
                })
            select o.OrderId
        } |> Seq.toList
    Assert.IsNotEmpty(qry)
    Assert.AreEqual(718, qry.Count())
    CollectionAssert.Contains(qry, 10305L)

[<Test>]
let ``simple select with subquery not exists parameter from main query``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for o in dc.Main.Orders do
            where(not(query {
                    for od in dc.Main.OrderDetails do
                    where (od.Quantity < (int16 10))
                    exists(od.OrderId = o.OrderId)
                }))
            select o.OrderId
        } |> Seq.toList
    Assert.IsNotEmpty(qry)
    Assert.AreEqual(506, qry.Count())
    CollectionAssert.Contains(qry, 10372L)

[<Test; Ignore("Not supported")>]
let ``simple select with subquery in parameter from main query``() =
    let dc = sql.GetDataContext()
    let qry = 
        query {
            for o in dc.Main.Orders do
            where( o.OrderId |=| query {
                    for od in dc.Main.OrderDetails do
                    where (od.Quantity > (int16 10) &&
                           o.Freight > 100m)
                    select (od.OrderId)
                })
            select o.OrderId
        } |> Seq.toList
    Assert.IsNotEmpty(qry)
    Assert.AreEqual(718, qry.Count())


[<Test; Ignore("Not supported nested Group-bys")>]
let ``simple select query with groupBy over groupBy``() = 
    let dc = sql.GetDataContext()

    let qry =
        query {
            for cust in dc.Main.Customers do
            groupBy cust.City into c
            join o in dc.Main.Orders on (c.Key = o.ShipCity)
            groupBy o.ShipName into c2
            select (c2.Key, c2.Count())
        }
    let res = qry |> dict  
    Assert.IsNotEmpty(res)
    Assert.AreEqual(6, res.["London"])

[<Test; Ignore("Not supported. Basically works, but creates 1+N selects, instead of one.")>]
let ``simple select navigation properties``() =
    let dc = sql.GetDataContext(SelectOperations.DatabaseSide)
    let qry = 
        query {
            for cust in dc.Main.Customers do
            take 2
            select (cust.CustomerId,
                        query {
                            for x in cust.``main.Orders by CustomerID`` do
                            where (x.OrderId>10L)
                            select x.OrderId
                        })
        } |> Map.ofSeq
    let alfkiOrders = qry.["ALFKI"] |> Seq.toList
    Assert.Less(alfkiOrders.Length, 10)
    let oid = alfkiOrders |> Seq.head
    Assert.AreEqual(10643L,oid)

[<Test>]
let ``where join order shouldn't matter``() = 
    let dc = sql.GetDataContext()

    let qry1 = 
        query {
            for o in dc.Main.Orders do
            join od in dc.Main.OrderDetails on (o.OrderId=od.OrderId)
            where (o.Freight > 800m)
            select (o.OrderId, od.OrderId)
        } |> Seq.toArray

    let qry2 = 
        query {
            for o in dc.Main.Orders do
            where (o.Freight > 800m)
            join od in dc.Main.OrderDetails on (o.OrderId=od.OrderId)
            select (o.OrderId, od.OrderId)
        } |> Seq.toArray

    let l1 = qry1.Length
    let l2 = qry2.Length
    Assert.AreEqual(l1, l2)
    CollectionAssert.Contains(qry1, (10372L, 10372L))
    CollectionAssert.Contains(qry2, (10372L, 10372L))

[<Test>]
let ``simple select with subquery of subqueries``() =
    let dc = sql.GetDataContext()
    let subquery (subQueryIds:IQueryable<string>) = 
        query {
            for cust in dc.Main.Customers do
            where(subQueryIds.Contains(cust.CustomerId))
            select cust.CustomerId
        }
    let subquery2 = 
        query {
            for custc in dc.Main.Customers do
            where(custc.City |=| (query {
                        for ord in dc.Main.Orders do
                        where(ord.ShipCity ="Helsinki")
                        distinct
                        select ord.ShipCity
                    }))
            select custc.CustomerId //"WILMK"
        }
    let initial1 = ["ALFKI"].AsQueryable()
    let initial2 = ["ANATR"].AsQueryable()
    let initial3 = ["AROUT"].AsQueryable()
    let qry = 
        query {
            for cust in dc.Main.Customers do
            where(
                subquery(subquery(subquery(subquery(initial1)))).Contains(cust.CustomerId) ||
                subquery(subquery(subquery(subquery(initial2)))).Contains(cust.CustomerId) || 
                subquery(initial3).Contains(cust.CustomerId) ||
                subquery2.Contains(cust.CustomerId))
            select cust.CustomerId
        }
    let eval = qry |> Seq.toList
    Assert.IsNotEmpty(eval)
    Assert.AreEqual(4, eval.Length)
    Assert.IsTrue(eval.Contains("ANATR"))
    
type Employee = {
    EmployeeId : int64
    FirstName : string
    LastName : string
    HireDate : DateTime
}
[<Test>]
let ``simple mapTo test``() =
    let dc = sql.GetDataContext()
    let qry =
        query {
            for emp in dc.Main.Employees do
            select emp
            skip 2
            take 5
        } 
        |> Seq.map (fun e -> e.MapTo<Employee>())
        // Optional type-mapping can be done as parameter function, e.g.:
        //|> Seq.map (fun e -> e.MapTo<Employee>(function | "EmployeeId", (:? int64 as id) -> Convert.ToInt32(id) |> box | k,v -> v))
        |> Seq.toList
    qry |> Assert.IsNotEmpty
    qry |> List.exists(fun i -> i.FirstName = "Laura") |> Assert.IsTrue

type sqlValueOption = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, CaseSensitivityChange=Common.CaseSensitivityChange.ORIGINAL, UseOptionTypes=FSharp.Data.Sql.Common.NullableColumnType.VALUE_OPTION, ResolutionPath = resolutionPath, SQLiteLibrary=Common.SQLiteLibrary.SystemDataSQLite>

[<Test >]
let ``simple select query with different return types valueoptions``() = 
    let dc = sqlValueOption.GetDataContext()
    let qry = 
        query {
            for ord in dc.Main.Orders do
            select (ord.Freight, ord.CustomerId, ord.OrderDate, ord.EmployeeId, ord.OrderId, ord.ShipCity.IsSome)
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty qry

[<Test>]
let ``simple select with ValueOption type query``() =
    let dcv = sqlValueOption.GetDataContext()
    let qry = 
        query {
            for cust in dcv.Main.Customers do
            where (cust.City.IsSome && (cust.City.Value.StartsWith "Lo" || cust.City.Value="Berlin"))
            select cust.City
        } |> Seq.toList

    qry |> Assert.IsNotEmpty

[<Test>]
let ``simple select with group-by ValueOption type query``() =
    let dcv = sqlValueOption.GetDataContext()
    let qry = 
        query {
            for cust in dcv.Main.Customers do
            groupBy cust.City.IsSome into g
            select(g.Key, g.Count())
        } |> Seq.toList

    let res = qry |> dict
    let trues = res.[true]

    res |> Assert.IsNotEmpty

[<Test>]
let ``simple select with group-by count distinct type query``() =
    let dcv = sqlValueOption.GetDataContext()
    let qry = 
        query {
            for o in dcv.Main.Orders do
            groupBy o.ShipCity.Value into g
            select(g.Key, g.Select(fun x -> x.Freight.Value).Distinct().Count())
        } |> Seq.toList

    let res = qry |> dict
    let custCount = res.["London"]

    res |> Assert.IsNotEmpty

[<Test>]
let ``valueoption copyOfStruct test``() =
    let dcv = sqlValueOption.GetDataContext()
    let qry = 
        query {
            for cust in dcv.Main.Customers do
            where (cust.City.IsSome)
        } 

    let qtest = qry.Where(fun cust -> cust.PostalCode.IsSome && cust.PostalCode.Value <> "ABC").Select(fun cust -> cust.PostalCode.Value)

    let c = qtest |> Seq.length

    Assert.IsNotNull c

let test_querylogic (customers:IQueryable<sqlValueOption.dataContext.``main.CustomersEntity``>) =
    let itms = 
        query {
            for cust in customers do
            where (cust.City.IsSome && cust.City.Value = "London" && cust.PostalCode.IsSome)
            select (cust.PostalCode.Value)
        }
    itms |> Seq.toList

[<Test>]
let ``mock for unit-testing: database-table``() =

    let realDb = sqlValueOption.GetDataContext()
    let res = test_querylogic (realDb.Main.Customers)
    Assert.IsTrue(res.Length > 2)

    let mockCustomers =
        [| {| City = Some "Paris"; PostalCode = None; |}
           {| City = Some "London"; PostalCode = Some "ABC" |}
           {| City = None; PostalCode = Some "123" |}
        |] |> FSharp.Data.Sql.Common.OfflineTools.CreateMockEntities<sqlValueOption.dataContext.``main.CustomersEntity``> "Customers"
    
    let res = test_querylogic mockCustomers
    Assert.AreEqual(1, res.Length)
    

let test_querylogic_cont (ctx:sqlValueOption.dataContext) =
    let itms = 
        query {
            for o in ctx.Main.Orders do
            join cust in ctx.Main.Customers on (o.CustomerId.Value = cust.CustomerId)
            where (cust.City.IsSome && cust.City.Value = "London" && cust.PostalCode.IsSome)
            select (cust.PostalCode.Value, o.OrderDate.Value)
        }
    itms |> Seq.toList

[<Test>]
let ``mock for unit-testing: datacontext``() =
    
    let sampleDataMap =
        [ "main.Customers",
            // Note: CustomerID, not CustomerId. These are DB-field-names, not nice LINQ names.
            [| {| CustomerID = "1"; City = ValueSome "Paris"; PostalCode = ValueNone; |}
               {| CustomerID = "2"; City = ValueSome "London"; PostalCode = ValueSome "ABC" |}
               {| CustomerID = "3"; City = ValueNone; PostalCode = ValueNone |}
            |] :> obj
          "main.Orders",
            [| {| CustomerID = ValueSome "1"; OrderDate = ValueSome DateTime.Today; |}
               {| CustomerID = ValueSome "2"; OrderDate = ValueSome (DateTime(2021,01,01)); |}
               {| CustomerID = ValueSome "2"; OrderDate = ValueSome (DateTime(2020,06,15)); |}
            |] :> obj

            ] |> Map.ofList
    let mockContext = FSharp.Data.Sql.Common.OfflineTools.CreateMockSqlDataContext sampleDataMap

    let res = test_querylogic_cont mockContext

    let _ = mockContext.Main.Customers.``Create(CompanyName)``("Test create") // shouldn't crash
    mockContext.SubmitUpdates() // do nothing, shouldn't crash

    Assert.AreEqual(2, res.Length)
    ()


// Generated with dc.Main.OrderDetails.TemplateAsRecord
type OrderDetailsRecordVopts = { CustomerId : String voption; EmployeeId : Int64 voption; Freight : Decimal voption; OrderDate : DateTime voption; OrderId : Int64; RequiredDate : DateTime voption; ShipAddress : String voption; ShipCity : String voption; ShipCountry : String voption; ShipName : String voption; ShipPostalCode : String voption; ShipRegion : String voption; ShippedDate : DateTime voption }

type OrderDetailsClassVopts() =
    member val CustomerId = ValueSome "" with get, set
    member val ShipRegion = ValueSome "" with get, set

[<Test >]
let ``simple select query with MapTo with voptions``() = 
    let dc = sqlValueOption.GetDataContext()

    let qry = 
        query {
            for ord in dc.Main.Orders do
            select (ord)
        } |> Seq.head
    let mapped1 = qry.MapTo<OrderDetailsRecordVopts>()
    let mapped2 = qry.MapTo<OrderDetailsClassVopts>()

    Assert.AreEqual(ValueSome "VINET", mapped1.CustomerId)
    Assert.True(mapped1.ShipRegion.IsNone)
    Assert.AreEqual(ValueSome "VINET", mapped2.CustomerId)
    Assert.True(mapped2.ShipRegion.IsNone)

type OrderDetailsRecordOpts = { CustomerId : String option; EmployeeId : Int64 option; Freight : Decimal option; OrderDate : DateTime option; OrderId : Int64; RequiredDate : DateTime option; ShipAddress : String voption; ShipCity : String option; ShipCountry : String option; ShipName : String option; ShipPostalCode : String option; ShipRegion : String option; ShippedDate : DateTime option }

type OrderDetailsClassOpts() =
    member val CustomerId = Some "" with get, set
    member val ShipRegion = Some "" with get, set

[<Test >]
let ``simple select query with MapTo with options``() = 
    let dc = sqlOption.GetDataContext()

    let qry = 
        query {
            for ord in dc.Main.Orders do
            select (ord)
        } |> Seq.head
    let mapped1 = qry.MapTo<OrderDetailsRecordOpts>()
    let mapped2 = qry.MapTo<OrderDetailsClassOpts>()

    Assert.AreEqual(Some "VINET", mapped1.CustomerId)
    Assert.True(mapped1.ShipRegion.IsNone)
    Assert.AreEqual(Some "VINET", mapped2.CustomerId)
    Assert.True(mapped2.ShipRegion.IsNone)
