module CapturedValueQueryTests

// Values a query captures from .NET: record field chains, static members, collections and method calls on them.
// They are evaluated when the query is built and sent as SQL parameters. Each test checks the rows against the same
// filter run in memory, or the exception the query raises, so that how they are evaluated can change safely.

open System
open System.Linq
open FSharp.Data.Sql
open NUnit.Framework

#if NETFRAMEWORK
[<Literal>]
let connectionString =  @"Data Source=" + __SOURCE_DIRECTORY__ + @"/db/northwindEF.db;Version=3;Read Only=false;FailIfMissing=True;"
[<Literal>]
let sqliteLib = Common.SQLiteLibrary.SystemDataSQLite
#else
[<Literal>]
let connectionString =  @"Data Source=" + __SOURCE_DIRECTORY__ + @"/db/northwindEF.db"
[<Literal>]
let sqliteLib = Common.SQLiteLibrary.MicrosoftDataSqlite
#endif

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + "/libs"

type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, CaseSensitivityChange=Common.CaseSensitivityChange.ORIGINAL, ResolutionPath = resolutionPath, SQLiteLibrary=sqliteLib>

type Inner = { B: string }
type Outer = { A: Inner }
type Deep = { D: Outer }
type IdList = { L: string array }
type IdWrap = { W: IdList }
type NullableHolder = { N: Nullable<int64> }
type DateRange = { Start: DateTime }

type Statics =
    static member CustomerId = "ALFKI"
    static member Fail() : string = invalidOp "method failed"

type Throwing() =
    member _.Value: string = invalidOp "getter failed"
    member _.Fail() : string = invalidOp "method failed"

type ThrowingHolder = { T: Throwing }

type MutableHolder() =
    [<DefaultValue>] val mutable Id: string

let private sorted (xs: seq<'T>) = xs |> Seq.sort |> List.ofSeq

let private allCustomerIds (dc: sql.dataContext) =
    query { for c in dc.Main.Customers do select c.CustomerId } |> sorted

let private allOrders (dc: sql.dataContext) =
    query { for o in dc.Main.Orders do select (o.OrderId, o.OrderDate, o.EmployeeId) } |> List.ofSeq

let private orderIdsWhere (dc: sql.dataContext) (keep: DateTime * int64 -> bool) =
    allOrders dc |> List.filter (fun (_, date, employee) -> keep (date, employee)) |> List.map (fun (id, _, _) -> id) |> sorted

[<Test; SqlCheck.ExpectSql("WHERE")>]
let ``captured record field chains are read``() =
    let dc = sql.GetDataContext()
    let r = { A = { B = "ALFKI" } }
    let deep = { D = { A = { B = "ANATR" } } }
    let byChain = query { for c in dc.Main.Customers do where (c.CustomerId = r.A.B); select c } |> Seq.map (fun c -> c.CustomerId) |> sorted
    let byDeepChain = query { for c in dc.Main.Customers do where (c.CustomerId = deep.D.A.B); select c.CustomerId } |> sorted
    Assert.AreEqual([ "ALFKI" ], byChain)
    Assert.AreEqual([ "ANATR" ], byDeepChain)

[<Test; SqlCheck.ExpectSql("WHERE")>]
let ``the same query with other captured values gives their rows``() =
    let dc = sql.GetDataContext()
    let byRecord (r: Outer) = query { for c in dc.Main.Customers do where (c.CustomerId = r.A.B); select c.CustomerId } |> sorted
    Assert.AreEqual([ "ALFKI" ], byRecord { A = { B = "ALFKI" } })
    Assert.AreEqual([ "ANATR" ], byRecord { A = { B = "ANATR" } })
    Assert.AreEqual(List.empty<string>, byRecord { A = { B = "NO SUCH" } })

[<Test; SqlCheck.ExpectSql("WHERE")>]
let ``static properties and fields are read``() =
    let dc = sql.GetDataContext()
    let byStaticProperty = query { for c in dc.Main.Customers do where (c.CustomerId = Statics.CustomerId); select c.CustomerId } |> sorted
    let notEmpty = query { for c in dc.Main.Customers do where (c.CustomerId <> String.Empty); select c.CustomerId } |> sorted
    let beforeMax = query { for o in dc.Main.Orders do where (o.OrderDate < DateTime.MaxValue); select o.OrderId } |> sorted
    Assert.AreEqual([ "ALFKI" ], byStaticProperty)
    Assert.AreEqual(allCustomerIds dc, notEmpty)
    Assert.AreEqual(orderIdsWhere dc (fun (date, _) -> date < DateTime.MaxValue), beforeMax)

[<Test; SqlCheck.ExpectSql("WHERE")>]
let ``a captured class field is read``() =
    let dc = sql.GetDataContext()
    let holder = MutableHolder()
    holder.Id <- "ALFKI"
    let ids = query { for c in dc.Main.Customers do where (c.CustomerId = holder.Id); select c.CustomerId } |> sorted
    Assert.AreEqual([ "ALFKI" ], ids)

[<Test; SqlCheck.NoSql>]
let ``a member of a null owner is not supported``() =
    let dc = sql.GetDataContext()
    let r: Outer = Unchecked.defaultof<_>
    Assert.Throws<Exception>(fun () ->
        query { for c in dc.Main.Customers do where (c.CustomerId = r.A.B); select c.CustomerId } |> Seq.length |> ignore)
    |> ignore

[<Test; SqlCheck.NoSql>]
let ``a getter that throws raises its error``() =
    let dc = sql.GetDataContext()
    let throwing = Throwing()
    Assert.Throws<Reflection.TargetInvocationException>(fun () ->
        query { for c in dc.Main.Customers do where (c.CustomerId = throwing.Value); select c.CustomerId } |> Seq.length |> ignore)
    |> ignore

[<Test; SqlCheck.NoSql>]
let ``a getter that throws further down a chain is not supported``() =
    let dc = sql.GetDataContext()
    let holder = { T = Throwing() }
    Assert.Throws<Exception>(fun () ->
        query { for c in dc.Main.Customers do where (c.CustomerId = holder.T.Value); select c.CustomerId } |> Seq.length |> ignore)
    |> ignore

[<Test; SqlCheck.ExpectSql("WHERE")>]
let ``the value of a captured Nullable is read``() =
    let dc = sql.GetDataContext()
    let holder = { N = Nullable 5L }
    let ids = query { for o in dc.Main.Orders do where (o.EmployeeId = holder.N.Value); select o.OrderId } |> sorted
    Assert.AreEqual(orderIdsWhere dc (fun (_, employee) -> employee = 5L), ids)
    Assert.IsNotEmpty ids

[<Test; SqlCheck.NoSql>]
let ``the value of a captured empty Nullable is not supported``() =
    let dc = sql.GetDataContext()
    let holder = { N = Nullable() }
    Assert.Throws<Exception>(fun () ->
        query { for o in dc.Main.Orders do where (o.EmployeeId = holder.N.Value); select o.OrderId } |> Seq.length |> ignore)
    |> ignore

[<Test; SqlCheck.ExpectSql("IN")>]
let ``IN and NOT IN take captured collections``() =
    let dc = sql.GetDataContext()
    let wrap = { W = { L = [| "ALFKI"; "ANATR" |] } }
    let ids = [| "AROUT"; "BERGS" |]
    let more = [| "BLAUS" |]
    let inChain = query { for c in dc.Main.Customers do where (c.CustomerId |=| wrap.W.L); select c.CustomerId } |> sorted
    let notInChain = query { for c in dc.Main.Customers do where (c.CustomerId |<>| wrap.W.L); select c.CustomerId } |> sorted
    let containsChain = query { for c in dc.Main.Customers do where (wrap.W.L.Contains c.CustomerId); select c.CustomerId } |> sorted
    let containsLocal = query { for c in dc.Main.Customers do where (ids.Contains c.CustomerId); select c.CustomerId } |> sorted
    let inCall = query { for c in dc.Main.Customers do where (c.CustomerId |=| Array.append ids more); select c.CustomerId } |> sorted
    Assert.AreEqual([ "ALFKI"; "ANATR" ], inChain)
    Assert.AreEqual(allCustomerIds dc |> List.except [ "ALFKI"; "ANATR" ], notInChain)
    Assert.AreEqual([ "ALFKI"; "ANATR" ], containsChain)
    Assert.AreEqual([ "AROUT"; "BERGS" ], containsLocal)
    Assert.AreEqual([ "AROUT"; "BERGS"; "BLAUS" ], inCall)

[<Test; SqlCheck.ExpectSql("WHERE")>]
let ``date methods on captured values are evaluated``() =
    let dc = sql.GetDataContext()
    let range = { Start = DateTime(1997, 1, 1) } // a captured DateTime local can't be the target of a call in a quotation
    let lastDay = query { for o in dc.Main.Orders do where (o.OrderDate > DateTime.Now.AddDays(-1.)); select o.OrderId } |> sorted
    let untilTomorrow = query { for o in dc.Main.Orders do where (o.OrderDate < DateTime.Now.AddDays(1.)); select o.OrderId } |> sorted
    let afterMonth = query { for o in dc.Main.Orders do where (o.OrderDate >= range.Start.AddMonths 1); select o.OrderId } |> sorted
    Assert.AreEqual(List.empty<int64>, lastDay)
    Assert.AreEqual(orderIdsWhere dc (fun _ -> true), untilTomorrow)
    Assert.AreEqual(orderIdsWhere dc (fun (date, _) -> date >= DateTime(1997, 2, 1)), afterMonth)

[<Test; SqlCheck.ExpectSql("WHERE")>]
let ``string methods on captured values are evaluated``() =
    let dc = sql.GetDataContext()
    let lower = "alfki"
    let padded = "  ANATR  "
    let longer = "AROUTxyz"
    let first, second = "BER", "GS"
    let byUpper = query { for c in dc.Main.Customers do where (c.CustomerId = lower.ToUpper()); select c.CustomerId } |> sorted
    let byTrim = query { for c in dc.Main.Customers do where (c.CustomerId = padded.Trim()); select c.CustomerId } |> sorted
    let bySubstring = query { for c in dc.Main.Customers do where (c.CustomerId = longer.Substring(0, 5)); select c.CustomerId } |> sorted
    let byConcat = query { for c in dc.Main.Customers do where (c.CustomerId = first + second); select c.CustomerId } |> sorted
    let byFormat = query { for c in dc.Main.Customers do where (c.CustomerId = String.Format("{0}{1}", "BLA", "US")); select c.CustomerId } |> sorted
    Assert.AreEqual([ "ALFKI" ], byUpper)
    Assert.AreEqual([ "ANATR" ], byTrim)
    Assert.AreEqual([ "AROUT" ], bySubstring)
    Assert.AreEqual([ "BERGS" ], byConcat)
    Assert.AreEqual([ "BLAUS" ], byFormat)

[<Test; SqlCheck.NoSql>]
let ``a captured static method that throws is not supported``() =
    let dc = sql.GetDataContext()
    Assert.Throws<Exception>(fun () ->
        query { for c in dc.Main.Customers do where (c.CustomerId = Statics.Fail()); select c.CustomerId } |> Seq.length |> ignore)
    |> ignore

[<Test; SqlCheck.NoSql>]
let ``a captured instance method that throws is not supported``() =
    let dc = sql.GetDataContext()
    let throwing = Throwing()
    Assert.Throws<Exception>(fun () ->
        query { for c in dc.Main.Customers do where (c.CustomerId = throwing.Fail()); select c.CustomerId } |> Seq.length |> ignore)
    |> ignore

[<Test; SqlCheck.ExpectSql("WHERE")>]
let ``captured values in a projection follow each run``() =
    let dc = sql.GetDataContext()
    let withSuffix (suffix: string) =
        query { for c in dc.Main.Customers do where (c.CustomerId = "ALFKI"); select (c.CustomerId, suffix) } |> List.ofSeq
    let withChain (r: Outer) =
        query { for c in dc.Main.Customers do where (c.CustomerId = "ALFKI"); select (c.CustomerId, r.A.B) } |> List.ofSeq
    let withMethod (s: string) =
        query { for c in dc.Main.Customers do where (c.CustomerId = "ALFKI"); select (c.CustomerId + s.ToUpper()) } |> List.ofSeq
    Assert.AreEqual([ ("ALFKI", "x") ], withSuffix "x")
    Assert.AreEqual([ ("ALFKI", "y") ], withSuffix "y")
    Assert.AreEqual([ ("ALFKI", "one") ], withChain { A = { B = "one" } })
    Assert.AreEqual([ ("ALFKI", "two") ], withChain { A = { B = "two" } })
    Assert.AreEqual([ "ALFKI-A" ], withMethod "-a")
    Assert.AreEqual([ "ALFKI-B" ], withMethod "-b")
