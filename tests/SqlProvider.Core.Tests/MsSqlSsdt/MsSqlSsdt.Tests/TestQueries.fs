module TestQueries
open FSharp.Data.Sql
open FSharp.Data.Sql.Schema

[<Literal>]
let AdventureWorks = @"Server=localhost\SQLEXPRESS;Database=AdventureWorksLT2019;Trusted_Connection=True;"

[<Literal>]
let SsdtPath = __SOURCE_DIRECTORY__ + "/AdventureWorks_SSDT/bin/Debug/AdventureWorks_SSDT.dacpac"

type DB = SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER_SSDT, SsdtPath = SsdtPath>

let ctx = DB.GetDataContext(AdventureWorks)

let orderDetails =
    query {
        for o in ctx.SalesLt.SalesOrderHeader do
        for d in o.``SalesLT.SalesOrderDetail by SalesOrderID`` do
        select (o.SalesOrderId, o.OrderDate, o.SubTotal, d.OrderQty, d.ProductId, d.LineTotal)
    }

let dottedTable =
    query {
        // [SalesLT].[Test.Table]
        for tt in ctx.SalesLt.TestTable do
        select (tt.Id, tt.Description)
    }

let storedProc1 =
    let r = ctx.Procedures.UspLogError.Invoke(123)
    r.ColumnValues

let storedProc2 =
    let r = ctx.Procedures.UspPrintError.Invoke()
    r.ResultSet |> Array.map (fun i -> i.ColumnValues |> Map.ofSeq)
    
