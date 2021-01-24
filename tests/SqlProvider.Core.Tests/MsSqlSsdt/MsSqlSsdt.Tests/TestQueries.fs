module TestQueries
open FSharp.Data.Sql

[<Literal>]
let SsdtPath = __SOURCE_DIRECTORY__ + "/AdventureWorks_SSDT/bin/Debug/AdventureWorks_SSDT.dacpac"

type DB = SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER_SSDT, SsdtPath = SsdtPath>

let ctx = DB.GetDataContext()

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
