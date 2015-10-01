#r @"../../../bin/FSharp.Data.SqlProvider.dll"
open System
open System.Linq
open FSharp.Data.Sql
open FSharp.Data.Sql.Common.QueryEvents

// Install this:
// http://www.microsoft.com/download/en/confirmation.aspx?id=23734

[<Literal>]
let connectionString = @"Provider=Microsoft.ACE.OLEDB.12.0; Data Source= " + __SOURCE_DIRECTORY__ + @"\..\..\..\docs\files\msaccess\Northwind.mdb"

type mdb = SqlDataProvider<Common.DatabaseProviderTypes.MSACCESS, connectionString, @"" , @"", 100, true>
let ctx = mdb.GetDataContext()

SqlQueryEvent.Add (printfn "%s")

let mattisOrderDetails =
    query { for c in ctx.Northwind.Customers do
            // you can directly enumerate relationships with no join information
            for o in c.FK_Orders_Customers do
            // or you can explicitly join on the fields you choose
            join od in ctx.Northwind.OrderDetails on (o.OrderId = od.OrderId.Value)
            //  the (!!) operator will perform an outer join on a relationship
            for prod in (!!) od.``FK_Order Details_Products`` do 
            // nullable columns can be represented as option types. The following generates IS NOT NULL
            where c.ContactName.IsSome          
            // standard operators will work as expected; the following shows the like operator and IN operator
            where (c.ContactName =% ("Matti%") && o.ShipCountry.Value |=| [|"Finland";"England"|] )
            sortBy o.ShipName
            // arbitrarily complex projections are supported
            select (c.ContactName,o.ShipAddress,o.ShipCountry,prod.ProductName,prod.UnitPrice) } 
    |> Seq.toArray

let stuf =
    query { for c in ctx.Northwind.Customers do
            join qo in ctx.Northwind.QuarterlyOrders on (c.CustomerId = qo.CustomerId)
            where (c.City =% ("Lon%"))
            select (c.ContactName.Value, qo.CompanyName, qo.Country.Value)
    } |> Seq.toArray
