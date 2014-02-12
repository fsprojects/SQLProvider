#r @"C:\code_root\SQLProvider\bin\FSharp.Data.SQLProvider.dll"
open System
open System.Linq
open FSharp.Data.Sql
open FSharp.Data.Sql.Common.QueryEvents
type mdb = SqlDataProvider< "Provider=Microsoft.ACE.OLEDB.12.0; Data Source= C:\\code_root\\SQLProvider\\docs\\files\\msaccess\\Northwind.mdb", Common.DatabaseProviderTypes.MSACCESS, @"" , 100, true >
let ctx = mdb.GetDataContext()


SqlQueryEvent.Add (printfn "%s")

let mattisOrderDetails =
    query { for c in ctx.``[Northwind].[Customers]`` do
            // you can directly enumerate relationships with no join information
            for o in c.FK_Orders_Customers do
            // or you can explicitly join on the fields you choose
            join od in ctx.``[Northwind].[Order Details]`` on (o.OrderID = od.OrderID.Value)
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