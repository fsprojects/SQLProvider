#I @"../../../bin/net451"
#r @"../../../bin/net451/FSharp.Data.SqlProvider.dll"

open System
open FSharp.Data.Sql
open FSharp.Data.Sql.Transactions

// ----------------------------------------------------------------
// Excel connection.
// First create some ie_data.xls
[<Literal>]
let connectionString = @"Driver={Microsoft Excel Driver (*.xls)};DriverId=790;Dbq=" + __SOURCE_DIRECTORY__ + @"\..\..\..\docs\files\ie_data.xls;DefaultDir=" + __SOURCE_DIRECTORY__ + @"\..\..\..\docs\files\;"

type database = SqlDataProvider<Common.DatabaseProviderTypes.ODBC, connectionString, Owner="">
let odbc = database.GetDataContext()

let contributors =
    query {
        for rows in odbc.Dbo.Sheet1 do
        select (rows.Name, rows.Commits)
    } |> Seq.toArray
//val it : (string * float) [] = [|("colinbull", 160.0); ("pezipink", 95.0); ("Thorium", 61.0); ("forki", 56.0); ("janno-p", 31.0)|]


// ----------------------------------------------------------------
// Access connection.
[<Literal>]
let connectionStringAccess = @"Driver={Microsoft Access Driver (*.mdb, *.accdb)};Dbq=" + __SOURCE_DIRECTORY__ + @"\..\..\..\docs\files\msaccess\Northwind.mdb"

type dbOdbcAccess = SqlDataProvider<Common.DatabaseProviderTypes.ODBC, connectionStringAccess, Owner="", UseOptionTypes = true, OdbcQuote=Common.OdbcQuoteCharacter.DOUBLE_QUOTES>

// Sadly MS-Access ODBC driver doesn't support DTC at all.
let odbcaContext = 
    dbOdbcAccess.GetDataContext(
        {Timeout = TimeSpan.MaxValue; IsolationLevel = Transactions.IsolationLevel.DontCreateTransaction})

FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent.Add (printfn "%O")

/// Normal query
let mattisOrderDetails =
    query { for c in odbcaContext.Dbo.Customers do
            // you can directly enumerate relationships with no join information
            //for o in ctx.Northwind.Customers.FK_Orders_Customers do
            // or you can explicitly join on the fields you choose
            join od in odbcaContext.Dbo.Orders on (c.CustomerId = od.CustomerId)
            //  the (!!) operator will perform an outer join on a relationship
            //for prod in (!!) od.``FK_Order Details_Products`` do 
            // nullable columns can be represented as option types. The following generates IS NOT NULL
            where c.Country.IsSome
            // standard operators will work as expected; the following shows the like operator and IN operator
            where (c.ContactName.Value =% ("Matti%") && od.ShipCountry.Value |=| [|"Finland";"England"|] )
            sortBy od.ShippedDate.Value
            // arbitrarily complex projections are supported
            select (c.ContactName, od.ShipAddress, od.ShipCountry, od.ShipName, od.ShippedDate.Value.Date) } 
    |> Seq.toArray

// Note that Orders and ShipName are having Description-field in Access, so they are displayed as XML-tooltips.

/// Query with space in table name
let orderDetail =
    query { 
        for c in odbcaContext.Dbo.OrderDetails do
        select (c)
        head
        }
//orderDetail.Discount <- 0.5f
//orderDetail.Delete()
//ctx.SubmitUpdates()


/// CRUD Test. To use CRUD you have to have a primary key in your table. 
let crudops =
    let neworder = odbcaContext.Dbo.Customers.``Create()``()
    neworder.CompanyName <- Some "FSharp.org"
    neworder.CustomerId <- Some "MyId"
    neworder.City <- Some "London"
    odbcaContext.SubmitUpdates()
    let fetched =
        query { 
            for c in odbcaContext.Dbo.Customers do
            where (c.CustomerId = Some "MyId")
            headOrDefault }
    fetched.Delete()
    odbcaContext.SubmitUpdates()

open System.Linq
let asyncContainsQuery =
    let contacts = ["Matti Karttunen"; "Maria Anders"]
    let r =
        async {
            let! res =
                query { 
                    for c in odbcaContext.Dbo.Customers do
                    where (contacts.Contains(c.ContactName.Value))
                    select (c.CustomerId.Value, c.ContactName.Value)
                }|> Seq.executeQueryAsync
            return res |> Seq.toArray
        } |> Async.StartAsTask
    r.Wait()
    r.Result

let canoncicalOpTest = 
    query {
        // Access doesn't support many ODBC functions.
        // Silly query not hitting indexes, so testing purposes only...
        for cust in odbcaContext.Dbo.Customers do
        join emp in odbcaContext.Dbo.Employees on (cust.City.Value.Trim() = emp.City.Value.Trim())
        where (
            abs(emp.EmployeeId)+1 > 4
            && emp.BirthDate.Value.Month + 1 > 3
            && emp.HireDate.Value.Subtract(emp.HireDate.Value).Days = 0
        )
        sortBy emp.BirthDate.Value.Year
        select (cust.CustomerId, cust.City, emp.BirthDate)
    } |> Seq.toArray


// ----------------------------------------------------------------
// SQL Server connection
// Database: http://pastebin.com/5W3PPVaD

// Configuring DSN on Windows ODBC Data Source Administrator server: 
// Control Panel -> Administrative Tools -> Data Sources (ODBC) 
// (Launch: c:\windows\syswow64\odbcad32.exe) 
// And add your driver to DSN.

open FSharp.Data.Sql 
[<Literal>] 
let dnsConn = @"DSN=foo" 
type db = SqlDataProvider<Common.DatabaseProviderTypes.ODBC, dnsConn>
let ctx = db.GetDataContext()

let entity = ctx.Dbo.Student.Create [| "StudentId", 1 |> box ; "Name", "Michael" |> box |]
ctx.SubmitUpdates()

let student =
    query {
        for c in ctx.Dbo.Student do
        select (c)
        head
    }

student.Name // val it : string = "Michael"

