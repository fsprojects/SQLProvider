#r @"../../../bin/FSharp.Data.SqlProvider.dll"

open System
open FSharp.Data.Sql

[<Literal>]
let connectionString = @"Data Source=localhost; port=3051;initial catalog=" + __SOURCE_DIRECTORY__ + @"\northwindfbd3.fdb;user id=SYSDBA;password=masterkey;Dialect=3"

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + "/../../../packages/scripts/FirebirdSql.Data.FirebirdClient/lib/net452"


open FSharp.Data.Sql


type sql = SqlDataProvider<Common.DatabaseProviderTypes.FIREBIRD, connectionString, ResolutionPath = resolutionPath>
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %s")

let ctx = sql.GetDataContext()

let customers = ctx.Dbo.Customers |> Seq.toArray

let firstCustomer = customers.[0]
let name = firstCustomer.Contactname

let orders = firstCustomer.``Dbo.ORDERS by CUSTOMERID`` |> Seq.toArray

type Simple = {First : string}

type Dummy<'t> = D of 't

let employeesFirstName1 =
    query {
        for emp in ctx.Dbo.Customers do
        select (D {First=emp.Contactname})
    } |> Seq.toList

let employeesFirstName2 =
    query {
        for emp in ctx.Dbo.Customers do
        select ({First=emp.Contactname} |> D)
    } |> Seq.toList

let customersQuery =
    query { for customer in ctx.Dbo.Customers do
            select customer } |> Seq.toArray


let customersQuerySortedByName =
    query { for customer in ctx.Dbo.Customers do
            sortBy customer.Companyname
            thenBy customer.Contactname
            select (customer.Companyname, customer.Contactname) } |> Seq.toArray

let filteredQuery =
    query { for customer in ctx.Dbo.Customers do
            where (customer.Contactname = "John Smith")
            select customer } |> Seq.toArray

let multipleFilteredQuery =
    query { for customer in ctx.Dbo.Customers do
            where ((customer.Contactname = "John Smith" && customer.Country = "England")
                    || customer.Contactname = "Joe Bloggs")
            select customer } |> Seq.toArray

let automaticJoinQuery =
   query { for customer in ctx.Dbo.Customers do
           for order in customer.``Dbo.ORDERS by CUSTOMERID`` do
           where (customer.Contactname = "John Steel")
           select (customer,order) } |> Seq.toArray

let explicitJoinQuery =
   query { for customer in ctx.Dbo.Customers do
           join orderr in ctx.Dbo.Orders on (customer.CustomerId = orderr.Customerid)
           where (customer.Contactname = "John Steel")
           select (customer,orderr) } |> Seq.toArray

let ordersQuery =
   query { for customer in ctx.Dbo.Customers do
           for order in customer.``Dbo.ORDERS by CUSTOMERID`` do
           where (customer.Contactname = "John Steel")
           select (customer.Contactname, order.OrderDate, order.Shipaddress) } |> Seq.toArray

let BERGS = ctx.Dbo.Customers.Individuals.BERGS


let christina = ctx.Dbo.Customers.Individuals.``As Contactname``.``BERGS, Christina Berglund``.Address

// TypeMapper Example

type Employee = {
    EmployeeId : int
    FirstName : string
    LastName : string
    HireDate : DateTime
}

let mapEmployee (dbRecord:sql.dataContext.``Dbo.EMPLOYEESEntity``) : Employee =
    { EmployeeId = dbRecord.Employeeid
      FirstName = dbRecord.Firstname
      LastName = dbRecord.Lastname
      HireDate = dbRecord.HireDate }

// Composable Query Example

// Notice that there is only one query with two filters created here:
// SELECT [customers].[CompanyName] as 'CompanyName' FROM main.Customers as [customers] WHERE (([customers].[ContactTitle]= @param1) AND ([customers].[CompanyName]= @param2)) - params @param1 - "Marketing Manager"; @param2 - "The Big Cheese"; 

let query1 =
    query {
      for customers in ctx.Dbo.Customers do
      where (customers.Contacttitle = "Marketing Manager")
      select customers} 

let query2 =
    query {
      for customers in query1 do
      where (customers.Companyname = "The Big Cheese")
      select customers.Companyname}
    |> Seq.toArray


type sqlOpt = SqlDataProvider<Common.DatabaseProviderTypes.FIREBIRD, connectionString, ResolutionPath = resolutionPath, CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL, UseOptionTypes=true>
let ctxOpt = sqlOpt.GetDataContext()
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %s")
let ``none option in left join`` =
        query { 
            for od in ctxOpt.Dbo.Orderdetails do
            //  the (!!) operator will perform an outer join on a relationship
            for prod in (!!) od.``Dbo.PRODUCTS by PRODUCTID`` do 
            // standard operators will work as expected; the following shows the like operator and IN operator
            select (prod.DiscontinuedDate)
            // arbitrarily complex projections are supported
        } |> Seq.toList |> List.head
