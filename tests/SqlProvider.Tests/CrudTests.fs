module CrudTests

open System
open System.IO
open FSharp.Data.Sql
open System.Linq
open NUnit.Framework

[<Literal>]
let connectionString = @"Data Source=.\db\northwindEF.db; Version = 3;FailIfMissing=True;"

type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString>
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %s")
 
[<Test>]
let ``Can create an entity``() = 
    let dc = sql.GetDataContext()
    
    let originalCustomers = 
        query { for cust in dc.Main.Customers do
                select cust }
        |> Seq.toList
     
    let newCustomer = dc.Main.Customers.Create()
    newCustomer.CustomerId <- "SQLPROVIDER"
    newCustomer.Address <- "FsPRojects"
    newCustomer.City <- "Fsharp"
    newCustomer.CompanyName <- "FSProjects"
    newCustomer.ContactName <- "A DB"
    newCustomer.ContactTitle <- "Mr"
    newCustomer.Country <- "England"
    newCustomer.Fax <- "Fax Number"
    newCustomer.Phone <- "Phone Number"
    newCustomer.PostalCode <- "PostCode"
    newCustomer.Region <- "London"
    
    dc.SubmitUpdates()

    let newCustomers = 
        query { for cust in dc.Main.Customers do
                select cust  }
        |> Seq.toList
    
    let created = 
        newCustomers |> List.find (fun x -> x.CustomerId = "SQLPROVIDER")

    created.Delete()
    dc.SubmitUpdates()
    Assert.AreEqual(originalCustomers.Length, newCustomers.Length - 1)