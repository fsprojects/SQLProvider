module CrudTests

open System
open System.IO
open FSharp.Data.Sql
open System.Linq
open NUnit.Framework
open System
open System.Transactions

[<Literal>]
let connectionString = @"Data Source=./db/northwindEF.db; Version = 3; Read Only=false; FailIfMissing=True;"


type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, CaseSensitivityChange=Common.CaseSensitivityChange.ORIGINAL>
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %s")

 
let createCustomer (dc:sql.dataContext) = 
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
    newCustomer

[<Test; Ignore>]
let ``Can create and delete an entity``() = 
    let dc = sql.GetDataContext()
    
    let originalCustomers = 
        query { for cust in dc.Main.Customers do
                select cust }
        |> Seq.toList
     
    createCustomer dc |> ignore
    
    dc.SubmitUpdates()

    let newCustomers = 
        query { for cust in dc.Main.Customers do
                select cust  }
        |> Seq.toList
    
    let created = 
        newCustomers |> List.find (fun x -> x.CustomerId = "SQLPROVIDER")
    
    Assert.AreEqual("Phone Number", created.Phone)
    created.Delete()
    dc.SubmitUpdates()
    Assert.AreEqual(originalCustomers.Length, newCustomers.Length - 1)

[<Test; Ignore>]
let ``Can enlist in a transaction scope and rollback changes without complete``() =
    let dc = sql.GetDataContext()

    let ts = new TransactionScope()
    createCustomer dc |> ignore
    dc.SubmitUpdates()
    ts.Dispose()

    let created = 
        query { for cust in dc.Main.Customers do
                where (cust.CustomerId = "SQLPROVIDER")
                select cust  
        }
        |> Seq.toList

    Assert.AreEqual([], created)
