module CrudTests

open System
open System.IO
open FSharp.Data.Sql
open System.Linq
open NUnit.Framework
open System
open System.Transactions

[<Literal>]
let connectionString = @"Data Source=./db/northwindEF.db;Version=3;Read Only=false;FailIfMissing=True;"


type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, CaseSensitivityChange=Common.CaseSensitivityChange.ORIGINAL>
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %O")

 
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

[<Test>]
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

[<Test>]
let ``Can create, update and delete an entity``() = 
    let dc = sql.GetDataContext()
    
    let originalCustomers = 
        query { for cust in dc.Main.Customers do
                select cust }
        |> Seq.toList
     
    let ent = createCustomer dc

    dc.SubmitUpdates()    
    dc.SubmitUpdates() // run twice just to test extra won't hurt

    ent.SetColumn("Phone", "Updated Number")

    dc.SubmitUpdates()    
    dc.SubmitUpdates()

    // let's create new context just to test that it is actually there.
    let dc2 = sql.GetDataContext()
 
    let newCustomers = 
        query { for cust in dc2.Main.Customers do
                select cust  }
        |> Seq.toList
    
    let created = 
        newCustomers |> List.find (fun x -> x.CustomerId = "SQLPROVIDER")

    Assert.AreEqual(originalCustomers.Length, newCustomers.Length - 1)

    Assert.AreEqual("Updated Number", created.Phone)
    created.Delete()
    dc2.SubmitUpdates()    
    dc2.SubmitUpdates()

    let reallyDeleted = 
        query { for cust in dc2.Main.Customers do
                select cust  }
        |> Seq.toList

    Assert.AreEqual(originalCustomers.Length, reallyDeleted.Length)

[<Test>]
let ``Can persist a blob``() = 
    let dc = sql.GetDataContext()
    
    let imageBytes = [| 0uy .. 100uy |]

    let savedEntity = dc.Main.Pictures.``Create(Image)`` imageBytes
    savedEntity.Id <- 1234
    dc.SubmitUpdates()

    let reloadedEntity =
        query { for image in dc.Main.Pictures do
                where (image.Id = savedEntity.Id)
                head }

    Assert.That( reloadedEntity.Image, Is.EqualTo(imageBytes))

[<Test; Ignore("Something wrong with SQLite transactions...")>]
let ``Can enlist in a transaction scope and rollback changes without complete``() =

    try
        let dc = sql.GetDataContext()
        use ts = new TransactionScope()
        createCustomer dc |> ignore
        dc.SubmitUpdates()
        ts.Dispose()
    finally
        GC.Collect()
    let dc2 = sql.GetDataContext()

    let created = 
        query { for cust in dc2.Main.Customers do
                where (cust.CustomerId = "SQLPROVIDER")
                select cust  
        }
        |> Seq.toList

    Assert.AreEqual([], created)
