module MsDataSqliteTransactions

open FSharp.Data.Sql
open NUnit.Framework

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + "/libs"

[<Literal>]
let connectionString =  @"Data Source=" + __SOURCE_DIRECTORY__ + @"/db/northwindEF.db;foreign keys=true"

type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, CaseSensitivityChange=Common.CaseSensitivityChange.ORIGINAL, ResolutionPath = resolutionPath, SQLiteLibrary=Common.SQLiteLibrary.MicrosoftDataSqlite>
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
let ``If Error during transactions, database should rollback to the initial state``() = 
    let dc = sql.GetDataContext()
    
    let originalCustomers = 
        query { for cust in dc.Main.Customers do
                select cust }
        |> Seq.toList

    try     
        createCustomer dc |> ignore
        createCustomer dc |> ignore
        dc.SubmitUpdates()
    with
    | ex when ex.Message.Contains("UNIQUE constraint failed") -> 
        ()

    let newCustomers = 
        query { for cust in dc.Main.Customers do
                select cust  }
        |> Seq.toList
    
    // Clean up
    dc.ClearUpdates() |> ignore    
    let createdOpt = newCustomers |> List.tryFind (fun x -> x.CustomerId = "SQLPROVIDER")
    if createdOpt.IsSome then 
        createdOpt.Value.Delete() 
        dc.SubmitUpdates()
    
    Assert.AreEqual(originalCustomers.Length, newCustomers.Length)

[<Test>]
let ``If Error during transactions, database should rollback to the initial state (Async version)``() = 
    let dc = sql.GetDataContext()
    
    let originalCustomers = 
        query { for cust in dc.Main.Customers do
                select cust }
        |> Seq.toList

    try     
        createCustomer dc |> ignore
        createCustomer dc |> ignore
        dc.SubmitUpdatesAsync() |> Async.AwaitTask |> Async.RunSynchronously
    with
    | :? System.AggregateException as ex ->
        if ex.GetBaseException().Message.Contains("UNIQUE constraint failed") |> not then 
            raise ex

    let newCustomers = 
        query { for cust in dc.Main.Customers do
                select cust  }
        |> Seq.toList
    
    // Clean up
    dc.ClearUpdates() |> ignore    
    let createdOpt = newCustomers |> List.tryFind (fun x -> x.CustomerId = "SQLPROVIDER")
    if createdOpt.IsSome then 
        createdOpt.Value.Delete() 
        dc.SubmitUpdates()
    
    Assert.AreEqual(originalCustomers.Length, newCustomers.Length)
    