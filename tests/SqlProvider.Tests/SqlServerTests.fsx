#r @"..\..\bin\FSharp.Data.SqlProvider.dll"

open System
open FSharp.Data.Sql

[<Literal>]
let connStr = "Data Source=.;Initial Catalog=AdventureWorks;Integrated Security=True"

FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %s")

let processId = System.Diagnostics.Process.GetCurrentProcess().Id;

type AdventureWorks = SqlDataProvider<ConnectionString = connStr, DatabaseVendor = Common.DatabaseProviderTypes.MSSQLSERVER>
let ctx = AdventureWorks.GetDataContext()

let employees = 
    query {
        for emp in ctx.``[HumanResources].[Employee]`` do
        select emp.JobTitle
    } |> Seq.toList

ctx.Procedures.uspGetManagerEmployees(12)

ctx.Functions.ufnGetAccountingEndDate().ReturnValue