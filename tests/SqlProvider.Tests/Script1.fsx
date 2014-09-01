#r @"..\..\bin\FSharp.Data.SqlProvider.dll"

open System
open FSharp.Data.Sql

[<Literal>]
let connStr = "Data Source=SQLSERVER;Initial Catalog=HR;User Id=sa;Password=password"
[<Literal>]
let resolutionFolder = __SOURCE_DIRECTORY__
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %s")

let processId = System.Diagnostics.Process.GetCurrentProcess().Id;

type HR = SqlDataProvider<ConnectionString = connStr, DatabaseVendor = Common.DatabaseProviderTypes.MSSQLSERVER, ResolutionPath = resolutionFolder>
let ctx = HR.GetDataContext()

let foo = 
    [
        for r in ctx.Procedures._GET_EMPLOYEES().Results.ResultSet do
            yield r.ColumnValues
    ]