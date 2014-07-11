#r @"..\..\bin\FSharp.Data.SqlProvider.dll"

open System
open FSharp.Data.Sql

[<Literal>]
let connStr = "User ID=postgres;Password=admin;Host=localhost;Port=5432;Database=dvdrental;"

[<Literal>]
let resolutionFolder =  @"D:\Downloads\Npgsql-2.1.3-net20"
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %s")

let processId = System.Diagnostics.Process.GetCurrentProcess().Id;

type DVD = SqlDataProvider<ConnectionString = connStr, DatabaseVendor = Common.DatabaseProviderTypes.POSTGRESQL, ResolutionPath = resolutionFolder>
let ctx = DVD.GetDataContext()

let films = 
    query {
        for film in ctx.``[public].[film]`` do
        select film.description 
    } |> Seq.toList