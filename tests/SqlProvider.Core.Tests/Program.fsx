// Learn more about F# at http://fsharp.org
//#r "../../bin/net461/FSharp.Data.SqlProvider.dll"
#r "../../bin/netcoreapp2.0/FSharp.Data.SqlProvider.dll"

open System
open FSharp.Data.Sql

type HR = SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER, "Data Source=localhost; Initial Catalog=HR; Integrated Security=True">

[<EntryPoint>]
let main argv =
    let ctx = HR.GetDataContext()
    let employeesFirstName = 
        query {
            for emp in ctx.Dbo.Employees do
            select emp.FirstName
        } |> Seq.head

    printfn "Hello %s!" employeesFirstName
    0 // return an integer exit code
