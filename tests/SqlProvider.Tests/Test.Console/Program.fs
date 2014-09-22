// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.

open System
open FSharp.Data.Sql


FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %s")

let processId = System.Diagnostics.Process.GetCurrentProcess().Id;

type HR = SqlDataProvider<Common.DatabaseProviderTypes.ORACLE, ConnectionStringName = "HR", Owner = "HR_TEST">
let ctx = HR.GetDataContext()

type Emp = {
    Id : decimal
    Name : string
}

[<EntryPoint>]
let main argv =
    
    let query = 
        query {
            for emp in ctx.``[HR_TEST].[EMPLOYEES]`` do
            select (emp.EMPLOYEE_ID, emp.FIRST_NAME)
        }

    Seq.iter (printfn "%A") query
    
    System.Console.ReadLine() |> ignore

    printfn "%A" argv
    0 // return an integer exit code
