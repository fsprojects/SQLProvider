
//#r "../../../bin/lib/net6.0/FSharp.Data.SqlProvider.dll"
//#r "nuget: Microsoft.Data.SqlClient"
//#r "nuget: BenchmarkDotNet"

open System

open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Running
open BenchmarkDotNet.Jobs

open System
open FSharp.Data.Sql

[<Literal>]
let connStr = @"Data Source=localhost;Initial Catalog=HR;Integrated Security=True;TrustServerCertificate=True"

[<Literal>]
let ssdtpath = __SOURCE_DIRECTORY__ +  "/HR.dacpac"

type HR = SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER_SSDT, connStr,
                SsdtPath = ssdtpath,
                UseOptionTypes=FSharp.Data.Sql.Common.NullableColumnType.VALUE_OPTION>

[<SimpleJob (RuntimeMoniker.Net80)>] [<MemoryDiagnoser(true)>]
type Benchmarks() =
    [<Params(25, 250, 2500)>]
    member val rowsReturned = 0 with get, set

    [<Benchmark(Baseline = true)>]
    member this.FirstNamesToList () =
        let max = this.rowsReturned
        let ctx = HR.GetDataContext()
        let res =
            query {
                for emp in ctx.Dbo.Employees do
                where emp.FirstName.IsSome
                take max
                select (emp.FirstName, emp.LastName, emp.Email)
            } |> Seq.toList
        res

    [<Benchmark>]
    member this.FirstNamesToListAsync () =
        let max = this.rowsReturned
        let ctx = HR.GetDataContext()
        let res =
            query {
                for emp in ctx.Dbo.Employees do
                where emp.FirstName.IsSome
                take max
                select (emp.FirstName, emp.LastName, emp.Email)
            } |> List.executeQueryAsync |> Async.AwaitTask |> Async.RunSynchronously
        res |> Seq.toList

BenchmarkRunner.Run<Benchmarks>() |> ignore

//System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription;

(*
// You can use this to debug
[<EntryPoint>]
let main args =
    printfn "Arguments passed to function : %A" args
    let b = Benchmarks()
    b.rowsReturned <- 2500
    for i in 1 .. 1000 do
        printfn "Running test"
        let r = b.FirstNamesToList()
        printfn "Result %O" r.Head
        ()
    0
*)
