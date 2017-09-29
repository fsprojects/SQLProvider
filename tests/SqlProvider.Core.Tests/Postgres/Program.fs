// Learn more about F# at http://fsharp.org
//#r "../../bin/net461/FSharp.Data.SqlProvider.dll"
#if INTERACTIVE
#r @"C:\Program Files\dotnet\sdk\2.0.0\Microsoft\Microsoft.NET.Build.Extensions\net461\lib\netstandard.dll"
#r @"C:\Program Files\dotnet\sdk\2.0.0\Microsoft\Microsoft.NET.Build.Extensions\net461\lib\System.Reflection.dll"
#r @"C:\Program Files\dotnet\sdk\2.0.0\Microsoft\Microsoft.NET.Build.Extensions\net461\lib\System.Runtime.dll"
#r @"C:\Program Files\dotnet\sdk\2.0.0\Microsoft\Microsoft.NET.Build.Extensions\net461\lib\System.Console.dll"
#r @"C:\Program Files\dotnet\shared\Microsoft.NETCore.App\2.0.0\System.Runtime.Loader.dll"
#r "../../../bin/netstandard2.0/FSharp.Data.SqlProvider.dll"
#else
module Netstandard
#endif

open System
open FSharp.Data.Sql

type HR = SqlDataProvider<
            Common.DatabaseProviderTypes.POSTGRESQL, 
            "User ID=postgres;Host=localhost;Port=5432;Database=sqlprovider;Password=postgres",
            ResolutionPath = "temp">

[<EntryPoint>]
let main argv =
    let ctx = HR.GetDataContext()
    let employeesFirstName = 
        query {
            for emp in ctx.Public.Employees do
            select emp.FirstName
        } |> Seq.head

    printfn "Hello %s!" employeesFirstName
    System.Threading.Thread.Sleep 2000
    0 // return an integer exit code
