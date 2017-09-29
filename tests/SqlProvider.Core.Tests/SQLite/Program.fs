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

[<Literal>]
let connStr = "Data Source=./../../SqlProvider.Tests/db/northwindEF.db;Version=3;Read Only=false;FailIfMissing=True;"

type HR = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connStr>

[<EntryPoint>]
let main argv =
    let ctx = HR.GetDataContext connStr
    let employeesFirstName = 
        query {
            for emp in ctx.Main.Employees do
            select emp.FirstName
        } |> Seq.head

    printfn "Hello %s!" employeesFirstName
    System.Threading.Thread.Sleep 2000
    0 // return an integer exit code
