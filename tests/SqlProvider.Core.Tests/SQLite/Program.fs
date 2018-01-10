#if INTERACTIVE
#r @"C:\Program Files\dotnet\sdk\2.0.0\Microsoft\Microsoft.NET.Build.Extensions\net461\lib\netstandard.dll"
#r "../../../packages/standard/Microsoft.Data.Sqlite.Core/lib/netstandard2.0/Microsoft.Data.Sqlite.dll"
#r "../../../bin/netstandard2.0/FSharp.Data.SqlProvider.dll"
// On Non-Windows-machine, fsharpi is not running on Core:
// #r "../../../bin/net451/FSharp.Data.SqlProvider.dll"
[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + "/temp"

#else
module NetstandardTest

[<Literal>]
let resolutionPath = "temp"

#endif

open System
open FSharp.Data.Sql

[<Literal>] // Relative to resolutionPath:
let connStr = @"Filename=" + @"./../../../SqlProvider.Tests/db/northwindEF.db"

// In case of dependency error, note that SQLite dependencies are processor architecture dependant
type HR = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connStr, ResolutionPath = resolutionPath, SQLiteLibrary=Common.SQLiteLibrary.MicrosoftDataSqlite>

[<EntryPoint>]
let main argv =
    let runtimeConnectionString = connStr
    let ctx = HR.GetDataContext runtimeConnectionString
    let employeesFirstName = 
        query {
            for emp in ctx.Main.Employees do
            select emp.FirstName
        } |> Seq.head

    printfn "Hello %s!" employeesFirstName
    System.Threading.Thread.Sleep 2000
    0 // return an integer exit code
