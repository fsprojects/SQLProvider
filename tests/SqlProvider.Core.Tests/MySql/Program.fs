#if INTERACTIVE
#r @"C:\Program Files\dotnet\sdk\2.0.0\Microsoft\Microsoft.NET.Build.Extensions\net461\lib\netstandard.dll"
#r "../../../bin/netstandard2.0/FSharp.Data.SqlProvider.dll"
// On Non-Windows-machine, fsharpi is not running on Core:
// #r "../../../bin/net451/FSharp.Data.SqlProvider.dll"

//[<Literal>]
//let msyqlDataPath = __SOURCE_DIRECTORY__ + "/dataTemp"
[<Literal>]
let msyqlConnectorPath = __SOURCE_DIRECTORY__ + "/connectorTemp"

#else
module NetstandardTest

//[<Literal>]
//let msyqlDataPath = "dataTemp"
[<Literal>]
let msyqlConnectorPath = "connectorTemp"
#endif

open System
open FSharp.Data.Sql

[<Literal>]
let connStr =  "Server=localhost;Database=HR;Uid=admin;Pwd=password;Convert Zero Datetime=true;" //SslMode=none;
//Use MySql.Data.dll (See the project file for details):
//type HRSpocs = SqlDataProvider<Common.DatabaseProviderTypes.MYSQL, connStr, Owner = "HR", ResolutionPath = msyqlDataPath>

//Use MySqlConnector.dll:
type HR = SqlDataProvider<Common.DatabaseProviderTypes.MYSQL, connStr, Owner = "HR", ResolutionPath = msyqlConnectorPath>

[<EntryPoint>]
let main argv =
    let runtimeConnectionString = connStr
    let ctx = HR.GetDataContext runtimeConnectionString
    let employeesFirstName = 
        query {
            for emp in ctx.Hr.Employees do
            select (emp.FirstName)
        } |> Seq.head

    printfn "Hello %s!" employeesFirstName
    System.Threading.Thread.Sleep 2000
    0 // return an integer exit code
