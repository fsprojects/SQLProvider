#I @"../../../bin"
#r @"../../../bin/FSharp.Data.SqlProvider.dll"

open FSharp.Data.Sql

// ----------------------------------------------------------------
// Excel connection.
// First create some ie_data.xls
[<Literal>]
let connectionString = @"Driver={Microsoft Excel Driver (*.xls)};DriverId=790;Dbq=" + __SOURCE_DIRECTORY__ + @"\..\..\..\docs\files\ie_data.xls;DefaultDir=" + __SOURCE_DIRECTORY__ + @"\..\..\..\docs\files\;"

type database = SqlDataProvider<Common.DatabaseProviderTypes.ODBC, connectionString, Owner="">
let odbc = database.GetDataContext()

let contributors =
    query {
        for rows in odbc.Dbo.Sheet1 do
        select (rows.Name, rows.Commits)
    } |> Seq.toArray
//val it : (string * float) [] = [|("colinbull", 160.0); ("pezipink", 95.0); ("Thorium", 61.0); ("forki", 56.0); ("janno-p", 31.0)|]


// ----------------------------------------------------------------
// SQL Server connection
// Database: http://pastebin.com/5W3PPVaD

// Configuring DSN on Windows ODBC Data Source Administrator server: 
// Control Panel -> Administrative Tools -> Data Sources (ODBC) 
// (Launch: c:\windows\syswow64\odbcad32.exe) 
// And add your driver to DSN.

open FSharp.Data.Sql 
[<Literal>] 
let dnsConn = @"DSN=foo" 
type db = SqlDataProvider<Common.DatabaseProviderTypes.ODBC, dnsConn>
let ctx = db.GetDataContext()

let entity = ctx.Dbo.Student.Create [| "StudentId", 1 |> box ; "Name", "Michael" |> box |]
ctx.SubmitUpdates()

let student =
    query {
        for c in ctx.Dbo.Student do
        select (c)
        head
    }

student.Name // val it : string = "Michael"

