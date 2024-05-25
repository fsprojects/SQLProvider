
// Instead of referencing nuget, it would be better to call it once, to download the package 
// and then reference the individual 3 files listed below via "#r", so intellisense works better.
// Or if you are doing a project, not a script, reference you can reference them like usual and you should be fine...
#r "nuget: DuckDB.NET.Data.Full"

#I @"../../../bin/net472"
#r @"../../../bin/net472/FSharp.Data.SqlProvider.dll"

// Note: In Windows you need to install first: https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist

open System
open FSharp.Data.Sql
open FSharp.Data.Sql.Common

[<Literal>]
let compileConnStr = @"Data Source=" + __SOURCE_DIRECTORY__ + @"\..\db\duckdbsample.db;ACCESS_MODE=READ_ONLY"


// We need resolutionPath of:
// DuckDB.NET.Data.dll (NetStandard2.0) 
// DuckDB.NET.Bindings.dll (NetStandard2.0) 
// And the native driver (from the DuckDB.NET.Data.Full, you need runtimes of your platform) 
// Windows: duckdb.dll, Mac: libduckdb.dylib, Unbuntu: libduckdb.so, and so on.

[<Literal>]
let dataPath = 
    @"C:\Users\(my_username)\.nuget\packages\duckdb.net.bindings.full\0.10.3\lib\netstandard2.0;" + 
    @"C:\Users\(my_username)\.nuget\packages\duckdb.net.data.full\0.10.3\lib\netstandard2.0;" + 
    @"C:\Users\(my_username)\.nuget\packages\duckdb.net.bindings.full\0.10.3\runtimes\win-x64\native"
// (Or just copy them to ../libs/ and reference only: 
// [<Literal>] let dataPath = __SOURCE_DIRECTORY__ + "/../libs/"

// Test manual connection:
// let duckDBConnection = new DuckDB.NET.Data.DuckDBConnection(connStr)
// duckDBConnection.Open()


type DbContext = SqlDataProvider<Common.DatabaseProviderTypes.DUCKDB, compileConnStr, ResolutionPath = dataPath>


 //Note: DuckDB concurrency limitation:
 // It only allows one read-write-connection, or multiple concurrent read-connections.
let runtimeConnString = @"Data Source=" + __SOURCE_DIRECTORY__ + @"\..\db\duckdbsample.db"

let ctx = DbContext.GetDataContext runtimeConnString
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %O")

let test = 
    query { 
        for city in ctx.Main.Cities do
        join x in ctx.Main.Weather on (city.Name = x.City)
        where (city.Name = "San Francisco")
        select x.City
    } |> Seq.toList 
    // Or better: |> Seq.executeQueryAsync
