
open System
open FSharp.Data.Sql

[<Literal>]
let compileConnStr = @"Data Source=" + __SOURCE_DIRECTORY__ + @"\..\..\SqlProvider.Tests\db\duckdbsample.db;ACCESS_MODE=READ_ONLY"

// Environment.SetEnvironmentVariable("Path", Environment.GetEnvironmentVariable("Path") + @";c:\temp\duckdb-driver") // Path for native duckdb.dll

type DbContext = FSharp.Data.Sql.DuckDb.SqlDataProvider<Common.DatabaseProviderTypes.DUCKDB, compileConnStr>

let ctx = DbContext.GetDataContext()
