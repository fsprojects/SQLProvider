// An example using MySQL driver installed from Nuget.
#r "../../packages/MySql.Data.6.9.7/lib/net45/MySql.Data.dll" 
#r "../../packages/SQLProvider.0.0.9-alpha/lib/net40/FSharp.Data.SqlProvider.dll"

open System
open System.Linq
open FSharp.Data.Sql
open MySql.Data

[<Literal>]
// Bind ResolutionPath to the folder containing MySQL driver dll files.
let ResolutionPath = "FOLDER CONTAINING MYSQL DRIVER DLL FILES"

type sql = SqlDataProvider<"Server=YOUR_SERVER;Database=YOUR_DATABASE;Uid=USER_NAME;Pwd=PASSWORD", Common.DatabaseProviderTypes.MYSQL,ResolutionPath=ResolutionPath>
let ctxt = sql.GetDataContext()

// Access the data
