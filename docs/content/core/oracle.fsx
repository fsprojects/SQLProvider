(*** hide ***)
#I @"../../files/sqlite"
(*** hide ***)
#I "../../../bin"
(*** hide ***)
[<Literal>]
let connectionString = "Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=MyHost)(PORT=MyPort))(CONNECT_DATA=(SERVICE_NAME=MyOracleSID)));User Id=myUsername;Password=myPassword;"
(*** hide ***)
[<Literal>]
let resolutionPath = "TODO"

(**
# SQL Provider for Oracle

Oracle is based on the current release (12.1.0.1.2) of the managed ODP.NET driver found [here](http://www.oracle.com/technetwork/topics/dotnet/downloads/index.html). However although the managed version is recommended it should also work with previous versions of the native driver.

*)

#r "FSharp.Data.SqlProvider.dll"

open FSharp.Data.Sql

type sql = SqlDataProvider< connectionString, Common.DatabaseProviderTypes.ORACLE, resolutionPath >
let ctx = sql.GetDataContext()

let customers = ctx.``[main].[Customers]`` |> Seq.toArray
