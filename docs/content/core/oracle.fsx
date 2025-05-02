(*** hide ***)
#I @"../../files/sqlite"
(*** hide ***)
#I "../../../bin/lib/netstandard2.0"
(*** hide ***)
[<Literal>]
let connectionString = "Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=MyHost)(PORT=MyPort))(CONNECT_DATA=(SERVICE_NAME=MyOracleSID)));User Id=myUsername;Password=myPassword;"
(*** hide ***)
[<Literal>]
let resolutionPath = "TODO"

(**
# SQL Provider for Oracle

Oracle is based on the current release (12.1.0.1.2) of the managed ODP.NET driver found [here](http://www.oracle.com/technetwork/topics/dotnet/downloads/index.html). However, although the managed version is recommended, it should also work with previous versions of the native driver.

SQLProvider with Oracle is available via both NuGet-Packages:

- NuGet: [SQLProvider.Oracle](https://www.nuget.org/packages/SQLProvider.Oracle) - Experimental, not tested. Fixed libraries. Doesn't need resolution path. TypeProvider class: `FSharp.Data.Sql.Oracle.SqlDataProvider`
- NuGet: [SQLProvider](https://www.nuget.org/packages/SQLProvider) - Dynamic version. TypeProvider class: `FSharp.Data.Sql.SqlDataProvider`

*)

#r "FSharp.Data.SqlProvider.Common.dll"
#r "FSharp.Data.SqlProvider.dll"

open FSharp.Data.Sql

type sql = SqlDataProvider<Common.DatabaseProviderTypes.ORACLE, connectionString, ResolutionPath = resolutionPath>
let ctx = sql.GetDataContext()

let customers = ctx.Customers |> Seq.toArray

(**

Because Oracle databases can be massive, an optional constructor parameter, `TableNames`, can be used as a filter.

*)
