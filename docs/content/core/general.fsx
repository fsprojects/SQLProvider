(*** hide ***)
#I @"../../files/sqlite"
(*** hide ***)
#I "../../../bin"
(*** hide ***)
[<Literal>]
let connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"\northwindEF.db;Version=3"
(*** hide ***)
[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + @"..\..\..\files\sqlite" 

(**
# SQL Provider

*)

#r "FSharp.Data.SqlProvider.dll"

open FSharp.Data.Sql

type sql = SqlDataProvider< connectionString, Common.DatabaseProviderTypes.SQLITE, resolutionPath >
let ctx = sql.GetDataContext()

let customers = ctx.``[main].[Customers]`` |> Seq.toArray

let christina = ctx.``[main].[Customers]``.Individuals.``As ContactName``.``BERGS, Christina Berglund``
