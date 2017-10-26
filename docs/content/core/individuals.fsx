(*** hide ***)
#I "../../files/sqlite"
(*** hide ***)
#I "../../../bin/net451"
(*** hide ***)
#r @"../../../bin/net451/FSharp.Data.SqlProvider.dll"

(*** hide ***)
[<Literal>]
let connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"/../../../tests/SqlProvider.Tests/scripts/northwindEF.db;Version=3"

(*** hide ***)
[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + @"/../../../tests/SqlProvider.Tests/libs"

(**
# Individuals

Find individual rows in tables with code completion in the editor.
*)

open System
open FSharp.Data.Sql

type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE,
                           connectionString,
                           ResolutionPath = resolutionPath,
                           CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL>

let ctx = sql.GetDataContext()

let customers = ctx.Main.Customers

(**
Get individual customer row by primary key value
*)
customers.Individuals.COMMI

(**
Get individual customer row using address
*)

customers.Individuals.``As ContactName``.``COMMI, Pedro Afonso``

(**
Get individual customer row using address
*)

customers.Individuals.``As Address``.``CONSH, Berkeley Gardens 12  Brewery``
