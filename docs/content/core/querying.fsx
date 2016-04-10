(*** hide ***)
#I @"../../files/sqlite"
(*** hide ***)
#I "../../../bin"
(*** hide ***)
[<Literal>]
let connectionString =
    "Data Source=" + __SOURCE_DIRECTORY__ + @"\northwindEF.db;Version=3"
(*** hide ***)
[<Literal>]
let resolutionPath =
    __SOURCE_DIRECTORY__ + @"..\..\..\files\sqlite"
#r "FSharp.Data.SqlProvider.dll"
open FSharp.Data.Sql
(**


# Querying
*)
type sql  = SqlDataProvider<
                connectionString,
                Common.DatabaseProviderTypes.SQLITE,
                resolutionPath
            >
let ctx = sql.GetDataContext()


(**
SQLProvider leverages F#'s `query {}` expression syntax to perform queries 
against the database.  Though many are supported, not all LINQ expressions are.
*)



(**
## Expressions

These operators perform no specific function in the code itself, rather they 
are placeholders replaced by their database-specific server-side operations.
Their utility is in forcing the compiler to check against the correct types.

*)

let ctx = sql.GetDataContext()

let bergs = ctx.``[main].[Customers]``.Individuals.BERGS


(**
### Operators

* `|=|` (In set)
* `|<>|` (Not in set)
* `=%` (Like)
* `<>%` (Not like)
* `!!` (Left join)
*)


