(*** hide ***)
#r "../../../bin/lib/netstandard2.0/FSharp.Data.SqlProvider.dll"
(*** hide ***)
let [<Literal>] resolutionPath = __SOURCE_DIRECTORY__ + @"/../../files/sqlite"
(*** hide ***)
let [<Literal>] connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"\..\northwindEF.db;Version=3;Read Only=false;FailIfMissing=True;"

(**
# Individuals

Find individual rows in tables with code completion in the editor.
*)

open System
open FSharp.Data.Sql

type sql  = SqlDataProvider<
                Common.DatabaseProviderTypes.SQLITE,
                connectionString,
                SQLiteLibrary=Common.SQLiteLibrary.SystemDataSQLite,
                ResolutionPath = resolutionPath,
                CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL
            >

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
