(*** hide ***)
#I "../../files/sqlite"
(*** hide ***)
#I "../../../bin"


(**
# SQL Provider

*)

#r "FSharp.Data.SqlProvider.dll"

open FSharp.Data.Sql

type sql = SqlDataProvider< @"Data Source=../../files/sqlite/northwindEF.db;Version=3", Common.DatabaseProviderTypes.SQLITE, "../../files/sqlite" >
let ctx = sql.GetDataContext()