#I "../SQLProvider"

#r "System.Transactions"
#r "System.Runtime.Serialization"
#load "Operators.fs"
#load "SchemaProjections.fs"
#load "SqlSchema.fs"
#load "DataTable.fs"
#load "SqlRuntime.Patterns.fs"
#load "SqlRuntime.Common.fs"
#load "Providers.MsSqlServer.fs"

open System
open FSharp.Data.Sql
open FSharp.Data.Sql.Providers
open MSSqlServer
open System.Data.SqlClient
open System.Data
open FSharp.Data.Sql.Schema

fsi.AddPrintTransformer(fun (x:Type) -> x.FullName |> box)
let connectionString = "Data Source=.;Initial Catalog=AdventureWorks;Integrated Security=True"

let connection = MSSqlServer.createConnection connectionString

MSSqlServer.connect connection MSSqlServer.createTypeMappings

MSSqlServer.connect connection MSSqlServer.getSprocs

MSSqlServer.connect connection (MSSqlServer.getSchema "DataTypes" [||])
|> DataTable.printDataTable
