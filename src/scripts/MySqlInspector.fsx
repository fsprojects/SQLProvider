#I "../SQLProvider"

#r "System.Transactions"
#r "System.Runtime.Serialization"
#load "Operators.fs"
#load "SchemaProjections.fs"
#load "SqlSchema.fs"
#load "DataTable.fs"
#load "SqlHelpers.fs"
#load "SqlRuntime.Patterns.fs"
#load "SqlRuntime.Common.fs"
#load "Providers.MySql.fs"

open System
open FSharp.Data.Sql
open FSharp.Data.Sql.Providers
open MySql

fsi.AddPrintTransformer(fun (x:Type) -> x.FullName |> box)
let connectionString = "Server=MYSQL;Database=HR;Uid=admin;Pwd=password;"
let resolutionPath = @"D:\Appdev\SqlProvider\tests\SqlProvider.Tests"

MySql.resolutionPath <- resolutionPath
MySql.owner <- "HR"

let connection = MySql.createConnection connectionString
MySql.connect connection MySql.createTypeMappings

MySql.typeMappings

MySql.connect connection (MySql.getSchema "DataTypes" [||])
|> DataTable.printDataTable

MySql.connect connection (MySql.getSchema "MetaDataCollections" [||])
|> DataTable.printDataTable

MySql.connect connection (MySql.getSchema "Restrictions" [||])
|> DataTable.printDataTable

MySql.connect connection (MySql.getSchema "Functions" [||])
|> DataTable.printDataTable

MySql.connect connection (MySql.getSchema "Databases" [||])
|> DataTable.printDataTable

MySql.connect connection (MySql.getSchema "Procedures" [||])
//|> DataTable.headers
|> DataTable.map (fun r -> r.["ROUTINE_NAME"], r.["ROUTINE_TYPE"])
//|> DataTable.printDataTable

MySql.connect connection (MySql.getSchema "Procedure Parameters" [||])
//|> DataTable.headers
|> DataTable.printDataTable

MySql.connect connection (MySql.getSchema "Columns" [||])
//|> DataTable.map (fun r -> r.["COLUMN_NAME"])
|> DataTable.printDataTable

MySql.connect connection (MySql.getSprocs)

