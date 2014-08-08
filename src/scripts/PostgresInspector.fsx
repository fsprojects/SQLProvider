#I "../SQLProvider"

#r "System.Transactions"
#r "System.Runtime.Serialization"
#load "Operators.fs"
#load "SchemaProjections.fs"
#load "SqlSchema.fs"
#load "DataTable.fs"
#load "SqlRuntime.Patterns.fs"
#load "SqlHelpers.fs"
#load "SqlRuntime.Common.fs"
#load "Providers.Postgresql.fs"

open System
open FSharp.Data.Sql
open FSharp.Data.Sql.Providers

fsi.AddPrintTransformer(fun (x:Type) -> x.FullName |> box)
let connectionString = "User ID=postgres;Password=password;Host=POSTGRESQL;Port=9090;Database=hr;"
PostgreSQL.resolutionPath <- @"D:\Downloads\Npgsql-2.1.3-net40"

let connection = PostgreSQL.createConnection connectionString

PostgreSQL.createTypeMappings()

PostgreSQL.connect connection (PostgreSQL.getSchema "Tables" [|"hr";"public"|])
|> DataTable.printDataTable

PostgreSQL.connect connection PostgreSQL.getSprocs
//|> DataTable.printDataTable






