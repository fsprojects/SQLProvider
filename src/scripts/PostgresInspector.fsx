#I "../SQLProvider"

#r "System.Transactions"
#r "System.Runtime.Serialization"
#load "Operators.fs"
#load "SchemaProjections.fs"
#load "SqlSchema.fs"
#load "DataTable.fs"
#load "SqlRuntime.Patterns.fs"
#load "SqlRuntime.Common.fs"
#load "Providers.Postgresql.fs"

open System
open FSharp.Data.Sql
open FSharp.Data.Sql.Providers

fsi.AddPrintTransformer(fun (x:Type) -> x.FullName |> box)
let connectionString = "User ID=postgres;Password=admin;Host=localhost;Port=5432;Database=dvdrental;"
PostgreHelper.resolutionPath <- @"D:\Downloads\Npgsql-2.1.3-net20"

let connection = PostgreHelper.createConnection connectionString

connection.GetType().GetMethods()
|> Array.iter (fun m -> printfn "%A" (m.Name, m.GetParameters()))

PostgreHelper.connect connection (PostgreHelper.getSchema "MetaDataCollections" [||])
|> DataTable.printDataTable