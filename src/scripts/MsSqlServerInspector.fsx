#I "../SQLProvider"

#r "System.Transactions"
#r "System.Runtime.Serialization"
#load "Operators.fs"
#load "SchemaProjections.fs"
#load "SqlHelpers.fs"
#load "SqlSchema.fs"
#load "DataTable.fs"
#load "SqlRuntime.Patterns.fs"
#load "SqlRuntime.Common.fs"
#load "Providers.MsSqlServer.fs"

open System
open FSharp.Data.Sql
open FSharp.Data.Sql.Providers

fsi.AddPrintTransformer(fun (x:Type) -> x.FullName |> box)
let connectionString = "Data Source=SQLSERVER;Initial Catalog=AdventureWorks;User Id=sa;Password=password"

let connection = MSSqlServer.createConnection connectionString

MSSqlServer.connect connection MSSqlServer.createTypeMappings

MSSqlServer.connect connection (MSSqlServer.getSchema "DataTypes" [||])
|> DataTable.printDataTable

MSSqlServer.connect connection (MSSqlServer.getSprocs)
|> List.map (function
            | Schema.Root("Functions", Schema.Sproc(name)) -> name.Name.FullName
            | Schema.Root("Procedures", Schema.Sproc(name)) -> name.Name.FullName
            | _ -> "Zero"
           )

MSSqlServer.typeMappings
