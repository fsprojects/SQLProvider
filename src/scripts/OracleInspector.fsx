#I "../SQLProvider"

#r "System.Transactions"
#r "System.Runtime.Serialization"
#r "System.Configuration"
#load "Operators.fs"
#load "Utils.fs"
#load "SqlSchema.fs"
//#load "SqlHelpers.fs"
#load "DataTable.fs"
#load "SqlRuntime.Patterns.fs"
#load "SqlRuntime.Common.fs"
#load "Providers.Oracle.fs"

open System
open FSharp.Data.Sql
open FSharp.Data.Sql.Providers
open FSharp.Data.Sql.Common
open Oracle

fsi.AddPrintTransformer(fun (x:Type) -> x.FullName |> box)
let connectionString = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.1.90)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=XE)));User Id=HR;Password=password;"
Oracle.resolutionPath <- "/Users/colinbull/appdev/SqlProvider/packages/scripts/Oracle.ManagedDataAccess/lib/net40"


Oracle.owner <- "HR"
let connection = Oracle.createConnection connectionString
Sql.connect connection Oracle.createTypeMappings

Sql.connect connection (Oracle.getSchema "Packages" [|"HR"|])
|> DataTable.printDataTable

Sql.connect connection (Oracle.getSchema "DataTypes" [||])
|> DataTable.printDataTable

Sql.connect connection (Oracle.getSchema "ForeignKeys" [|"HR"|])
|> DataTable.printDataTable

Sql.connect connection (Oracle.getSchema "ForeignKeyColumns" [|"HR"|])
|> DataTable.printDataTable

Sql.connect connection (Oracle.getSchema "PackageBodies" [|"HR"|])
|> DataTable.printDataTable

Sql.connect connection (Oracle.getSchema "Functions" [|"HR"|])
|> DataTable.printDataTable

Sql.connect connection (Oracle.getSchema "Procedures" [|"HR"|])
|> DataTable.printDataTable

Sql.connect connection (Oracle.getSchema "ProcedureParameters" [|"HR"|])
|> DataTable.printDataTable

Sql.connect connection (fun c ->
    let tableNames = ""
    let tables = Oracle.getTables tableNames c
    let priKeys = Oracle.getPrimaryKeys tableNames c
    [
        for table in tables do
            yield table.FullName, (Oracle.getColumns priKeys table.Name c)
    ])


Oracle.typeMappings

seq {
    for sproc in  Sql.connect connection Oracle.getSprocs do
        match sproc with
        | Schema.Root("Packages", Schema.Package(name, x)) ->
            Sql.ensureOpen connection
            yield! x.Sprocs connection
        | _ -> ()           
} |> Seq.toArray
        
