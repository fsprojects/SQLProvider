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
//let connectionString = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.56.101)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=pdb1)));User Id=HR;Password=oracle;"
//Oracle.resolutionPath <- @"D:\Oracle\product\12.1.0\client_1\odp.net\managed\common"

let connectionString = "Data Source=fused41;User Id=mopdc;Password=mopdc;"
Oracle.resolutionPath <- @"C:\Program Files\Oracle\product\11.2.0\client_1\ODP.NET\bin\4"

Oracle.owner <- "MOPDC"
let connection = Oracle.createConnection connectionString
Sql.connect connection Oracle.createTypeMappings

Sql.connect connection (Oracle.getSchema "Packages" [|"MOPDC"|])
|> DataTable.printDataTable

Sql.connect connection (Oracle.getSchema "DataTypes" [||])
|> DataTable.printDataTable

Sql.connect connection (Oracle.getSchema "ForeignKeys" [|"MOPDC"; "AUDIT_TRAIL"|])
|> DataTable.printDataTable

Sql.connect connection (Oracle.getSchema "ForeignKeyColumns" [|"MOPDC"; "AUDIT_TRAIL"|])
|> DataTable.printDataTable

Sql.connect connection (Oracle.getSchema "PackageBodies" [|"MOPDC"|])
|> DataTable.printDataTable

Sql.connect connection (Oracle.getSchema "Functions" [|"MOPDC"|])
|> DataTable.printDataTable

Sql.connect connection (Oracle.getSchema "Procedures" [|"MOPDC"|])
|> DataTable.printDataTable

Sql.connect connection (Oracle.getSchema "ProcedureParameters" [|"MOPDC"|])
|> DataTable.printDataTable

Sql.connect connection (fun c -> 
    let tables = Oracle.getTables c
    let priKeys = Oracle.getPrimaryKeys c |> dict
    [
        for table in tables do
            yield table.FullName, (Oracle.getColumns priKeys table.Name c)
    ])


Oracle.typeMappings

Sql.connect connection Oracle.getSprocs
