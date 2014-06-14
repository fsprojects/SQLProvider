#I "../SQLProvider"

#r "System.Transactions"
#r "System.Runtime.Serialization"
#load "Operators.fs"
#load "SchemaProjections.fs"
#load "SqlSchema.fs"
#load "DataTable.fs"
#load "SqlRuntime.Patterns.fs"
#load "SqlRuntime.Common.fs"
#load "Providers.Oracle.fs"

open FSharp.Data.Sql
open FSharp.Data.Sql.Providers

let connectionString = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.56.101)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=pdb1)));User Id=HR;Password=oracle;"
OracleHelpers.resolutionPath <- @"D:\Oracle\product\12.1.0\client_1\odp.net\managed\common"
OracleHelpers.owner <- "HR"
let connection = OracleHelpers.createConnection connectionString
OracleHelpers.connect connection OracleHelpers.createTypeMappings

OracleHelpers.connect connection (OracleHelpers.getSchema "Packages" [|"HR"|])
|> DataTable.printDataTable

OracleHelpers.connect connection (OracleHelpers.getSchema "DataTypes" [||])
|> DataTable.printDataTable

OracleHelpers.connect connection (OracleHelpers.getSchema "PackageBodies" [|"HR"|])
|> DataTable.printDataTable

OracleHelpers.connect connection (OracleHelpers.getSchema "Functions" [|"HR"|])
|> DataTable.printDataTable

OracleHelpers.connect connection (OracleHelpers.getSchema "Procedures" [|"HR"|])
|> DataTable.printDataTable

OracleHelpers.connect connection (OracleHelpers.getSchema "ProcedureParameters" [|"HR"|])
|> DataTable.printDataTable

OracleHelpers.sqlToEnum "REF CURSOR"
OracleHelpers.sqlToClr "REF CURSOR"



let sprocs = OracleHelpers.connect connection OracleHelpers.getSprocs

#r "D:\Oracle\product\12.1.0\client_1\odp.net\managed\common\Oracle.ManagedDataAccess.dll"

open Oracle.ManagedDataAccess.Client
open Oracle.ManagedDataAccess.Types

typeof<OracleRefCursor>.Name