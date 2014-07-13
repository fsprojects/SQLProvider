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

open System
open FSharp.Data.Sql
open FSharp.Data.Sql.Providers
open Oracle

fsi.AddPrintTransformer(fun (x:Type) -> x.FullName |> box)
let connectionString = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.56.101)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=pdb1)));User Id=HR;Password=oracle;"
Oracle.resolutionPath <- @"D:\Oracle\product\12.1.0\client_1\odp.net\managed\common"


//let connectionString = "Data Source=fused41;User Id=mopdc;Password=mopdc;"
//Oracle.resolutionPath <- @"C:\Program Files\Oracle\product\11.2.0\client_1\ODP.NET\bin\4"

Oracle.owner <- "HR"
let connection = Oracle.createConnection connectionString
Oracle.connect connection Oracle.createTypeMappings

Oracle.connect connection (Oracle.getSchema "Packages" [|"HR"|])
|> DataTable.printDataTable

Oracle.connect connection (Oracle.getSchema "DataTypes" [||])
|> DataTable.printDataTable

Oracle.connect connection (Oracle.getSchema "PackageBodies" [|"HR"|])
|> DataTable.printDataTable

Oracle.connect connection (Oracle.getSchema "Functions" [|"HR"|])
|> DataTable.printDataTable

Oracle.connect connection (Oracle.getSchema "Procedures" [|"HR"|])
|> DataTable.printDataTable

Oracle.connect connection (Oracle.getSchema "ProcedureParameters" [|"HR"|])
|> DataTable.printDataTable

Oracle.typeMappings

Oracle.connect connection Oracle.getSprocs

#r  @"D:\Oracle\product\12.1.0\client_1\odp.net\managed\common\Oracle.ManagedDataAccess.dll"

open Oracle.ManagedDataAccess.Client
open Oracle.ManagedDataAccess.Types

[|
    for v in Enum.GetValues(typeof<OracleDbType>) do
        yield v :?> int, (Enum.GetName(typeof<OracleDbType>, v))
|]

open System

type MyRecord = {
    TimeStamp : DateTime
    Name : string
    Value : float
}


let e = new Common.SqlEntity(Unchecked.defaultof<_>, "foo")
e.SetData(["TIME_STAMP", DateTime.Now |>  box; "NAME", "Colin Bull" |> box; "Value", 1. |> box])

e.MapTo<MyRecord>()

typeof<MyRecord>.GetProperties()
