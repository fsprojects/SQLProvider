// For more information see https://aka.ms/fsharp-console-apps
open System
open FSharp.Data
open FSharp.Data.Sql
open FSharp.Data.Sql.Odbc

[<Literal>]
let connectionString = @"Driver={Microsoft Excel Driver (*.xls)};DriverId=790;Dbq=" + __SOURCE_DIRECTORY__ + @"\..\..\..\docs\files\ie_data.xls;DefaultDir=" + __SOURCE_DIRECTORY__ + @"\..\..\..\docs\files\;"

type TypeProviderConnection =
    SqlDataProvider< 
        ConnectionString = connectionString,
        DatabaseVendor = Common.DatabaseProviderTypes.ODBC,
        IndividualsAmount=100,
        UseOptionTypes=FSharp.Data.Sql.Common.NullableColumnType.VALUE_OPTION>

let ctx = TypeProviderConnection.GetDataContext()

// let qry = query { for c in ctx...
