#I @"../../../bin"
#r @"../../../bin/FSharp.Data.SqlProvider.dll"

open FSharp.Data.Sql

[<Literal>]
let connectionString = @"Driver={Microsoft Excel Driver (*.xls)};DriverId=790;Dbq=" + __SOURCE_DIRECTORY__ + @"\..\..\..\docs\files\ie_data.xls;DefaultDir=" + __SOURCE_DIRECTORY__ + @"\..\..\..\docs\files\;"

type database = SqlDataProvider<Common.DatabaseProviderTypes.ODBC, connectionString>
let odbc = database.GetDataContext()

let x =
    query {
        for i in odbc.``[].[Print_Area]`` do
        //where (i.F10 > 10.)
        select i.F1
    } |> Seq.take 10 |> Seq.toArray
x |> Array.iter (printfn "%A")
