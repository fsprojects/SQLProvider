#r @"..\..\bin\FSharp.Data.SqlProvider.dll"

open FSharp.Data.Sql

type database = SqlDataProvider<"Driver={Microsoft Excel Driver (*.xls)};DriverId=790;Dbq=C:\Code\ie_data.xls;DefaultDir=C:\Code;", Common.DatabaseProviderTypes.ODBC>
let odbc = database.GetDataContext();

let x =
    query {
        for i in odbc.``[].[Print_Area]`` do
        //where (i.F10 > 10.)
        select i.F1
    } |> Seq.take 10 |> Seq.toArray
x |> Array.iter (printfn "%A")
