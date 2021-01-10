module Dacpac.ParseSchemaTests
open NUnit.Framework
open FSharp.Data.Sql.Providers

[<Test>]
let ``Parse Dacpac Model`` () =
    UnzipTests.dacPacPath
    |> UnzipTests.extractModelXml 
    |> MSSqlServerSsdt.parseXml
    |> printfn "%A"

[<Test>]
let ``Parse local dacpac``() =
    @"C:\_mdsk\CEI.BimHub\Product\Source\CEI.BimHub.DB\bin\Debug\CEI.BimHub.DB.dacpac"
    |> UnzipTests.extractModelXml
    |> MSSqlServerSsdt.parseXml
    |> printfn "%A"
