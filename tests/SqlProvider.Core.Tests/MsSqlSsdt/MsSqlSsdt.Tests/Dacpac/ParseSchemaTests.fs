module Dacpac.ParseSchemaTests
open NUnit.Framework
open FSharp.Data.Sql.Providers

[<Test>]
let ``Parse AdventureWorks dacpac`` () =
    UnzipTests.dacPacPath
    |> UnzipTests.extractModelXml 
    |> MSSqlServerSsdt.parseXml
    |> printfn "%A"
