module Dacpac.ParseSchemaTests
open NUnit.Framework
open FSharp.Data.Sql.Providers
open FSharp.Data.Sql.Ssdt
open FSharp.Data.Sql.Ssdt.DacpacParser

[<Test>]
let ``Parse AdventureWorks dacpac`` () =
    UnzipTests.dacPacPath
    |> UnzipTests.extractModelXml 
    |> DacpacParser.parseXml
    |> printfn "%A"


open MSSqlServerSsdt

[<Test>]
let ``Split table containing dot``() =
    let parts = "[dbo].[Products.Table]" |> RegexParsers.splitFullName
    Assert.AreEqual([|"dbo"; "Products.Table"|], parts)

[<Test>]
let ``Split table containing dot and mixed brackets``() =
    let parts = "dbo.[Products.Table]" |> RegexParsers.splitFullName
    Assert.AreEqual([|"dbo"; "Products.Table"|], parts)

[<Test>]
let ``Split table with no brackets``() =
    let parts = "dbo.Products" |> RegexParsers.splitFullName
    Assert.AreEqual([|"dbo"; "Products"|], parts)

[<Test>]
let ``Split table with pound sign and space``() =
    let parts = "[dbo].[#Products Table]" |> RegexParsers.splitFullName
    Assert.AreEqual([|"dbo"; "#Products Table"|], parts)
