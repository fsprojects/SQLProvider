module FileScanTests
open NUnit.Framework
open FSharp.Data.Sql.Providers.MSSqlServerSsdt
open Utils
open System

[<Literal>]
let ssdtDirectory = __SOURCE_DIRECTORY__ + @"/SSDT Project/"

[<Test>]
let ``Should Recursively Find Scripts`` () =
    let scripts = ssdtDirectory |> IO.DirectoryInfo |> findAllScriptsInDir |> Seq.toList
    printfn "%A" scripts
    Assert.IsTrue(scripts.Length > 0)

[<Test>]
let ``Should Find Table Scripts`` () =
    let scripts = ssdtDirectory |> IO.DirectoryInfo |> findAllScriptsInDir |> Seq.map (readFile >> Utils.sqlToSchemaXml >> analyzeXml) |> Seq.toList
    printfn "%A" scripts
    Assert.IsTrue(scripts.Length > 0)

[<Literal>]
let ssdtProjFile = __SOURCE_DIRECTORY__ + @"/SSDT Project/SampleProject.sqlproj"

[<Test>]
let ``Should Find Scripts From Sql Project File`` () =
    let scripts = ssdtProjFile |> IO.FileInfo |> getScriptsFromSqlProj |> Seq.toList
    printfn "%A" scripts
    Assert.IsTrue(scripts.Length > 0)
    let allFilesExist = not (scripts |> List.exists(fun s -> s.Exists = false))
    Assert.IsTrue(allFilesExist, "All files should exist.")
