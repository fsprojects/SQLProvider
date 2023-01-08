module MsSqlSsdt.Tests.UnzipTests
open NUnit.Framework
open System.IO.Compression
open System

[<Literal>]
let dacPacPath = __SOURCE_DIRECTORY__ + "/AdventureWorks_SSDT/bin/Debug/AdventureWorks_SSDT.dacpac"

let extractModelXml(path: string) = 
    use stream = new IO.FileStream(path, IO.FileMode.Open)
    use zip = new ZipArchive(stream, ZipArchiveMode.Read, false)
    let modelEntry = zip.GetEntry("model.xml")
    use modelStream = modelEntry.Open()
    use rdr = new IO.StreamReader(modelStream)
    rdr.ReadToEnd()

[<Test>]
let ``Unzip Dacpac Model XML``() =
    let xml = extractModelXml(dacPacPath)
    printfn "XML: %s" xml
