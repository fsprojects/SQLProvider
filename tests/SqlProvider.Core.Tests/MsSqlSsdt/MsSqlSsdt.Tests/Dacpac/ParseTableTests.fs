module Dacpac.ParseTableTests
open NUnit.Framework
open System.Xml
open Utils
open System

[<Literal>]
let projDir = __SOURCE_DIRECTORY__

let modelPath =
    let dir = IO.DirectoryInfo(projDir).Parent.FullName
    IO.Path.Combine(dir, @"AdventureWorks_SSDT\bin\Debug\AdventureWorks_SSDT\model.xml")

[<Test>]
let ``Parse Dacpac Model`` () =
    let doc, node, nodes = IO.File.ReadAllText(modelPath) |> toXmlNamespaceDoc "http://schemas.microsoft.com/sqlserver/dac/Serialization/2012/02"

    let parseColumn (colEntry: XmlNode) =
        let el = colEntry |> node "x:Element"
        let colType, fullName = el |> att "Type", el |> att "Name"
        match colType with
        | "SqlSimpleColumn" -> 
            let allowNulls = el |> nodes "x:Property" |> Seq.tryFind (fun p -> p |> att "Name" = "IsNullable") |> Option.map (fun p -> p |> att "Value")
            let isIdentity = el |> nodes "x:Property" |> Seq.tryFind (fun p -> p |> att "Name" = "IsIdentity") |> Option.map (fun p -> p |> att "Value")
            let dataType = el |> node "x:Relationship/x:Entry/x:Element/x:Relationship/x:Entry/x:References" |> att "Name"
            let dataType = dataType.Replace("[", "").Replace("]", "")
            Some
                { SsdtColumn.Name = fullName
                  SsdtColumn.AllowNulls = match allowNulls with | Some allowNulls -> allowNulls = "True" | _ -> true
                  SsdtColumn.DataType = dataType
                  SsdtColumn.HasDefault = false
                  SsdtColumn.Identity =
                    let isIdentity = isIdentity |> Option.map (fun isId -> isId = "True") |> Option.defaultValue false
                    if isIdentity then Some { IdentitySpec.Seed = 1; IdentitySpec.Increment = 1 } else None // seed/incr is not used
                }
        | _ ->
            None // Unsupported column type

    let parseTable (tblElement: XmlNode) =
        let tblName = tblElement |> att "Name"
        let rColumns = tblElement |> nodes "x:Relationship" |> Seq.find (fun r -> r |> att "Name" = "Columns")
        let columns = rColumns |> nodes "x:Entry" |> Seq.choose parseColumn |> Seq.toList
        tblName, columns

    let model = doc |> node "/x:DataSchemaModel/x:Model"

    let tables =
        model
        |> nodes "x:Element"
        |> Seq.filter (fun e -> e |> att "Type" = "SqlTable")
        |> Seq.map parseTable
        |> Seq.toList

    printfn "%A" tables
    Assert.IsTrue(tables.Length > 0)
