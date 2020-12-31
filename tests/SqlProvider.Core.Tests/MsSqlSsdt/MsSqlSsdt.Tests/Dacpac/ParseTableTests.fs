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

let parse() =
    let removeBrackets (s: string) = s.Replace("[", "").Replace("]", "")
    let doc, node, nodes = IO.File.ReadAllText(modelPath) |> toXmlNamespaceDoc "http://schemas.microsoft.com/sqlserver/dac/Serialization/2012/02"
    let model = doc :> XmlNode |> node "/x:DataSchemaModel/x:Model"

    let parsePrimaryKeyConstraint (pkElement: XmlNode) =
        let fullName = pkElement |> att "Name"
        let relationship = pkElement |> nodes "x:Relationship" |> Seq.find (fun r -> r |> att "Name" = "ColumnSpecifications")
        let columns =
            relationship
            |> nodes "x:Entry"
            |> Seq.map (node "x:Element/x:Relationship/x:Entry/x:References" >> att "Name")
            |> Seq.map (fun full ->
                { PrimaryKeyColumnRef.FullName = full
                  PrimaryKeyColumnRef.Name = full.Split([|'.';'[';']'|], StringSplitOptions.RemoveEmptyEntries) |> Array.last
                }
            )
            |> Seq.toList
        { PrimaryKeyConstraint.Name = fullName
          PrimaryKeyConstraint.Columns = columns }

    let pkConstraintsByColumn =
        model
        |> nodes "x:Element"
        |> Seq.filter (fun e -> e |> att "Type" = "SqlPrimaryKeyConstraint")
        |> Seq.map parsePrimaryKeyConstraint
        |> Seq.collect (fun pk -> pk.Columns |> List.map (fun col -> col, pk))
        |> Seq.toList

    let parseColumn (colEntry: XmlNode) =
        let el = colEntry |> node "x:Element"
        let colType, fullName = el |> att "Type", el |> att "Name"
        match colType with
        | "SqlSimpleColumn" -> 
            let allowNulls = el |> nodes "x:Property" |> Seq.tryFind (fun p -> p |> att "Name" = "IsNullable") |> Option.map (fun p -> p |> att "Value")
            let isIdentity = el |> nodes "x:Property" |> Seq.tryFind (fun p -> p |> att "Name" = "IsIdentity") |> Option.map (fun p -> p |> att "Value")
            let dataType = el |> node "x:Relationship/x:Entry/x:Element/x:Relationship/x:Entry/x:References" |> att "Name"
            Some
                { SsdtColumn.Name = fullName.Split([|'.';'[';']'|], StringSplitOptions.RemoveEmptyEntries) |> Array.last
                  SsdtColumn.FullName = fullName
                  SsdtColumn.AllowNulls = match allowNulls with | Some allowNulls -> allowNulls = "True" | _ -> true
                  SsdtColumn.DataType = dataType |> removeBrackets
                  SsdtColumn.HasDefault = false
                  SsdtColumn.IsIdentity = isIdentity |> Option.map (fun isId -> isId = "True") |> Option.defaultValue false }
        | _ ->
            None // Unsupported column type

    let parseTable (tblElement: XmlNode) =
        let fullName = tblElement |> att "Name" |> removeBrackets
        let relationship = tblElement |> nodes "x:Relationship" |> Seq.find (fun r -> r |> att "Name" = "Columns")
        let columns = relationship |> nodes "x:Entry" |> Seq.choose parseColumn |> Seq.toList
        let nameParts = fullName.Split([|'.'|], StringSplitOptions.RemoveEmptyEntries)
        let primaryKey =
            columns
            |> List.choose(fun c -> pkConstraintsByColumn |> List.tryFind(fun (colRef, pk) -> colRef.FullName = c.FullName))
            |> List.tryHead
            |> Option.map snd
        { SsdtTable.Schema = match nameParts with | [|schema;name|] -> schema | _ -> ""
          SsdtTable.Name = match nameParts with | [|schema;name|] -> name | [|name|] -> name | _ -> ""
          SsdtTable.Columns = columns
          SsdtTable.IsView = false
          SsdtTable.PrimaryKey = primaryKey
          SsdtTable.ForeignKeys = []
        }

    let tables = 
        model
        |> nodes "x:Element"
        |> Seq.filter (fun e -> e |> att "Type" = "SqlTable")
        |> Seq.map parseTable
        |> Seq.toList

    tables

[<Test>]
let ``Parse Dacpac Model`` () =
    let tables = parse()

    printfn "%A" tables
