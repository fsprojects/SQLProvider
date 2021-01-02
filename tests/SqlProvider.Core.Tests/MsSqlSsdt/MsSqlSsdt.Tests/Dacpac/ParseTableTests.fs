module Dacpac.ParseTableTests
open NUnit.Framework
open System.Xml
open Utils
open System

let parseXml(xml: string) =
    let removeBrackets (s: string) = s.Replace("[", "").Replace("]", "")
    let splitFullName (fn: string) = fn.Split([|'.';']';'['|], StringSplitOptions.RemoveEmptyEntries)
    let doc, node, nodes = xml |> toXmlNamespaceDoc "http://schemas.microsoft.com/sqlserver/dac/Serialization/2012/02"
    let model = doc :> XmlNode |> node "/x:DataSchemaModel/x:Model"

    let parsePrimaryKeyConstraint (pkElement: XmlNode) =
        let name = pkElement |> att "Name"
        let relationship = pkElement |> nodes "x:Relationship" |> Seq.find (fun r -> r |> att "Name" = "ColumnSpecifications")
        let columns =
            relationship
            |> nodes "x:Entry"
            |> Seq.map (node "x:Element/x:Relationship/x:Entry/x:References" >> att "Name")
            |> Seq.map (fun full -> { ConstraintColumn.FullName = full })
            |> Seq.toList
        { PrimaryKeyConstraint.Name = name
          PrimaryKeyConstraint.Columns = columns }

    let pkConstraintsByColumn =
        model
        |> nodes "x:Element"
        |> Seq.filter (fun e -> e |> att "Type" = "SqlPrimaryKeyConstraint")
        |> Seq.map parsePrimaryKeyConstraint
        |> Seq.collect (fun pk -> pk.Columns |> List.map (fun col -> col, pk))
        |> Seq.toList

    let parseFkRelationship (fkElement: XmlNode) =
        let name = fkElement |> att "Name"
        let localColumns = fkElement |> nodes "x:Relationship" |> Seq.find(fun r -> r |> att "Name" = "Columns") |> nodes "x:Entry/x:References" |> Seq.map (att "Name")
        let localTable = fkElement |> nodes "x:Relationship" |> Seq.find(fun r -> r |> att "Name" = "DefiningTable") |> node "x:Entry/x:References" |> att "Name"
        let foreignColumns = fkElement |> nodes "x:Relationship" |> Seq.find(fun r -> r |> att "Name" = "ForeignColumns") |> nodes "x:Entry/x:References" |> Seq.map (att "Name")
        let foreignTable = fkElement |> nodes "x:Relationship" |> Seq.find(fun r -> r |> att "Name" = "ForeignTable") |> node "x:Entry/x:References" |> att "Name"
        { SsdtRelationship.Name = name
          SsdtRelationship.DefiningTable =
            { RefTable.FullName = localTable
              RefTable.Columns = localColumns |> Seq.map (fun fnm -> { ConstraintColumn.FullName = fnm } ) |> Seq.toList }
          SsdtRelationship.ForeignTable =
            { RefTable.FullName = foreignTable
              RefTable.Columns = foreignColumns |> Seq.map (fun fnm -> { ConstraintColumn.FullName = fnm } ) |> Seq.toList } }

    let relationships =
        model
        |> nodes "x:Element"
        |> Seq.filter (fun e -> e |> att "Type" = "SqlForeignKeyConstraint")
        |> Seq.map parseFkRelationship
        |> Seq.toList
        
    let parseTableColumn (colEntry: XmlNode) =
        let el = colEntry |> node "x:Element"
        let colType, fullName = el |> att "Type", el |> att "Name"
        match colType with
        | "SqlSimpleColumn" -> 
            let allowNulls = el |> nodes "x:Property" |> Seq.tryFind (fun p -> p |> att "Name" = "IsNullable") |> Option.map (fun p -> p |> att "Value")
            let isIdentity = el |> nodes "x:Property" |> Seq.tryFind (fun p -> p |> att "Name" = "IsIdentity") |> Option.map (fun p -> p |> att "Value")
            let dataType = el |> node "x:Relationship/x:Entry/x:Element/x:Relationship/x:Entry/x:References" |> att "Name"
            Some
                { SsdtColumn.Name = fullName |> splitFullName |> Array.last
                  SsdtColumn.FullName = fullName
                  SsdtColumn.AllowNulls = match allowNulls with | Some allowNulls -> allowNulls = "True" | _ -> true
                  SsdtColumn.DataType = dataType |> removeBrackets
                  SsdtColumn.HasDefault = false
                  SsdtColumn.IsIdentity = isIdentity |> Option.map (fun isId -> isId = "True") |> Option.defaultValue false }
        | _ ->
            None // Unsupported column type

    let parseTable (tblElement: XmlNode) =
        let fullName = tblElement |> att "Name"
        let relationship = tblElement |> nodes "x:Relationship" |> Seq.find (fun r -> r |> att "Name" = "Columns")
        let columns = relationship |> nodes "x:Entry" |> Seq.choose parseTableColumn |> Seq.toList
        let nameParts = fullName |> splitFullName
        let primaryKey =
            columns
            |> List.choose(fun c -> pkConstraintsByColumn |> List.tryFind(fun (colRef, pk) -> colRef.FullName = c.FullName))
            |> List.tryHead
            |> Option.map snd
        { SsdtTable.Schema = match nameParts with | [|schema;name|] -> schema | _ -> failwithf "Unable to parse table '%s' schema." fullName
          SsdtTable.Name = match nameParts with | [|schema;name|] -> name | [|name|] -> name | _ -> failwithf "Unable to parse table '%s' name." fullName
          SsdtTable.FullName = fullName
          SsdtTable.Columns = columns
          SsdtTable.IsView = false
          SsdtTable.PrimaryKey = primaryKey }

    let parseViewColumn (colEntry:  XmlNode) =
        let colFullNm = colEntry |> node "x:Element" |> att "Name"
        let typeRelation = colEntry |> node "x:Element" |> node "x:Relationship" |> Option.ofObj
        let colRefPath = typeRelation |> Option.map (fun rel -> rel |> node "x:Entry/x:References" |> att "Name")
        { SsdtViewColumn.FullName = colFullNm
          SsdtViewColumn.ColumnRefPath = colRefPath }

    /// Recursively collections view column refs from any nested 'DynamicObjects' (ex: CTEs).
    let collectDynamicColumnRefs (viewElement: XmlNode) =
        let rec recurse (columns: SsdtViewColumn list) (el: XmlNode)  =
            let relationshipColumns = el |> nodes "x:Relationship" |> Seq.tryFind (fun r -> r |> att "Name" = "Columns")
            let relationshipDynamicObjects = el |> nodes "x:Relationship" |> Seq.tryFind (fun r -> r |> att "Name" = "DynamicObjects")
            let cols = relationshipColumns |> Option.map (nodes "x:Entry" >> Seq.map parseViewColumn) |> Option.defaultValue Seq.empty |> Seq.toList
            let accumulatedColumns = columns @ cols
            match relationshipDynamicObjects with
            | Some rel -> rel |> nodes "x:Entry" |> Seq.map (node "x:Element") |> Seq.collect (recurse accumulatedColumns) |> Seq.toList
            | None -> accumulatedColumns

        recurse [] viewElement

    let parseView (viewElement: XmlNode) =
        let fullName = viewElement |> att "Name"
        let relationshipColumns = viewElement |> nodes "x:Relationship" |> Seq.find (fun r -> r |> att "Name" = "Columns")
        let columns = relationshipColumns |> nodes "x:Entry" |> Seq.map parseViewColumn
        let dynamicColumns = collectDynamicColumnRefs viewElement

        let nameParts = fullName |> splitFullName
        { SsdtView.FullName = fullName
          SsdtView.Schema = match nameParts with | [|schema;name|] -> schema | _ -> ""
          SsdtView.Name = match nameParts with | [|schema;name|] -> name | _ -> ""
          SsdtView.Columns = columns |> Seq.toList
          SsdtView.DynamicColumns = dynamicColumns }

    /// Recursively resolves column references.
    let resolveColumnRefPath (tableColumnsByPath: Map<string, SsdtColumn>) (viewColumnsByPath: Map<string, SsdtViewColumn>) (viewCol: SsdtViewColumn) =
        let rec resolve (path: string) =
            match tableColumnsByPath.TryFind(path) with
            | Some tblCol ->
                { tblCol with
                    FullName = viewCol.FullName
                    Name = viewCol.FullName |> splitFullName |> Array.last } |> Some
            | None -> 
                match viewColumnsByPath.TryFind(path) with
                | Some viewCol when viewCol.ColumnRefPath <> Some path ->
                    match viewCol.ColumnRefPath with
                    | Some colRefPath -> resolve colRefPath
                    | None -> None
                | _ -> None

        match viewCol.ColumnRefPath with
        | Some path -> resolve path
        | None -> None

    let parseStoredProc (spElement: XmlNode) =
        let fullName = spElement |> att "Name"        
        let parameters =
            match spElement |> nodes "x:Relationship" |> Seq.tryFind (fun r -> r |> att "Name" = "Parameters") with
            | Some relationshipParameters ->
                relationshipParameters
                |> nodes "x:Entry"
                |> Seq.map (fun entry ->
                    let el = entry |> node "x:Element"
                    let pFullName = el |> att "Name"
                    let isOutput =
                        match el |> nodes "x:Property" |> Seq.tryFind (fun p -> p |> att "Name" = "IsOutput") with
                        | Some p when p |> att "Value" = "True" -> true
                        | _ -> false

                    let dataType = el |> node "x:Relationship/x:Entry/x:Element/x:Relationship/x:Entry/x:References" |> att "Name"
                    { FullName = pFullName
                      Name = pFullName |> splitFullName |> Array.last
                      DataType = dataType |> removeBrackets
                      Length = None // TODO: Implement
                      IsOutput = isOutput }
                )
            | None -> Seq.empty

        let parts = fullName |> splitFullName
        { FullName = fullName
          Schema = match parts with | [|schema;name|] -> schema | _ -> ""
          Name = match parts with | [|schema;name|] -> name | _ -> failwithf "Unable to parse sp name from '%s'" fullName
          Parameters = parameters |> Seq.toList }

    let storedProcs =
        model
        |> nodes "x:Element"
        |> Seq.filter (fun e -> e |> att "Type" = "SqlProcedure")
        |> Seq.map parseStoredProc
        |> Seq.toList

    let tables = 
        model
        |> nodes "x:Element"
        |> Seq.filter (fun e -> e |> att "Type" = "SqlTable")
        |> Seq.map parseTable
        |> Seq.toList

    let views =
        model
        |> nodes "x:Element"
        |> Seq.filter (fun e -> e |> att "Type" = "SqlView")
        |> Seq.map parseView
        |> Seq.toList

    let tableColumnsByPath = tables |> List.collect (fun t -> t.Columns) |> List.map (fun c -> c.FullName, c) |> Map.ofList
    let viewColumnsByPath = views |> List.collect (fun v -> v.Columns @ v.DynamicColumns) |> List.map (fun c -> c.FullName, c) |> Map.ofList
    let resolveColumnRefPath = resolveColumnRefPath tableColumnsByPath viewColumnsByPath

    let viewToTable (view: SsdtView) =
        { SsdtTable.FullName = view.FullName
          SsdtTable.Name = view.Name
          SsdtTable.Schema = view.Schema
          SsdtTable.IsView = true
          SsdtTable.PrimaryKey = None
          SsdtTable.Columns =
            view.Columns
            |> List.map (fun vc ->
                match resolveColumnRefPath vc with
                | Some tc -> tc
                | None ->
                    // If we can't resolve a view column, default to an obj so the user can cast it themselves.
                    { SsdtColumn.FullName = vc.FullName
                      SsdtColumn.Name = vc.FullName |> splitFullName |> Array.last
                      SsdtColumn.DataType = "SQL_VARIANT"
                      SsdtColumn.AllowNulls = false
                      SsdtColumn.HasDefault = false
                      SsdtColumn.IsIdentity = false } 
            )
        }

    let tablesAndViews = tables @ (views |> List.map viewToTable)

    { SsdtSchema.Tables = tablesAndViews
      SsdtSchema.StoredProcs = storedProcs
      SsdtSchema.TryGetTableByName = fun nm -> tablesAndViews |> List.tryFind (fun t -> t.Name = nm)
      SsdtSchema.Relationships = relationships }

open UnzipTests

[<Test>]
let ``Parse Dacpac Model`` () =
    dacPacPath
    |> extractModelXml 
    |> parseXml
    |> printfn "%A"

[<Test>]
let ``Parse local dacpac``() =
    @"C:\_mdsk\CEI.BimHub\Product\Source\CEI.BimHub.DB\bin\Debug\CEI.BimHub.DB.dacpac"
    |> extractModelXml
    |> parseXml
    |> printfn "%A"
