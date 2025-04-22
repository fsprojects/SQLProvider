module FSharp.Data.Sql.Ssdt.DacpacParser

open System
open System.Xml
open System.IO.Compression

type [<Struct>] SsdtColumn = {
    FullName: string
    Name: string
    Description: string
    DataType: string
    AllowNulls: bool
    IsIdentity: bool
    HasDefault: bool
    ComputedColumn: bool
}
type [<Struct>] SsdtViewColumn = {
    FullName: string
    ColumnRefPath: string voption
}
type [<Struct>] CommentAnnotation = {
    Column: string
    DataType: string
    Nullability: string voption
}
type SsdtView = {
    FullName: string
    Schema: string
    Name: string
    Columns: SsdtViewColumn array
    DynamicColumns: SsdtViewColumn array
    Annotations: CommentAnnotation array
}
type [<Struct>] ConstraintColumn = {
    FullName: string
    Name: string
}
type RefTable = {
    FullName: string
    Schema: string
    Name: string
    Columns: ConstraintColumn array
}

type SsdtRelationship = {
    Name: string
    DefiningTable: RefTable
    ForeignTable: RefTable
}

type PrimaryKeyConstraint = {
    Name: string
    Columns: ConstraintColumn array
}
type [<Struct>] SsdtStoredProcParam = {
    FullName: string
    Name: string
    DataType: string
    Length: int voption
    IsOutput: bool
}
type SsdtStoredProc = {
    FullName: string
    Schema: string
    Name: string
    Parameters: SsdtStoredProcParam array
    ReturnValueDataType: string voption
}
type [<Struct>] SsdtDescriptionItem = {
    DecriptionType: string
    Schema: string
    TableName: string
    ColumnName: string voption
    Description: string
}
type [<Struct>] UDDTName = UDDTName of string
type [<Struct>] UDDTInheritedType = UDDTInheritedType of string
type SsdtUserDefinedDataType = Map<UDDTName, UDDTInheritedType>

type SsdtTable = {
    FullName: string
    Schema: string
    Name: string
    Columns: SsdtColumn array
    PrimaryKey: PrimaryKeyConstraint voption
    IsView: bool
}

type SsdtSchema = {
    Tables: SsdtTable array
    TryGetTableByName: string -> SsdtTable voption
    StoredProcs: SsdtStoredProc array
    Relationships: SsdtRelationship array
    TryGetRelationshipsByTableName: string -> (SsdtRelationship array * SsdtRelationship array)
    Descriptions: SsdtDescriptionItem array
    UserDefinedDataTypes: SsdtUserDefinedDataType
    Functions: SsdtStoredProc array
}


module RegexParsers =
    open System.Text.RegularExpressions

    let tablePattern = System.Text.RegularExpressions.Regex(@"(\[(?<Brackets>[A-Za-z_@#]+[^\[\]]*)\]|(?<NoBrackets>[A-Za-z_@#]+[A-Za-z09_]*)(\.)?)", RegexOptions.IgnoreCase)
    let colPattern = System.Text.RegularExpressions.Regex(@"\/\*\s*(?<DataType>\w*)\s*(?<Nullability>(null|not null))?\s*\*\/", RegexOptions.IgnoreCase)
    let viewPattern = System.Text.RegularExpressions.Regex(@"\[?(?<Column>\w+)\]?\s*\/\*\s*(?<DataType>\w*)\s*(?<Nullability>(null|not null))?\s*\*\/", RegexOptions.IgnoreCase)


    /// Splits a fully qualified name into parts. 
    /// Name can start with a letter, _, @ or #. Names in square brackets can contain any char except for square brackets.
    let splitFullName (fn: string) =
        tablePattern.Matches fn
        |> Seq.cast<Match>
        |> Seq.collect(fun m ->
            seq { yield! m.Groups.["Brackets"].Captures |> Seq.cast<Capture>
                  yield! m.Groups.["NoBrackets"].Captures |> Seq.cast<Capture> }
        )
        |> Seq.map (fun c -> c.Value)
        |> Seq.toArray

    /// Tries to find an in-line commented type annotation in a computed table column.
    let parseTableColumnAnnotation colName colExpression =
        let m = colPattern.Match colExpression
        if m.Success then
            ValueSome { Column = colName
                        DataType = m.Groups.["DataType"].Captures.[0].Value
                        Nullability = m.Groups.["Nullability"].Captures |> Seq.cast<Capture> |> Seq.tryHead |> (function Some x -> ValueSome x | None -> ValueNone) |> ValueOption.map (fun c -> c.Value) }
        else ValueNone

    /// Tries to find in-line commented type annotations in a view declaration.
    let parseViewAnnotations sql =
        viewPattern.Matches sql
        |> Seq.cast<Match>
        |> Seq.map (fun m ->
            { Column = m.Groups.["Column"].Captures.[0].Value
              DataType = m.Groups.["DataType"].Captures.[0].Value
              Nullability = m.Groups.["Nullability"].Captures |> Seq.cast<Capture> |> Seq.tryHead |> (function Some x -> ValueSome x | None -> ValueNone) |> ValueOption.map (fun c -> c.Value) }
        )
        |> Seq.toArray
    
/// Extracts model.xml from the given .dacpac file path.
let extractModelXml (dacPacPath: string) = 
    use stream = new IO.FileStream(dacPacPath, IO.FileMode.Open, IO.FileAccess.Read)
    use zip = new ZipArchive(stream, ZipArchiveMode.Read, false)
    let modelEntry = zip.GetEntry("model.xml")
    use modelStream = modelEntry.Open()
    use rdr = new IO.StreamReader(modelStream)
    rdr.ReadToEnd()

/// Returns a doc and node/nodes ns helper fns
let toXmlNamespaceDoc ns xml =
    let doc = XmlDocument()
    let nsMgr = XmlNamespaceManager(doc.NameTable)
    nsMgr.AddNamespace("x", ns)
    doc.LoadXml(xml)

    let node (path: string) (node: XmlNode) =
        node.SelectSingleNode(path, nsMgr)

    let nodes (path: string) (node: XmlNode) =
        node.SelectNodes(path, nsMgr) |> Seq.cast<XmlNode>
            
    doc, node, nodes    

let attMaybe (nm: string) (node: XmlNode) = 
    node.Attributes 
    |> Seq.cast<XmlAttribute> 
    |> Seq.tryFind (fun a -> a.Name = nm) 
    |> Option.map (fun a -> a.Value) 

let att (nm: string) (node: XmlNode) = 
    attMaybe nm node |> Option.defaultValue ""

/// Parses the xml that is extracted from a .dacpac file.
let parseXml(xml: string) =
    let removeBrackets (s: string) = s.Replace("[", "").Replace("]", "")

    let doc, node, nodes = xml |> toXmlNamespaceDoc "http://schemas.microsoft.com/sqlserver/dac/Serialization/2012/02"
    let model = doc :> XmlNode |> node "/x:DataSchemaModel/x:Model"

    let parsePrimaryKeyConstraint (pkElement: XmlNode) =
        let name = pkElement |> att "Name"
        let relationship = pkElement |> nodes "x:Relationship" |> Seq.find (fun r -> r |> att "Name" = "ColumnSpecifications")
        let columns =
            relationship
            |> nodes "x:Entry"
            |> Seq.map (
                node "x:Element/x:Relationship/x:Entry/x:References"
                    >> att "Name" 
                    >> fun fnm -> { ConstraintColumn.FullName = fnm; Name = fnm |> RegexParsers.splitFullName |> Array.last })
            |> Seq.toArray
        { PrimaryKeyConstraint.Name = name
          PrimaryKeyConstraint.Columns = columns }

    let pkConstraintsByColumn =
        model
        |> nodes "x:Element"
        |> Seq.filter (fun e -> e |> att "Type" = "SqlPrimaryKeyConstraint")
        |> Seq.map parsePrimaryKeyConstraint
        |> Seq.collect (fun pk -> pk.Columns |> Array.map (fun col -> col, pk))
        |> Seq.toArray

    let parseFkRelationship (fkElement: XmlNode) =
        let name = fkElement |> att "Name"
        let localColumns = fkElement |> nodes "x:Relationship" |> Seq.find(fun r -> r |> att "Name" = "Columns") |> nodes "x:Entry/x:References" |> Seq.map (att "Name")
        let localTable = fkElement |> nodes "x:Relationship" |> Seq.find(fun r -> r |> att "Name" = "DefiningTable") |> node "x:Entry/x:References" |> att "Name"
        let foreignColumns = fkElement |> nodes "x:Relationship" |> Seq.find(fun r -> r |> att "Name" = "ForeignColumns") |> nodes "x:Entry/x:References" |> Seq.map (att "Name")
        let foreignTable = fkElement |> nodes "x:Relationship" |> Seq.find(fun r -> r |> att "Name" = "ForeignTable") |> node "x:Entry/x:References" |> att "Name"
        { SsdtRelationship.Name = name
          SsdtRelationship.DefiningTable =
            let parts = localTable |> RegexParsers.splitFullName
            { RefTable.FullName = localTable
              RefTable.Schema = match parts with | [|schema;name|] -> schema | _ -> ""
              RefTable.Name = match parts with | [|schema;name|] -> name | _ -> "" 
              RefTable.Columns = 
                localColumns
                |> Seq.map (fun fnm -> { ConstraintColumn.FullName = fnm; Name = fnm |> RegexParsers.splitFullName |> Array.last })
                |> Seq.toArray }
          SsdtRelationship.ForeignTable =
            let parts = foreignTable |> RegexParsers.splitFullName
            { RefTable.FullName = foreignTable
              RefTable.Schema = match parts with | [|schema;name|] -> schema | _ -> ""
              RefTable.Name = match parts with | [|schema;name|] -> name | _ -> "" 
              RefTable.Columns =
                foreignColumns
                |> Seq.map (fun fnm -> { ConstraintColumn.FullName = fnm; Name = fnm |> RegexParsers.splitFullName |> Array.last })
                |> Seq.toArray }
        }

    let relationships =
        model
        |> nodes "x:Element"
        |> Seq.filter (fun e -> e |> att "Type" = "SqlForeignKeyConstraint")
        |> Seq.map parseFkRelationship
        |> Seq.toArray
        
    let parseTableColumn (colEntry: XmlNode) =
        let el = colEntry |> node "x:Element"
        let colType, fullName = el |> att "Type", el |> att "Name"
        let colName = fullName |> RegexParsers.splitFullName |> Array.last
        match colType with
        | "SqlSimpleColumn" -> 
            let allowNulls = el |> nodes "x:Property" |> Seq.tryFind (fun p -> p |> att "Name" = "IsNullable") |> Option.map (fun p -> p |> att "Value")
            let isIdentity = el |> nodes "x:Property" |> Seq.tryFind (fun p -> p |> att "Name" = "IsIdentity") |> Option.map (fun p -> p |> att "Value")
            let dataType = el |> node "x:Relationship/x:Entry/x:Element/x:Relationship/x:Entry/x:References" |> att "Name"
            Some
                { SsdtColumn.Name = colName
                  SsdtColumn.FullName = fullName
                  SsdtColumn.AllowNulls = match allowNulls with | Some allowNulls -> allowNulls = "True" | _ -> true
                  SsdtColumn.DataType = dataType |> removeBrackets
                  SsdtColumn.HasDefault = false
                  SsdtColumn.Description = "Simple Column"
                  SsdtColumn.IsIdentity = isIdentity |> Option.map (fun isId -> isId = "True") |> Option.defaultValue false
                  SsdtColumn.ComputedColumn = false}
        | "SqlComputedColumn" ->
            // Check for annotation
            let colExpr = (el |> node "x:Property/x:Value").InnerText
            let annotation = RegexParsers.parseTableColumnAnnotation colName colExpr
            let dataType =
                annotation
                |> ValueOption.map (fun a -> a.DataType.ToUpper()) // Ucase to match typeMappings
                |> ValueOption.defaultValue "SQL_VARIANT"
            let allowNulls =
                match annotation with
                | ValueSome { Nullability = ValueSome nlb } -> nlb.ToUpper() = "NULL"
                | ValueSome { Nullability = ValueNone } -> true // Sql Server column declarations allow nulls by default
                | ValueNone -> false // Default to "SQL_VARIANT" (obj) with no nulls if annotation is not found
            
            Some
                { SsdtColumn.Name = colName
                  SsdtColumn.FullName = fullName
                  SsdtColumn.AllowNulls = allowNulls
                  SsdtColumn.DataType = dataType
                  SsdtColumn.HasDefault = false
                  SsdtColumn.Description =
                    "Computed Column" +
                        if annotation.IsNone && dataType = "SQL_VARIANT"
                        then ". You can add type annotation to definition SQL to get type. E.g. " + colName + " AS ('c' /* varchar not null */)"
                        else ""
                  SsdtColumn.IsIdentity = false
                  SsdtColumn.ComputedColumn = true}
        | _ ->
            None // Unsupported column type

    let parseTable (tblElement: XmlNode) =
        let fullName = tblElement |> att "Name"
        let relationship = tblElement |> nodes "x:Relationship" |> Seq.find (fun r -> r |> att "Name" = "Columns")
        let columns = relationship |> nodes "x:Entry" |> Seq.choose parseTableColumn |> Seq.toArray
        let nameParts = fullName |> RegexParsers.splitFullName
        let primaryKey =
            columns
            |> Array.choose(fun c -> pkConstraintsByColumn |> Array.tryFind(fun (colRef, pk) -> colRef.FullName = c.FullName))
            |> Array.tryHead
            |> (function Some x -> ValueSome x | None -> ValueNone)
            |> ValueOption.map snd
        { Schema = match nameParts with | [|schema;name|] -> schema | _ -> failwithf "Unable to parse table '%s' schema." fullName
          Name = match nameParts with | [|schema;name|] -> name | [|name|] -> name | _ -> failwithf "Unable to parse table '%s' name." fullName
          FullName = fullName
          Columns = columns
          IsView = false
          PrimaryKey = primaryKey } : SsdtTable

    let parseViewColumn (colEntry:  XmlNode) =
        let colFullNm = colEntry |> node "x:Element" |> att "Name"
        let typeRelation = colEntry |> node "x:Element" |> node "x:Relationship" |> ValueOption.ofObj
        let colRefPath = typeRelation |> ValueOption.map (node "x:Entry/x:References" >> att "Name")
        { SsdtViewColumn.FullName = colFullNm
          SsdtViewColumn.ColumnRefPath = colRefPath }

    /// Recursively collections view column refs from any nested 'DynamicObjects' (ex: CTEs).
    let collectDynamicColumnRefs (viewElement: XmlNode) =
        let rec recurse (columns: SsdtViewColumn array) (el: XmlNode)  =
            let relationshipColumns = el |> nodes "x:Relationship" |> Seq.tryFind (fun r -> r |> att "Name" = "Columns")
            let relationshipDynamicObjects = el |> nodes "x:Relationship" |> Seq.tryFind (fun r -> r |> att "Name" = "DynamicObjects")
            let cols = relationshipColumns |> Option.map (nodes "x:Entry" >> Seq.map parseViewColumn) |> Option.defaultValue Seq.empty |> Seq.toArray
            let accumulatedColumns = Array.concat [|columns; cols|]
            match relationshipDynamicObjects with
            | Some rel -> rel |> nodes "x:Entry" |> Seq.map (node "x:Element") |> Seq.collect (recurse accumulatedColumns) |> Seq.toArray
            | None -> accumulatedColumns

        recurse [||] viewElement

    let parseView (viewElement: XmlNode) =
        let fullName = viewElement |> att "Name"
        let relationshipColumns = viewElement |> nodes "x:Relationship" |> Seq.find (fun r -> r |> att "Name" = "Columns")
        let columns = relationshipColumns |> nodes "x:Entry" |> Seq.map parseViewColumn
        let dynamicColumns = collectDynamicColumnRefs viewElement
        let query = (viewElement |> nodes "x:Property" |> Seq.find (fun n -> n |> att "Name" = "QueryScript") |> node "x:Value").InnerText
        let annotations = RegexParsers.parseViewAnnotations query

        let nameParts = fullName |> RegexParsers.splitFullName
        { FullName = fullName
          Schema = match nameParts with | [|schema;name|] -> schema | _ -> failwithf "Unable to parse view '%s' schema." fullName
          Name = match nameParts with | [|schema;name|] -> name | _ -> failwithf "Unable to parse view '%s' name." fullName
          Columns = columns |> Seq.toArray
          DynamicColumns = dynamicColumns
          Annotations = annotations } : SsdtView

    /// Recursively resolves column references.
    let resolveColumnRefPath (tableColumnsByPath: Map<string, SsdtColumn>) (viewColumnsByPath: Map<string, SsdtViewColumn>) (viewCol: SsdtViewColumn) =
        let rec resolve (path: string) =
            match tableColumnsByPath.TryFind(path) with
            | Some tblCol ->
                { tblCol with
                    FullName = viewCol.FullName
                    Name = viewCol.FullName |> RegexParsers.splitFullName |> Array.last } |> Some
            | None -> 
                match viewColumnsByPath.TryFind(path) with
                | Some viewCol when viewCol.ColumnRefPath <> ValueSome path ->
                    match viewCol.ColumnRefPath with
                    | ValueSome colRefPath -> resolve colRefPath
                    | ValueNone -> None
                | _ -> None

        match viewCol.ColumnRefPath with
        | ValueSome path -> resolve path
        | ValueNone -> None

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
                      Name = pFullName |> RegexParsers.splitFullName |> Array.last
                      DataType = dataType |> removeBrackets
                      Length = ValueNone // TODO: Implement
                      IsOutput = isOutput }
                )
            | None -> Seq.empty
        let returnDataType =
            match spElement |> nodes "x:Relationship" |> Seq.tryFind (fun r -> r |> att "Name" = "Type") with
            | None -> ValueNone
            | Some datatypeinfo ->
                let dataType = datatypeinfo |> node "x:Entry/x:Element/x:Relationship/x:Entry/x:References" |> att "Name"
                if String.IsNullOrEmpty dataType then ValueNone
                else ValueSome (dataType |> removeBrackets)

        let parts = fullName |> RegexParsers.splitFullName
        { FullName = fullName
          Schema = parts.[0]
          Name = parts.[1]
          Parameters = parameters |> Seq.toArray
          ReturnValueDataType = returnDataType }

    let parseDescription (extElement: XmlNode) =
        let fullName = extElement |> att "Name"
        let parts = fullName |> RegexParsers.splitFullName
        let description = (extElement |> nodes "x:Property" |> Seq.find (fun n -> n |> att "Name" = "Value") |> node "x:Value").InnerText
        {   // Mostly interesting decription types table/view/column: SqlTableBase / SqlView / SqlColumn
            // But there can be many others too: SqlSchema / SqlDmlTrigger / SqlConstraint / SqlDatabaseOptions / SqlFilegroup / SqlSubroutineParameter / SqlXmlSchemaCollection / ...
            DecriptionType = parts.[0]
            Schema = parts.[1]
            TableName = if parts.Length > 2 then parts.[2] else ""
            ColumnName = if parts.Length > 3 && parts.[0] <> "SqlTableBase" && parts.[3] <> "MS_Description" then ValueSome parts.[3] else ValueNone
            Description = description
        }

    let parseUserDefinedDataType (uddts: XmlNode) =
        let name = uddts |> att "Name"
        let name = name |> RegexParsers.splitFullName |> fun parsed -> (String.concat "." parsed).ToUpper() |> UDDTName
        let inheritedType =
            uddts
            |> nodes "x:Relationship"
            |> Seq.find (fun n -> n |> att "Name" = "Type")
            |> node "x:Entry/x:References"
            |> att "Name"
            |> RegexParsers.splitFullName
            |> fun parsed -> String.concat "." parsed
            |> fun t -> t.ToUpper()
            |> UDDTInheritedType
        (name, inheritedType)

    let userDefinedDataTypes =
        model
        |> nodes "x:Element"
        |> Seq.filter (fun e -> e |> att "Type" = "SqlUserDefinedDataType")
        |> Seq.map parseUserDefinedDataType
        |> Map.ofSeq

    let functions =
        let filterPredicate elem =
            (att "Type" elem) = "SqlScalarFunction"
            || (att "Type" elem) = "SqlInlineTableValuedFunction"
        model
        |> nodes "x:Element"
        |> Seq.filter filterPredicate
        |> Seq.map parseStoredProc
        |> Seq.toArray

    let storedProcs =
        model
        |> nodes "x:Element"
        |> Seq.filter (fun e -> e |> att "Type" = "SqlProcedure")
        |> Seq.map parseStoredProc
        |> Seq.toArray

    let tables = 
        model
        |> nodes "x:Element"
        |> Seq.filter (fun e -> e |> att "Type" = "SqlTable")
        |> Seq.map parseTable
        |> Seq.toArray

    let views =
        model
        |> nodes "x:Element"
        |> Seq.filter (fun e -> e |> att "Type" = "SqlView")
        |> Seq.map parseView
        |> Seq.toArray

    let descriptions =
        model
        |> nodes "x:Element"
        |> Seq.filter (fun e -> e |> att "Type" = "SqlExtendedProperty" && (e |> att "Name").EndsWith(".[MS_Description]"))
        |> Seq.map parseDescription
        |> Seq.toArray

    let tableColumnsByPath = tables |> Array.collect (fun t -> t.Columns) |> Array.map (fun c -> c.FullName, c) |> Map.ofArray
    let viewColumnsByPath = views |> Array.collect (fun v -> Array.concat [| v.Columns ; v.DynamicColumns |]) |> Array.map (fun c -> c.FullName, c) |> Map.ofArray
    let resolveColumnRefPath = resolveColumnRefPath tableColumnsByPath viewColumnsByPath

    let viewToTable (view: SsdtView) =
        { FullName = view.FullName
          Name = view.Name
          Schema = view.Schema
          IsView = true
          PrimaryKey = ValueNone
          Columns =
            view.Columns
            |> Array.map (fun vc ->
                let colName = vc.FullName |> RegexParsers.splitFullName |> Array.last
                let annotation = view.Annotations |> Array.tryFind (fun a -> a.Column = colName)
                match resolveColumnRefPath vc with
                | Some tc when annotation.IsNone -> tc
                | tcOpt ->
                    // Can't resolve column, or annotation override: try to find a commented type annotation
                    let dataType =
                        annotation
                        |> Option.map (fun a -> a.DataType.ToUpper()) // Ucase to match typeMappings
                        |> Option.defaultValue "SQL_VARIANT"
                    let allowNulls =
                        match annotation with
                        | Some { Nullability = ValueSome nlb } -> nlb.ToUpper() = "NULL"
                        | Some { Nullability = ValueNone } -> true // Sql Server column declarations allow nulls by default
                        | None -> false // Default to "SQL_VARIANT" (obj) with no nulls if annotation is not found
                    let description =
                        if dataType = "SQL_VARIANT"
                        then sprintf "Unable to resolve this column's data type from the .dacpac file; consider adding a type annotation in the view. Ex: %s /* varchar not null */ " colName
                        else "This column's data type was resolved from a comment annotation in the SSDT view definition."

                    if dataType = "SQL_VARIANT" && tcOpt.IsSome then tcOpt.Value else
                    { FullName = vc.FullName
                      Name = colName
                      Description = description
                      DataType = dataType
                      AllowNulls = allowNulls
                      HasDefault = false
                      IsIdentity = false
                      ComputedColumn = true } : SsdtColumn
            )
        } : SsdtTable

    let tablesAndViews = Array.concat [| tables ; (views |> Array.map viewToTable) |] 
    let tablesDict = tablesAndViews |> Array.distinctBy(fun t -> t.Name) |> Array.map(fun t -> t.Name, t) |> dict
    let definingRelations = relationships |> Array.groupBy(fun r -> r.DefiningTable.Name) |> Map.ofArray
    let foreignRelations = relationships |> Array.groupBy(fun r -> r.ForeignTable.Name) |> Map.ofArray

    { Tables = tablesAndViews
      StoredProcs = storedProcs
      TryGetTableByName = fun nm -> match tablesDict.TryGetValue nm with | true, v -> ValueSome v | false, _ -> ValueNone
      Relationships = relationships
      TryGetRelationshipsByTableName = fun nm ->
            (match definingRelations.TryGetValue nm with | true, v -> v | false, _ -> Array.empty),
            (match foreignRelations.TryGetValue nm with | true, v -> v | false, _ -> Array.empty)
      Descriptions = descriptions
      UserDefinedDataTypes = userDefinedDataTypes
      Functions = functions } : SsdtSchema
