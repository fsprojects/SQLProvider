/// <summary>
/// Module for parsing SQL Server Data Tools (SSDT) .dacpac files to extract database schema information.
/// Provides functionality to read and parse the model.xml file contained within .dacpac packages.
/// </summary>
module FSharp.Data.Sql.Ssdt.DacpacParser

open System
open System.Xml
open System.IO.Compression

/// <summary>Represents a database table column from SSDT schema.</summary>
type [<Struct>] SsdtColumn = {
    /// The fully qualified column name (schema.table.column)
    FullName: string
    /// The column name without qualification
    Name: string
    /// Extended description/comment for the column
    Description: string
    /// The SQL data type (e.g., varchar, int, datetime)
    DataType: string
    /// True if the column allows NULL values
    AllowNulls: bool
    /// True if the column is an identity/auto-increment column
    IsIdentity: bool
    /// True if the column has a default value constraint
    HasDefault: bool
    /// True if the column is computed/calculated
    ComputedColumn: bool
}

/// <summary>Represents a view column from SSDT schema.</summary>
type [<Struct>] SsdtViewColumn = {
    /// The fully qualified column name
    FullName: string
    /// Optional reference path for complex column definitions
    ColumnRefPath: string voption
}

/// <summary>Represents a comment annotation for schema documentation.</summary>
type [<Struct>] CommentAnnotation = {
    /// The column name the annotation applies to
    Column: string
    /// The data type information
    DataType: string
    /// Optional nullability information
    Nullability: string voption
}

/// <summary>Represents a database view from SSDT schema.</summary>
type SsdtView = {
    /// The fully qualified view name (schema.view)
    FullName: string
    /// The schema name
    Schema: string
    /// The view name without schema
    Name: string
    /// Array of regular columns in the view
    Columns: SsdtViewColumn array
    /// Array of dynamically generated columns
    DynamicColumns: SsdtViewColumn array
    /// Array of comment annotations for documentation
    Annotations: CommentAnnotation array
}

/// <summary>Represents a column in a constraint definition.</summary>
type [<Struct>] ConstraintColumn = {
    /// The fully qualified column name
    FullName: string
    /// The column name without qualification
    Name: string
}

/// <summary>Represents a table reference in relationship definitions.</summary>
type RefTable = {
    /// The fully qualified table name
    FullName: string
    /// The schema name
    Schema: string
    /// The table name without schema
    Name: string
    /// Array of columns involved in the reference
    Columns: ConstraintColumn array
}

/// <summary>Represents a foreign key relationship between tables.</summary>
type SsdtRelationship = {
    /// The name of the relationship/constraint
    Name: string
    /// The table that defines the primary key
    DefiningTable: RefTable
    /// The table that contains the foreign key
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
    
/// <summary>
/// Extracts the model.xml file from a SQL Server Data Tools (SSDT) .dacpac package.
/// The model.xml contains the complete database schema definition.
/// </summary>
/// <param name="dacPacPath">Path to the .dacpac file</param>
/// <returns>The XML content of the model.xml file as a string</returns>
let extractModelXml (dacPacPath: string) = 
    use stream = new IO.FileStream(dacPacPath, IO.FileMode.Open, IO.FileAccess.Read)
    use zip = new ZipArchive(stream, ZipArchiveMode.Read, false)
    let modelEntry = zip.GetEntry("model.xml")
    use modelStream = modelEntry.Open()
    use rdr = new IO.StreamReader(modelStream)
    rdr.ReadToEnd()

/// <summary>
/// Creates an XML document with namespace support and returns helper functions for node selection.
/// This is specifically designed for parsing SSDT model.xml files with their Microsoft namespace.
/// </summary>
/// <param name="ns">The XML namespace URI</param>
/// <param name="xml">The XML content to parse</param>
/// <returns>A tuple containing (XmlDocument, single node selector function, multiple nodes selector function)</returns>
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

/// <summary>
/// Safely extracts an XML attribute value from a node, returning None if the attribute doesn't exist.
/// </summary>
/// <param name="nm">The attribute name to extract</param>
/// <param name="node">The XML node to extract from</param>
/// <returns>Some(attribute value) if found, None otherwise</returns>
let attMaybe (nm: string) (node: XmlNode) =
    if isNull node.Attributes then None
    else
    node.Attributes 
    |> Seq.cast<XmlAttribute> 
    |> Seq.tryFind (fun a -> a.Name = nm) 
    |> Option.map (fun a -> a.Value) 

/// <summary>
/// Extracts an XML attribute value from a node, returning an empty string if the attribute doesn't exist.
/// This is a convenience function for cases where a default empty value is acceptable.
/// </summary>
/// <param name="nm">The attribute name to extract</param>
/// <param name="node">The XML node to extract from</param>
/// <returns>The attribute value, or empty string if not found</returns>
let att (nm: string) (node: XmlNode) = 
    attMaybe nm node |> Option.defaultValue ""

/// <summary>
/// Parses the model.xml content extracted from a SQL Server Data Tools (SSDT) .dacpac file.
/// Extracts database schema information including tables, views, columns, relationships, and stored procedures.
/// </summary>
/// <param name="xml">The XML content from the model.xml file</param>
/// <returns>A parsed representation of the database schema</returns>
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
                |> ValueOption.map (fun a -> a.DataType.ToUpperInvariant()) // Ucase to match typeMappings
                |> ValueOption.defaultValue "SQL_VARIANT"
            let allowNulls =
                match annotation with
                | ValueSome { Nullability = ValueSome nlb } -> nlb.ToUpperInvariant() = "NULL"
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
        let name = name |> RegexParsers.splitFullName |> fun parsed -> (String.concat "." parsed).ToUpperInvariant() |> UDDTName
        let inheritedType =
            uddts
            |> nodes "x:Relationship"
            |> Seq.find (fun n -> n |> att "Name" = "Type")
            |> node "x:Entry/x:References"
            |> att "Name"
            |> RegexParsers.splitFullName
            |> fun parsed -> String.concat "." parsed
            |> fun t -> t.ToUpperInvariant()
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
                        |> Option.map (fun a -> a.DataType.ToUpperInvariant()) // Ucase to match typeMappings
                        |> Option.defaultValue "SQL_VARIANT"
                    let allowNulls =
                        match annotation with
                        | Some { Nullability = ValueSome nlb } -> nlb.ToUpperInvariant() = "NULL"
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
    let tablesDict = tablesAndViews |> Array.groupBy(fun t -> t.Name) |> Map.ofArray
    let definingRelations = relationships |> Array.groupBy(fun r -> r.DefiningTable.Name) |> Map.ofArray
    let foreignRelations = relationships |> Array.groupBy(fun r -> r.ForeignTable.Name) |> Map.ofArray

    let inline findByName (tableName:string) (schemaFilter: string -> _ -> bool) (mappedData:Map<string,_>) =
        if tableName.Contains "." then
            let schema = tableName.Substring(0, tableName.LastIndexOf '.')
            let name = tableName.Substring(tableName.LastIndexOf '.'+1)
            match mappedData.TryGetValue name with
            | true, foundItems -> foundItems |> Array.filter (schemaFilter schema)
            | false, _ -> Array.empty
        else
            match mappedData.TryGetValue tableName with
            | true, foundItems -> foundItems
            | false, _ -> Array.empty

    { Tables = tablesAndViews
      StoredProcs = storedProcs
      TryGetTableByName = fun tableName ->
            let inline schemaFilter schema tbl = tbl.Schema = schema
            match tablesDict |> findByName tableName schemaFilter with
            | [| v |] -> ValueSome v
            | _ -> ValueNone
      Relationships = relationships
      TryGetRelationshipsByTableName = fun tableName ->
            let inline definingFilter schema rl = rl.DefiningTable.Schema = schema
            let inline foreignFilter schema rl = rl.ForeignTable.Schema = schema
            definingRelations  |> findByName tableName definingFilter,
            foreignRelations  |> findByName tableName foreignFilter
      Descriptions = descriptions
      UserDefinedDataTypes = userDefinedDataTypes
      Functions = functions } : SsdtSchema
