module Dacpac.Utils
open System
open System.Xml

/// Returns a doc and node/nodes ns helper fns
let toXmlNamespaceDoc ns xml =
    let doc = new XmlDocument()
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

let splitAndRemoveBrackets (s: string) =
    s.Split([|'.';'[';']'|], StringSplitOptions.RemoveEmptyEntries)

type SsdtSchema = {
    Tables: SsdtTable list
    TryGetTableByName: string -> SsdtTable option
    StoredProcs: SsdtStoredProc list
    Relationships: SsdtRelationship list
}
and SsdtTable = {
    FullName: string
    Schema: string
    Name: string
    Columns: SsdtColumn list
    PrimaryKey: PrimaryKeyConstraint option
    IsView: bool
}
and SsdtColumn = {
    FullName: string
    Name: string
    DataType: string
    AllowNulls: bool
    IsIdentity: bool
    HasDefault: bool
}
and SsdtView = {
    FullName: string
    Schema: string
    Name: string
    Columns: SsdtViewColumn list
    DynamicColumns: SsdtViewColumn list
}
and SsdtViewColumn = {
    FullName: string
    ColumnRefPath: string option
}
and SsdtRelationship = {
    Name: string
    DefiningTable: RefTable
    ForeignTable: RefTable
}
and RefTable = {
    FullName: string
    Columns: ConstraintColumn list
} with        
    member this.Schema = match this.FullName |> splitAndRemoveBrackets with | [|schema;name|] -> schema | _ -> ""
    member this.Name = match this.FullName |> splitAndRemoveBrackets with | [|schema;name|] -> name | _ -> ""

and PrimaryKeyConstraint = {
    Name: string
    Columns: ConstraintColumn list
}
and ConstraintColumn = {
    FullName: string
} with
   member this.Name = this.FullName |> splitAndRemoveBrackets |> Array.last
and SsdtStoredProc = {
    FullName: string
    Schema: string
    Name: string
    Parameters: SsdtStoredProcParam list
}
and SsdtStoredProcParam = {
    FullName: string
    Name: string
    DataType: string
    Length: int option
    IsOutput: bool
}
