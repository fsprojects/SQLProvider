module Dacpac.Utils
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

type SsdtSchema = {
    Tables: SsdtTable list
    TryGetTableByName: string -> SsdtTable option
    StoredProcs: SsdtStoredProc list
}
and SsdtTable = {
    Schema: string
    Name: string
    Columns: SsdtColumn list
    PrimaryKey: PrimaryKeyConstraint option
    ForeignKeys: ForeignKeyConstraint list
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
and ForeignKeyConstraint = {
    Name: string
    Columns: string list
    References: RefTable
}
and RefTable = {
    Schema: string
    Table: string
    Columns: string list
}
and PrimaryKeyConstraint = {
    Name: string
    Columns: PrimaryKeyColumnRef list
}
and PrimaryKeyColumnRef = {
    Name: string
    FullName: string
}
and SsdtViewColumn = {
    Name: string
    Alias: string option
    RefCol: SsdtColumn
}
and SsdtStoredProc = {
    Schema: string
    Name: string
    Parameters: SsdtStoredProcParam list
}
and SsdtStoredProcParam = {
    Name: string
    DataType: string
    Length: int option
    IsOutput: bool
}
