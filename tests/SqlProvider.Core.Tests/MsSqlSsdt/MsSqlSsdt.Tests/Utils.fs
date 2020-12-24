module Utils
open NUnit.Framework
open Microsoft.SqlServer.Management.SqlParser.Parser
open FSharp.Data.Sql.Providers

let sqlToSchemaXml (sql: string) = (Parser.Parse(sql)).Script.Xml
let xmlToTableModel = sqlToSchemaXml >> MSSqlServerSsdt.parseTableSchemaXml
let printSchemaXml = sqlToSchemaXml >> printfn "%s"
let printTableModel = sqlToSchemaXml >> MSSqlServerSsdt.parseTableSchemaXml >> printfn "%A"
let printStoredProcModel = sqlToSchemaXml >> MSSqlServerSsdt.parseStoredProcSchemaXml >> printfn "%A"

