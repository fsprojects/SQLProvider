module FSharp.Data.Sql.SchemaProjections

let buildTableName (tableName:string) = tableName.Substring(0,tableName.LastIndexOf("]")+1)

let buildFieldName (fieldName:string) = fieldName