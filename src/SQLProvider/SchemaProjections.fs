module FSharp.Data.Sql.SchemaProjections

    let buildTableName (tableName:string) = tableName.Substring(0,tableName.LastIndexOf("]")+1).ToUpper()

    let buildFieldName (fieldName:string) = fieldName.ToUpper()

    let buildSprocName (sprocName:string) = sprocName.ToUpper()