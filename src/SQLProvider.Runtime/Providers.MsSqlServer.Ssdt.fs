// fsharplint:disable Hints CanBeReplacedWithComposition
namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Concurrent
open System.Data
#if MICROSOFTSQL
open Microsoft.Data.SqlClient
#else
open System.Data.SqlClient
#endif
open System.IO
open System.Reflection
open System.Text
open FSharp.Data.Sql
open FSharp.Data.Sql.Transactions
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Ssdt.DacpacParser

module MSSqlServerSsdt =

    [<Literal>]
    let DACPAC_SEARCH_PATH_ENV_VAR_NAME = "SQLPROVIDER_SSDT_DACPAC_FILE_LOOKUP_PATH"

    /// Tries to find .dacpac file using the given path at design time or by searching the runtime assembly path.
    let findDacPacFile (dacPacPath: string) =

        let fileInfoOpt path =
            try
                FileInfo path |> Some
            with e -> None

        // Find at design time using SsdtPath
        let ssdtFile = IO.FileInfo(dacPacPath)
        let dacPacFileName = ssdtFile.Name
        let origPath =
            (Option.ofObj ssdtFile.Directory)
            |> Option.map (fun d -> d.FullName)
            |> Option.defaultValue "."

        let asmToPath (asm:Assembly) =
            Option.ofObj asm
            |> Option.bind (fun a -> fileInfoOpt a.Location)
            |> Option.map (fun d -> d.Directory.FullName)

        /// generate many combinations to try.
        let paths =
            [
                // working dir
                yield Environment.CurrentDirectory
                yield Path.Combine(Environment.CurrentDirectory, origPath)
                // entry asm dir
                match asmToPath (Assembly.GetEntryAssembly()) with
                | Some p ->
                    yield p
                    yield Path.Combine(p, origPath)
                | _ -> ()
                // executing assembly dir
                match asmToPath (Assembly.GetExecutingAssembly()) with
                | Some p ->
                    yield p
                    yield Path.Combine(p, origPath)
                | _ -> ()
            ]
            |> List.map Path.GetFullPath // sort out the trailing slashes situation
            |> List.distinct

        let chooseDacpac dirPath =
            if String.IsNullOrWhiteSpace dirPath then
                None
            else
                try
                    Path.Combine(dirPath, dacPacFileName)
                    |> fileInfoOpt
                with
                | :? UnauthorizedAccessException -> None


        // also read the special environment variable
        let envVarPath = Environment.GetEnvironmentVariable(DACPAC_SEARCH_PATH_ENV_VAR_NAME)
        let fromEnvVar =
            if String.IsNullOrWhiteSpace envVarPath then
                []
            else
                let envFilePath = fileInfoOpt envVarPath
                let relativeEnvFilePath =
                    envFilePath |> Option.bind (fun d -> chooseDacpac d.Directory.FullName)
                [
                    envFilePath
                    relativeEnvFilePath
                ] |> List.choose id



        let allPossiblePaths =
            paths
            |> List.choose chooseDacpac
            |> List.distinct
            |> List.append fromEnvVar

        let bestOption = allPossiblePaths |> List.tryFind (fun fi -> fi.Exists)

        match bestOption with
        | Some b -> b.FullName
        | None ->
            let sb = new StringBuilder()
            sb.AppendLine(sprintf "Unable to find .dacpac file. Search path includes executing assembly, configured ssd path, entry assembly, and the environment variable '%s'." DACPAC_SEARCH_PATH_ENV_VAR_NAME) |> ignore
            sb.AppendLine("Looked in:") |> ignore
            for s in allPossiblePaths do
                sb.Append("\t") |> ignore
                sb.AppendLine(s.FullName) |> ignore
            failwith (sb.ToString())


    /// Tries to parse a schema model from the given .dacpac file path.
    let parseDacpac = findDacPacFile >> extractModelXml >> parseXml

    let typeMappingsByName =
        let toInt = int >> ValueSome
        // https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-data-type-mappings
        [ "UNIQUEIDENTIFIER", "System.Guid", DbType.Guid, toInt SqlDbType.UniqueIdentifier
          "BIT", "System.Boolean", DbType.Boolean, toInt SqlDbType.Bit
          "INT", "System.Int32", DbType.Int32, toInt SqlDbType.Int
          "BIGINT", "System.Int64", DbType.Int64, toInt SqlDbType.BigInt
          "SMALLINT", "System.Int16", DbType.Int16, toInt SqlDbType.SmallInt
          "TINYINT", "System.Byte", DbType.Byte, toInt SqlDbType.TinyInt
          "FLOAT", "System.Double", DbType.Double, toInt SqlDbType.Float
          "REAL", "System.Single", DbType.Single, toInt SqlDbType.Real
          "DECIMAL", "System.Decimal", DbType.Decimal, toInt SqlDbType.Decimal
          "NUMERIC", "System.Decimal", DbType.Decimal, toInt SqlDbType.Decimal
          "MONEY", "System.Decimal", DbType.Decimal, toInt SqlDbType.Money
          "SMALLMONEY", "System.Decimal", DbType.Decimal, toInt SqlDbType.SmallMoney
          "VARCHAR", "System.String", DbType.String, toInt SqlDbType.VarChar
          "NVARCHAR", "System.String", DbType.String, toInt SqlDbType.NVarChar
          "CHAR", "System.String", DbType.String, toInt SqlDbType.Char
          "NCHAR", "System.String", DbType.StringFixedLength, toInt SqlDbType.NChar
          "TEXT", "System.String", DbType.String, toInt SqlDbType.Text
          "NTEXT", "System.String", DbType.String, toInt SqlDbType.NText
          "DATETIMEOFFSET", "System.DateTimeOffset", DbType.DateTimeOffset, toInt SqlDbType.DateTimeOffset
          "DATE", "System.DateTime", DbType.Date, toInt SqlDbType.Date
          "DATETIME", "System.DateTime", DbType.DateTime, toInt SqlDbType.DateTime
          "DATETIME2", "System.DateTime", DbType.DateTime2, toInt SqlDbType.DateTime2
          "SMALLDATETIME", "System.DateTime", DbType.DateTime, toInt SqlDbType.SmallDateTime
          "TIME", "System.TimeSpan", DbType.Time, toInt SqlDbType.Time
          "VARBINARY", "System.Byte[]", DbType.Binary, toInt SqlDbType.VarBinary
          "BINARY", "System.Byte[]", DbType.Binary, toInt SqlDbType.Binary
          "IMAGE", "System.Byte[]", DbType.Binary, toInt SqlDbType.Image
          "ROWVERSION", "System.Byte[]", DbType.Binary, ValueNone
          "XML", "System.Xml.Linq.XElement", DbType.Xml, toInt SqlDbType.Xml
          "CURSOR", ((typeof<SqlEntity[]>).ToString()), DbType.Object, ValueNone
          "SQL_VARIANT", "System.Object", DbType.Object, toInt SqlDbType.Variant
          "GEOGRAPHY", "Microsoft.SqlServer.Types.SqlGeography", DbType.Object, ValueSome 29
          "GEOMETRY", "Microsoft.SqlServer.Types.SqlGeometry", DbType.Object, ValueSome 29
          "HIERARCHYID", "Microsoft.SqlServer.Types.SqlHierarchyId", DbType.Object, ValueSome 29 ]
        |> List.map (fun (providerTypeName, clrType, dbType, providerType) ->
            providerTypeName,
            { TypeMapping.ProviderTypeName = ValueSome providerTypeName
              TypeMapping.ClrType = clrType
              TypeMapping.DbType = dbType
              TypeMapping.ProviderType = providerType }
        )
        |> Map.ofList

    let tryFindMapping (dataType: string) =
        typeMappingsByName.TryFind (dataType.ToUpper())

    let rec tryFindMappingOrVariant (uddts: SsdtUserDefinedDataType) (dataType: string) =
        let dataType = dataType.ToUpper()
        match typeMappingsByName.TryFind dataType with
        | Some tm -> tm
        | None ->
            match Map.tryFind (UDDTName dataType) uddts with
            | Some (UDDTInheritedType x) -> tryFindMappingOrVariant uddts x
            | None -> typeMappingsByName["SQL_VARIANT"]

    let ssdtTableToTable (tbl: SsdtTable) =
        { Schema = tbl.Schema ; Name = tbl.Name ; Type =  if tbl.IsView then "view" else "base table" }

    let ssdtColumnToColumn uddts (tbl: SsdtTable) (col: SsdtColumn) =
        let typeMapping = tryFindMappingOrVariant uddts col.DataType        
        { Column.Name = col.Name
          Column.TypeMapping = typeMapping
          Column.IsNullable = col.AllowNulls
          Column.IsPrimaryKey =
            tbl.PrimaryKey
            |> ValueOption.map (fun pk -> pk.Columns |> List.exists (fun pkCol -> pkCol.Name = col.Name))
            |> ValueOption.defaultValue false
          Column.IsAutonumber = col.IsIdentity
          Column.HasDefault = col.HasDefault
          Column.IsComputed = col.ComputedColumn
          Column.TypeInfo = if col.DataType = "" then ValueNone else ValueSome col.DataType }

type internal MSSqlServerProviderSsdt(tableNames: string, ssdtPath: string) =
    let schemaCache = SchemaCache.Empty
    let createInsertCommand = MSSqlServer.createInsertCommand schemaCache
    let createUpdateCommand = MSSqlServer.createUpdateCommand schemaCache
    let createDeleteCommand = MSSqlServer.createDeleteCommand schemaCache
    let myLock = new Object()
    // Remembers the version of each instance it connects to
    let mssqlVersionCache = ConcurrentDictionary<string, Lazy<Version>>()

    let ssdtSchema = lazy (MSSqlServerSsdt.parseDacpac ssdtPath)
    let sprocReturnParam i =
        { Name = "ResultSet" + (match i with | 0 -> "" | x -> "_" + x.ToString())
          TypeMapping =
                { TypeMapping.ProviderTypeName = ValueSome "cursor"
                  TypeMapping.ClrType = typeof<SqlEntity[]>.ToString()
                  TypeMapping.DbType = DbType.Object
                  TypeMapping.ProviderType = ValueNone }
          Direction = ParameterDirection.Output
          Length = ValueNone
          Ordinal = i }


    let sprocReturnCache = ConcurrentDictionary<string, QueryParameter[]>()
    /// SSDT dacpac doesn't contain info about return parameters. A little hacky, but also SQL Server efficiently caches the query
    let getSprocReturnParams con sprocDbName inputParameters =
        sprocReturnCache.GetOrAdd(sprocDbName,
            fun _ ->
                MSSqlServer.findDbType <- MSSqlServerSsdt.tryFindMapping
                let r = MSSqlServer.getSprocReturnCols con {ProcName = sprocDbName; PackageName = ""; Owner = ""} inputParameters
                r |> List.toArray)

    interface ISqlProvider with
        member __.GetLockObject() = myLock
        member __.GetTableDescription(con,tableName) =
            tableName +
            (ssdtSchema.Value.Descriptions
             |> List.filter(fun d -> (d.DecriptionType = "SqlTableBase" || d.DecriptionType = "SqlView") && d.ColumnName.IsNone)
             |> List.tryFind(fun d -> if tableName.Contains "." then d.Schema + "." + d.TableName = tableName else d.TableName = tableName)
             |> Option.map (fun d ->
                if String.IsNullOrEmpty d.Description then ""
                elif d.Description.StartsWith("N'") then
                    " / " + d.Description.Substring(0, d.Description.Length-1).Replace("N'", "")
                else " / " + d.Description)
             |> Option.defaultValue ""
             )
        member __.GetColumnDescription(con,tableName,columnName) =
            let tableName = RegexParsers.splitFullName tableName |> Seq.last
            (ssdtSchema.Value.Tables
             |> List.tryFind (fun t -> if tableName.Contains "." then t.Schema + "." + t.Name = tableName else t.Name = tableName)
             |> Option.bind (fun t -> t.Columns |> List.tryFind (fun c -> c.Name = columnName))
             |> Option.map (fun c ->
                if String.IsNullOrEmpty c.Description then ""
                elif c.Description.StartsWith("N'") then
                    " / " + c.Description.Substring(0, c.Description.Length-1).Replace("N'", "")
                else " / " + c.Description)
             |> Option.defaultValue columnName)
            +
            (ssdtSchema.Value.Descriptions
             |> List.filter(fun d -> d.DecriptionType = "SqlColumn" && d.ColumnName.IsSome && d.ColumnName.Value = columnName)
             |> List.tryFind(fun d ->
                    if tableName.Contains "." then d.Schema + "." + d.TableName = tableName else d.TableName = tableName)
             |> Option.map(fun d ->
                    if String.IsNullOrEmpty d.Description then ""
                    elif d.Description.StartsWith("N'") then
                        " / " + d.Description.Substring(0, d.Description.Length-1).Replace("N'", "")
                    else " / " + d.Description)
             |> Option.defaultValue ""
             )
        member __.CreateConnection(connectionString) = new SqlConnection(connectionString) :> IDbConnection
        member __.CreateCommand(connection,commandText) = new SqlCommand(commandText, downcast connection) :> IDbCommand
        member __.CreateCommandParameter(param, value) =
            let p = SqlParameter(param.Name,value)
            p.DbType <- param.TypeMapping.DbType
            ValueOption.iter (fun (t:int) -> p.SqlDbType <- Enum.ToObject(typeof<SqlDbType>, t) :?> SqlDbType) param.TypeMapping.ProviderType
            p.Direction <- param.Direction
            ValueOption.iter (fun l -> p.Size <- l) param.Length
            match param.TypeMapping.ProviderTypeName with
            | ValueSome "Microsoft.SqlServer.Types.SqlGeometry" -> p.UdtTypeName <- "Geometry"
            | ValueSome "Microsoft.SqlServer.Types.SqlGeography" -> p.UdtTypeName <- "Geography"
            | ValueSome "Microsoft.SqlServer.Types.SqlHierarchyId" -> p.UdtTypeName <- "HierarchyId"
            | _ -> ()
            p :> IDbDataParameter
        member __.ExecuteSprocCommand(com, inputParameters, returnCols, values:obj array) =
                let returnCols2 =
                    try getSprocReturnParams com.Connection com.CommandText (inputParameters |> Seq.toList)
                    with _ -> returnCols

                if com.Connection.State <> ConnectionState.Open then com.Connection.Open()
                MSSqlServer.executeSprocCommand com inputParameters returnCols2 values
        member __.ExecuteSprocCommandAsync(com, inputParameters, returnCols, values:obj array) =
            task {
                let returnCols2 =
                    try getSprocReturnParams com.Connection com.CommandText (inputParameters |> Seq.toList)
                    with _ -> returnCols
                if com.Connection.State <> ConnectionState.Open then
                    do! com.Connection.OpenAsync()

                return! MSSqlServer.executeSprocCommandAsync com inputParameters returnCols2 values
            }
        member __.CreateTypeMappings(con) = ()
        member __.GetSchemaCache() = schemaCache

        member __.GetTables(con,_) =
            let allowed = tableNames.Split([|','|], StringSplitOptions.RemoveEmptyEntries) |> Array.map (fun s -> s.Trim())

            let filterByTableNames (tbl: Table) =
                if allowed = [||] then true
                else allowed |> Array.exists (fun tblName -> String.Compare(tbl.Name, tblName, true) = 0)

            ssdtSchema.Value.Tables
            |> Seq.map MSSqlServerSsdt.ssdtTableToTable
            |> Seq.filter filterByTableNames
            |> Seq.map (fun tbl -> schemaCache.Tables.GetOrAdd(tbl.FullName, tbl))
            |> Seq.toList

        member __.GetPrimaryKey(table) =
            match ssdtSchema.Value.TryGetTableByName(table.Name) with
            |  ValueSome { PrimaryKey = ValueSome { Columns = [c] } } -> Some (c.Name)
            | _ -> None

        member __.GetColumns(con,table) =
            let columns =
                match ssdtSchema.Value.TryGetTableByName(table.Name) with
                | ValueSome ssdtTbl ->
                    ssdtTbl.Columns
                    |> List.map (MSSqlServerSsdt.ssdtColumnToColumn (ssdtSchema.Value.UserDefinedDataTypes) ssdtTbl)
                    |> List.map (fun col -> col.Name, col)
                | ValueNone -> []
                |> Map.ofList

            // Add PKs to cache
            columns
            |> Seq.map (fun kvp -> kvp.Value)
            |> Seq.iter (fun col ->
                if col.IsPrimaryKey then
                    schemaCache.PrimaryKeys.AddOrUpdate(table.FullName, [col.Name], fun key old ->
                         match col.Name with
                         | "" -> old
                         | x -> match old with
                                | [] -> [x]
                                | os -> x::os |> Seq.distinct |> Seq.toList |> List.sort
                    ) |> ignore
            )

            // Add columns to cache
            schemaCache.Columns.AddOrUpdate(table.FullName, columns, fun x old -> match columns.Count with 0 -> old | x -> columns)

        member __.GetRelationships(con, table) =
            let ssdtSchema = ssdtSchema.Value
            schemaCache.Relationships.GetOrAdd(table.FullName, fun name ->
                let children =
                    ssdtSchema.Relationships
                    |> List.filter (fun r -> Table.CreateFullName(r.ForeignTable.Schema, r.ForeignTable.Name) = table.FullName)
                    |> List.map (fun r ->
                        { Name = r.Name
                          PrimaryTable = Table.CreateFullName(r.ForeignTable.Schema, r.ForeignTable.Name)
                          PrimaryKey = r.ForeignTable.Columns.Head.Name
                          ForeignTable = Table.CreateFullName(r.DefiningTable.Schema, r.DefiningTable.Name)
                          ForeignKey = r.DefiningTable.Columns.Head.Name }
                    )

                let parents =
                    ssdtSchema.Relationships
                    |> List.filter (fun r -> Table.CreateFullName(r.DefiningTable.Schema, r.DefiningTable.Name) = table.FullName)
                    |> List.map (fun r ->
                        { Name = r.Name
                          PrimaryTable = Table.CreateFullName(r.ForeignTable.Schema, r.ForeignTable.Name)
                          PrimaryKey = r.ForeignTable.Columns.Head.Name
                          ForeignTable = Table.CreateFullName(r.DefiningTable.Schema, r.DefiningTable.Name)
                          ForeignKey = r.DefiningTable.Columns.Head.Name }
                    )

                children, parents
            )

        member __.GetSprocs(con) =
            ssdtSchema.Value.StoredProcs
            |> List.map (fun sp ->
                let inParams =
                    sp.Parameters
                    |> List.mapi (fun idx p ->
                        { Name = p.Name
                          TypeMapping = MSSqlServerSsdt.tryFindMappingOrVariant (ssdtSchema.Value.UserDefinedDataTypes) p.DataType
                          Direction = if p.IsOutput then ParameterDirection.InputOutput else ParameterDirection.Input
                          Length = p.Length
                          Ordinal = idx }
                    )
                let outParams =
                    sp.Parameters
                    |> List.filter (fun p -> p.IsOutput)
                    |> List.mapi (fun idx p ->
                        { Name = p.Name
                          TypeMapping = MSSqlServerSsdt.tryFindMappingOrVariant (ssdtSchema.Value.UserDefinedDataTypes) p.DataType
                          Direction = ParameterDirection.InputOutput
                          Length = p.Length
                          Ordinal = idx }
                    )

                // If no outParams, add a "ResultSet" property (see issue #706)
                let outParams =
                    match outParams with
                    | [] -> [ sprocReturnParam 0 ]
                    | _ -> outParams

                let spName = { ProcName = sp.Name; Owner = sp.Schema; PackageName = sp.Schema; }

                Root("Procedures", Sproc({ Name = spName; Params = (fun con -> inParams); ReturnColumns = (fun con sparams -> outParams) }))
            )
        member __.GetIndividualsQueryText(table,amount) = String.Empty // Not implemented for SSDT
        member __.GetIndividualQueryText(table,column) = String.Empty // Not implemented for SSDT

        member __.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns,isDeleteScript, con) =
            // TODO: Copied from Providers.MsSqlServer -- maybe this code should be shared? (also exists in Providers.MsSqlServer.Dynamic)

            let parameters = ResizeArray<_>()
            // make this nicer later..
            let param = ref 0
            let nextParam() =
                incr param
                sprintf "@param%i" !param

            let createParam (value:obj) =
                let paramName = nextParam()
                let p = MSSqlServer.createOpenParameter(paramName,value)
                p :> IDbDataParameter

            let fieldParam (value:obj) =
                let paramName = nextParam()
                parameters.Add(MSSqlServer.createOpenParameter(paramName,value):> IDbDataParameter)
                paramName

            let mssqlPaging =
              match mssqlVersionCache.TryGetValue(con.ConnectionString) with
              // SQL 2008 and earlier do not support OFFSET
              | true, mssqlVersion when mssqlVersion.Value.Major < 11 -> MSSQLPagingCompatibility.RowNumber
              | _ -> MSSQLPagingCompatibility.Offset

            let rec fieldNotation (al:alias) (c:SqlColumnType) =
                let buildf (c:Condition)=
                    let sb = System.Text.StringBuilder()
                    let (~~) (t:string) = sb.Append t |> ignore
                    filterBuilder (~~) [c]
                    sb.ToString()
                let x = fieldNotation
                let colSprint =
                    match String.IsNullOrEmpty(al) with
                    | true -> sprintf "[%s]"
                    | false -> sprintf "[%s].[%s]" al
                match c with
                // Custom database spesific overrides for canonical functions:
                | SqlColumnType.CanonicalOperation(cf,col) ->
                    let column = fieldNotation al col
                    match cf with
                    // String functions
                    | Replace(SqlConstant(searchItm),SqlCol(al2, col2)) -> sprintf "REPLACE(%s,%s,%s)" column (fieldParam searchItm) (fieldNotation al2 col2)
                    | Replace(SqlCol(al2, col2),SqlConstant(toItm)) -> sprintf "REPLACE(%s,%s,%s)" column (fieldNotation al2 col2) (fieldParam toItm)
                    | Replace(SqlCol(al2, col2),SqlCol(al3, col3)) -> sprintf "REPLACE(%s,%s,%s)" column (fieldNotation al2 col2) (fieldNotation al3 col3)
                    | Replace(SqlConstant searchItm, SqlConstant toItm) -> sprintf "REPLACE(%s,%s,%s)" column (fieldParam searchItm) (fieldParam toItm)
                    | Substring(SqlConstant startPos) -> sprintf "SUBSTRING(%s, %s)" column (fieldParam startPos)
                    | Substring(SqlCol(al2, col2)) -> sprintf "SUBSTRING(%s, %s)" column (fieldNotation al2 col2)
                    | SubstringWithLength(SqlConstant startPos,SqlConstant strLen) -> sprintf "SUBSTRING(%s, %s, %s)" column (fieldParam startPos) (fieldParam strLen)
                    | SubstringWithLength(SqlConstant startPos,SqlCol(al2, col2)) -> sprintf "SUBSTRING(%s, %s, %s)" column (fieldParam startPos) (fieldNotation al2 col2)
                    | SubstringWithLength(SqlCol(al2, col2), SqlConstant strLen) -> sprintf "SUBSTRING(%s, %s, %s)" column (fieldNotation al2 col2) (fieldParam strLen)
                    | SubstringWithLength(SqlCol(al2, col2),SqlCol(al3, col3)) -> sprintf "SUBSTRING(%s, %s, %s)" column (fieldNotation al2 col2) (fieldNotation al3 col3)
                    | Trim -> sprintf "LTRIM(RTRIM(%s))" column
                    | Length -> sprintf "DATALENGTH(%s)" column
                    | IndexOf(SqlConstant search) -> sprintf "CHARINDEX(%s,%s)" (fieldParam search) column
                    | IndexOf(SqlCol(al2, col2)) -> sprintf "CHARINDEX(%s,%s)" (fieldNotation al2 col2) column
                    | IndexOfStart(SqlConstant search, SqlConstant startPos) -> sprintf "CHARINDEX(%s,%s,%s)" (fieldParam search) column (fieldParam startPos)
                    | IndexOfStart(SqlConstant search, SqlCol(al2, col2)) -> sprintf "CHARINDEX(%s,%s,%s)" (fieldParam search) column (fieldNotation al2 col2)
                    | IndexOfStart(SqlCol(al2, col2), SqlConstant startPos) -> sprintf "CHARINDEX(%s,%s,%s)" (fieldNotation al2 col2) column (fieldParam startPos)
                    | IndexOfStart(SqlCol(al2, col2), SqlCol(al3, col3)) -> sprintf "CHARINDEX(%s,%s,%s)" (fieldNotation al2 col2) column (fieldNotation al3 col3)
                    | CastVarchar -> sprintf "CAST(%s AS NVARCHAR(MAX))" column
                    // Date functions
                    | Date -> sprintf "CAST(%s AS DATE)" column
                    | Year -> sprintf "YEAR(%s)" column
                    | Month -> sprintf "MONTH(%s)" column
                    | Day -> sprintf "DAY(%s)" column
                    | Hour -> sprintf "DATEPART(HOUR, %s)" column
                    | Minute -> sprintf "DATEPART(MINUTE, %s)" column
                    | Second -> sprintf "DATEPART(SECOND, %s)" column
                    | AddYears(SqlConstant x) -> sprintf "DATEADD(YEAR, %s, %s)" (fieldParam x) column
                    | AddYears(SqlCol(al2, col2)) -> sprintf "DATEADD(YEAR, %s, %s)" (fieldNotation al2 col2) column
                    | AddMonths x -> sprintf "DATEADD(MONTH, %s, %s)" (fieldParam x) column
                    | AddDays(SqlConstant x) -> sprintf "DATEADD(DAY, %s, %s)" (fieldParam x) column // SQL ignores decimal part :-(
                    | AddDays(SqlCol(al2, col2)) -> sprintf "DATEADD(DAY, %s, %s)" (fieldNotation al2 col2) column
                    | AddHours x -> sprintf "DATEADD(HOUR, %f, %s)" x column
                    | AddMinutes(SqlConstant x) -> sprintf "DATEADD(MINUTE, %s, %s)" (fieldParam x) column
                    | AddMinutes(SqlCol(al2, col2)) -> sprintf "DATEADD(MINUTE, %s, %s)" (fieldNotation al2 col2) column
                    | AddSeconds x -> sprintf "DATEADD(SECOND, %f, %s)" x column
                    | DateDiffDays(SqlCol(al2, col2)) -> sprintf "DATEDIFF(DAY, %s, %s)" (fieldNotation al2 col2) column
                    | DateDiffSecs(SqlCol(al2, col2)) -> sprintf "DATEDIFF(SECOND, %s, %s)" (fieldNotation al2 col2) column
                    | DateDiffDays(SqlConstant x) -> sprintf "DATEDIFF(DAY, %s, %s)" (fieldParam x) column
                    | DateDiffSecs(SqlConstant x) -> sprintf "DATEDIFF(SECOND, %s, %s)" (fieldParam x) column
                    // Math functions
                    | Truncate -> sprintf "TRUNCATE(%s)" column
                    | BasicMathOfColumns(o, a, c) when o = "/" -> sprintf "(%s %s (1.0*%s))" column o (fieldNotation a c)
                    | BasicMathOfColumns(o, a, c) -> sprintf "(%s %s %s)" column (o.Replace("||","+")) (fieldNotation a c)
                    | BasicMath(o, par) when (par :? String || par :? Char) -> sprintf "(%s %s %s)" column (o.Replace("||","+")) (fieldParam par)
                    | BasicMathLeft(o, par) when (par :? String || par :? Char) -> sprintf "(%s %s %s)" (fieldParam par) (o.Replace("||","+")) column
                    | Greatest(SqlConstant x) -> sprintf "(SELECT MAX(V) FROM (VALUES (%s), (%s)) AS VALUE(V))" (fieldParam x) column
                    | Greatest(SqlCol(al2, col2)) -> sprintf "(SELECT MAX(V) FROM (VALUES (%s), (%s)) AS VALUE(V))" (fieldNotation al2 col2) column
                    | Least(SqlConstant x) -> sprintf "(SELECT MIN(V) FROM (VALUES (%s), (%s)) AS VALUE(V))" (fieldParam x) column
                    | Least(SqlCol(al2, col2)) -> sprintf "(SELECT MIN(V) FROM (VALUES (%s), (%s)) AS VALUE(V))" (fieldNotation al2 col2) column
                    | Pow(SqlCol(al2, col2)) -> sprintf "POWER(%s, %s)" column (fieldNotation al2 col2)
                    | Pow(SqlConstant x) -> sprintf "POWER(%s, %s)" column (fieldParam x)
                    | PowConst(SqlConstant x) -> sprintf "POWER(%s, %s)" (fieldParam x) column
                    //if-then-else
                    | CaseSql(f, SqlCol(al2, col2)) -> sprintf "CASE WHEN %s THEN %s ELSE %s END " (buildf f) column (fieldNotation al2 col2)
                    | CaseSql(f, SqlConstant itm) -> sprintf "CASE WHEN %s THEN %s ELSE %s END " (buildf f) column (fieldParam itm)
                    | CaseNotSql(f, SqlConstant itm) -> sprintf "CASE WHEN %s THEN %s ELSE %s END " (buildf f) (fieldParam itm) column
                    | CaseSqlPlain(f, itm, itm2) -> sprintf "CASE WHEN %s THEN %s ELSE %s END " (buildf f) (fieldParam itm) (fieldParam itm2)
                    | _ -> Utilities.genericFieldNotation (fieldNotation al) colSprint c
                | GroupColumn (StdDevOp key, KeyColumn _) -> sprintf "STDEV(%s)" (colSprint key)
                | GroupColumn (StdDevOp _,x) -> sprintf "STDEV(%s)" (fieldNotation al x)
                | GroupColumn (VarianceOp key, KeyColumn _) -> sprintf "VAR(%s)" (colSprint key)
                | GroupColumn (VarianceOp _,x) -> sprintf "VAR(%s)" (fieldNotation al x)
                | _ -> Utilities.genericFieldNotation (fieldNotation al) colSprint c

            and filterBuilder (~~) (f:Condition list) =
                // the filter expressions

                let rec filterBuilder' = function
                    | [] -> ()
                    | (cond::conds) ->
                        let build op preds (rest:Condition list option) =
                            ~~ "("
                            preds |> List.iteri( fun i (alias,col,operator,data) ->
                                    let column = fieldNotation alias col
                                    let extractData data =
                                            match data with
                                            | Some(x) when (box x :? System.Linq.IQueryable) -> [||]
                                            | Some(x) when (box x :? obj array) ->
                                                // in and not in operators pass an array
                                                let elements = box x :?> obj array
                                                Array.init (elements.Length) (fun i -> createParam (elements.GetValue(i)))
                                            | Some(x) -> [|createParam (box x)|]
                                            | None ->    [|createParam DBNull.Value|]

                                    let operatorIn operator (array : IDbDataParameter[]) =
                                        if Array.isEmpty array then
                                            match operator with
                                            | FSharp.Data.Sql.In -> "1=0" // nothing is in the empty set
                                            | FSharp.Data.Sql.NotIn -> "1=1" // anything is not in the empty set
                                            | _ -> failwith "Should not be called with any other operator"
                                        else
                                            let text = String.Join(",", array |> Array.map (fun p -> p.ParameterName))
                                            Array.iter parameters.Add array
                                            match operator with
                                            | FSharp.Data.Sql.In -> sprintf "%s IN (%s)" column text
                                            | FSharp.Data.Sql.NotIn -> sprintf "%s NOT IN (%s)" column text
                                            | _ -> failwith "Should not be called with any other operator"

                                    let prefix = if i>0 then (sprintf " %s " op) else ""
                                    let paras = extractData data

                                    let operatorInQuery operator (array : IDbDataParameter[]) =
                                        let innersql, innerpars = data.Value |> box :?> string * IDbDataParameter[]
                                        Array.iter parameters.Add innerpars
                                        match operator with
                                        | FSharp.Data.Sql.NestedExists -> sprintf "EXISTS (%s)" innersql
                                        | FSharp.Data.Sql.NestedNotExists -> sprintf "NOT EXISTS (%s)" innersql
                                        | FSharp.Data.Sql.NestedIn -> sprintf "%s IN (%s)" column innersql
                                        | FSharp.Data.Sql.NestedNotIn -> sprintf "%s NOT IN (%s)" column innersql
                                        | _ -> failwith "Should not be called with any other operator"

                                    ~~(sprintf "%s%s" prefix <|
                                        match operator with
                                        | FSharp.Data.Sql.IsNull -> sprintf "%s IS NULL" column
                                        | FSharp.Data.Sql.NotNull -> sprintf "%s IS NOT NULL" column
                                        | FSharp.Data.Sql.In
                                        | FSharp.Data.Sql.NotIn -> operatorIn operator paras
                                        | FSharp.Data.Sql.NestedExists
                                        | FSharp.Data.Sql.NestedNotExists
                                        | FSharp.Data.Sql.NestedIn
                                        | FSharp.Data.Sql.NestedNotIn -> operatorInQuery operator paras
                                        | _ ->
                                            let aliasformat = sprintf "%s %s %s" column
                                            match data with
                                            | Some d when (box d :? alias * SqlColumnType) ->
                                                let alias2, col2 = box d :?> (alias * SqlColumnType)
                                                let alias2f = fieldNotation alias2 col2
                                                aliasformat (operator.ToString()) alias2f
                                            | _ ->
                                                parameters.Add paras.[0]
                                                aliasformat (operator.ToString()) paras.[0].ParameterName
                            ))
                            // there's probably a nicer way to do this
                            let rec aux = function
                                | x::[] when preds.Length > 0 ->
                                    ~~ (sprintf " %s " op)
                                    filterBuilder' [x]
                                | x::[] -> filterBuilder' [x]
                                | x::xs when preds.Length > 0 ->
                                    ~~ (sprintf " %s " op)
                                    filterBuilder' [x]
                                    ~~ (sprintf " %s " op)
                                    aux xs
                                | x::xs ->
                                    filterBuilder' [x]
                                    ~~ (sprintf " %s " op)
                                    aux xs
                                | [] -> ()

                            Option.iter aux rest
                            ~~ ")"

                        match cond with
                        | Or(preds,rest) -> build "OR" preds rest
                        | And(preds,rest) ->  build "AND" preds rest
                        | ConstantTrue -> ~~ " (1=1) "
                        | ConstantFalse -> ~~ " (1=0) "
                        | NotSupported x ->  failwithf "Not supported: %O" x
                        filterBuilder' conds
                filterBuilder' f

            let sb = System.Text.StringBuilder()

            let (~~) (t:string) = sb.Append t |> ignore

            match sqlQuery.Take, sqlQuery.Skip, sqlQuery.Ordering with
            | Some _, Some _, [] -> failwith "skip and take paging requires an orderBy clause."
            | _ -> ()

            let getTable x =
                match sqlQuery.Aliases.TryFind x with
                | Some(a) when x <> "" -> a
                | _ -> baseTable

            let singleEntity = sqlQuery.Aliases.Count = 0

            // build  the select statement, this is easy ...
            let selectcolumns =
                if projectionColumns |> Seq.isEmpty then "1" else
                String.Join(",",
                    [|for KeyValue(k,v) in projectionColumns do
                        let cols = (getTable k).FullName
                        let k = if k <> "" then k elif baseAlias <> "" then baseAlias else baseTable.Name
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in schemaCache.Columns.[cols] |> Seq.map (fun c -> c.Key) do
                                if singleEntity then yield sprintf "[%s].[%s] as '%s'" k col col
                                else yield sprintf "[%s].[%s] as '[%s].[%s]'" k col k col
                        else
                            for colp in v |> Seq.distinct do
                                match colp with
                                | EntityColumn col ->
                                    if singleEntity then yield sprintf "[%s].[%s] as '%s'" k col col
                                    else yield sprintf "[%s].[%s] as '[%s].[%s]'" k col k col
                                | OperationColumn(n,op) ->
                                    yield sprintf "%s as '%s'" (fieldNotation k op) n|])

            // Cache select-params to match group-by params
            let tmpGrpParams = System.Collections.Generic.Dictionary<(alias*SqlColumnType), string>()

            // Create sumBy, minBy, maxBy, ... field columns
            let columns =
                let extracolumns =
                    match sqlQuery.Grouping with
                    | [] -> FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation MSSqlServer.fieldNotationAlias sqlQuery.AggregateOp
                    | g  ->
                        let keys = g |> List.map(fst) |> List.concat |> List.map(fun (a,c) ->
                            let fn = fieldNotation a c
                            if not (tmpGrpParams.ContainsKey (a,c)) then
                                tmpGrpParams.Add((a,c), fn)
                            if sqlQuery.Aliases.Count < 2 then fn
                            else sprintf "%s as '%s'" fn fn)
                        let aggs = g |> List.map(snd) |> List.concat
                        let res2 = FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation MSSqlServer.fieldNotationAlias aggs |> List.toSeq
                        [String.Join(", ", keys) + (if List.isEmpty aggs || List.isEmpty keys then ""  else ", ") + String.Join(", ", res2)]
                match extracolumns with
                | [] -> selectcolumns
                | h::t -> h

            // next up is the FROM statement which includes joins ..
            let fromBuilder() =
                sqlQuery.Links
                |> List.iter(fun (fromAlias, data, destAlias)  ->
                    let joinType = if data.OuterJoin then "LEFT OUTER JOIN " else "INNER JOIN "
                    let destTable = getTable destAlias
                    ~~  (sprintf "%s [%s].[%s] as [%s] on "
                            joinType destTable.Schema destTable.Name destAlias)
                    ~~  (String.Join(" AND ", (List.zip data.ForeignKey data.PrimaryKey) |> List.map(fun (foreignKey,primaryKey) ->
                        sprintf "%s = %s"
                            (fieldNotation (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias) foreignKey)
                            (fieldNotation (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias) primaryKey)
                        ))))

            let groupByBuilder groupkeys =
                groupkeys
                |> List.iteri(fun i (alias,column) ->
                    let cname =
                        if tmpGrpParams.ContainsKey(alias,column) then tmpGrpParams.[alias,column]
                        else fieldNotation alias column
                    if i > 0 then ~~ ", "
                    ~~ cname)

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) ->
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "%s %s" (fieldNotation alias column) (if not desc then "DESC " else "")))

            if isDeleteScript then
                ~~(sprintf "DELETE FROM [%s].[%s] " baseTable.Schema baseTable.Name)
            else
                // SELECT
                if sqlQuery.Distinct && sqlQuery.Count then
                    let colsAggrs = columns.Split([|" as "|], StringSplitOptions.None)
                    let distColumns = colsAggrs.[0] + (if colsAggrs.Length = 2 then "" else " + ',' + " + String.Join(" + ',' + ", colsAggrs |> Seq.filter(fun c -> c.Contains ",") |> Seq.map(fun c -> c.Substring(c.IndexOf(",")+1))))
                    ~~(sprintf "SELECT COUNT(DISTINCT %s) " distColumns)
                elif sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s%s " (if sqlQuery.Take.IsSome then sprintf "TOP %i " sqlQuery.Take.Value else "")   columns)
                elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
                else
                    match sqlQuery.Skip, sqlQuery.Take with
                    | None, Some take -> ~~(sprintf "SELECT TOP %i %s " take columns)
                    | _ -> ~~(sprintf "SELECT %s " columns)
                //ROW_NUMBER
                match mssqlPaging,sqlQuery.Skip, sqlQuery.Take with
                | MSSQLPagingCompatibility.RowNumber, Some _, _ ->
                    //INCLUDE order by clause in ROW_NUMBER () OVER() of CTE
                    if sqlQuery.Ordering.Length > 0 then
                        ~~", ROW_NUMBER() OVER(ORDER BY  "
                        orderByBuilder()
                        ~~" ) AS RN  "
                | _ -> ()
                // FROM
                let bal = if baseAlias = "" then baseTable.Name else baseAlias
                ~~(sprintf "FROM [%s].[%s] as [%s] " baseTable.Schema baseTable.Name bal)
                sqlQuery.CrossJoins |> Seq.iter(fun (a,t) -> ~~(sprintf ", [%s].[%s] as [%s] " t.Schema t.Name a))
            fromBuilder()
            // WHERE
            if sqlQuery.Filters.Length > 0 then
                // each filter is effectively the entire contents of each where clause in the LINQ query,
                // of which there can be many. Simply turn them all into one big AND expression as that is the
                // only logical way to deal with them.
                let f = [And([],Some sqlQuery.Filters)]
                ~~"WHERE "
                filterBuilder (~~) f

            // GROUP BY
            if sqlQuery.Grouping.Length > 0 then
                let groupkeys = sqlQuery.Grouping |> List.map(fst) |> List.concat
                if groupkeys.Length > 0 then
                    ~~" GROUP BY "
                    groupByBuilder groupkeys

            if sqlQuery.HavingFilters.Length > 0 then
                let keys = sqlQuery.Grouping |> List.map(fst) |> List.concat

                let f = [And([],Some (sqlQuery.HavingFilters |> CommonTasks.parseHaving fieldNotation keys))]
                ~~" HAVING "
                filterBuilder (~~) f

            // ORDER BY
            match mssqlPaging, sqlQuery.Skip, sqlQuery.Take with
            | MSSQLPagingCompatibility.Offset, _, _
            | MSSQLPagingCompatibility.RowNumber, None, _ ->
              if sqlQuery.Ordering.Length > 0 then
                  ~~"ORDER BY "
                  orderByBuilder()
            | _ ->
              //when RowNumber compatibility with SKIP, ommit order by clause as it's already in CTE
              ()

            match sqlQuery.Union with
            | Some(UnionType.UnionAll, suquery, pars) ->
                parameters.AddRange pars
                ~~(sprintf " UNION ALL %s " suquery)
            | Some(UnionType.NormalUnion, suquery, pars) ->
                parameters.AddRange pars
                ~~(sprintf " UNION %s " suquery)
            | Some(UnionType.Intersect, suquery, pars) ->
                parameters.AddRange pars
                ~~(sprintf " INTERSECT %s " suquery)
            | Some(UnionType.Except, suquery, pars) ->
                parameters.AddRange pars
                ~~(sprintf " EXCEPT %s " suquery)
            | None -> ()

            let sql =
                match mssqlPaging with
                | MSSQLPagingCompatibility.RowNumber ->
                    let outerSb = System.Text.StringBuilder()
                    outerSb.Append "WITH CTE AS ( "  |> ignore
                    match sqlQuery.Skip, sqlQuery.Take with
                    | Some skip, Some take ->
                        outerSb.Append (sb.ToString()) |> ignore
                        outerSb.Append ")" |> ignore
                        outerSb.Append (sprintf "SELECT %s FROM CTE [%s] WHERE RN BETWEEN %i AND %i" columns (if baseAlias = "" then baseTable.Name else baseAlias) (skip+1) (skip+take))  |> ignore
                        outerSb.ToString()
                    | Some skip, None ->
                        outerSb.Append (sb.ToString()) |> ignore
                        outerSb.Append ")" |> ignore
                        outerSb.Append (sprintf "SELECT %s FROM CTE [%s] WHERE RN > %i " columns (if baseAlias = "" then baseTable.Name else baseAlias) skip)  |> ignore
                        outerSb.ToString()
                    | _ ->
                      sb.ToString()
                | _ ->
                    match sqlQuery.Skip, sqlQuery.Take with
                    | Some skip, Some take ->
                        // Note: this only works in >=SQL2012
                        ~~ (sprintf "OFFSET %i ROWS FETCH NEXT %i ROWS ONLY" skip take)
                    | Some skip, None ->
                        // Note: this only works in >=SQL2012
                        ~~ (sprintf "OFFSET %i ROWS FETCH NEXT %i ROWS ONLY" skip System.UInt32.MaxValue)
                    | _ -> ()
                    sb.ToString()

            (sql,parameters)

        member this.ProcessUpdates(con, entities, transactionOptions, timeout) =
            // TODO: Copied from Providers.MsSqlServer -- maybe this code should be shared?

            let sb = Text.StringBuilder()

            CommonTasks.``ensure columns have been loaded`` (this :> ISqlProvider) con entities

            if entities.Count = 0 then
                ()
            else
            use scope = TransactionUtils.ensureTransaction transactionOptions
            try
                // close the connection first otherwise it won't get enlisted into the transaction
                if con.State = ConnectionState.Open then con.Close()
                con.Open()
                // initially supporting update/create/delete of single entities, no hierarchies yet
                entities.Keys
                |> Seq.iter(fun e ->
                    match e._State with
                    | Created ->
                        let cmd = createInsertCommand con sb e
                        Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        let id = cmd.ExecuteScalar()
                        CommonTasks.checkKey schemaCache.PrimaryKeys id e
                        e._State <- Unchanged
                    | Modified fields ->
                        let cmd = createUpdateCommand con sb e fields
                        Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        cmd.ExecuteNonQuery() |> ignore
                        e._State <- Unchanged
                    | Delete ->
                        let cmd = createDeleteCommand con sb e
                        Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        cmd.ExecuteNonQuery() |> ignore
                        // remove the pk to prevent this attempting to be used again
                        e.SetPkColumnOptionSilent(schemaCache.PrimaryKeys.[e.Table.FullName], None)
                        e._State <- Deleted
                    | Deleted | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!")
                                   // but is possible if you try to use same context on multiple threads. Don't do that.
                if scope<>null then scope.Complete()

            finally
                con.Close()

        member this.ProcessUpdatesAsync(con, entities, transactionOptions, timeout) =
            // TODO: Copied from Providers.MsSqlServer -- maybe this code should be shared?

            let sb = Text.StringBuilder()

            CommonTasks.``ensure columns have been loaded`` (this :> ISqlProvider) con entities

            if entities.Count = 0 then
                task { () }
            else

            task {
                use scope = TransactionUtils.ensureTransaction transactionOptions
                try
                    // close the connection first otherwise it won't get enlisted into the transaction
                    if con.State = ConnectionState.Open then con.Close()
                    do! con.OpenAsync()
                    // initially supporting update/create/delete of single entities, no hierarchies yet
                    let handleEntity (e: SqlEntity) =
                        match e._State with
                        | Created ->
                            task {
                                let cmd = createInsertCommand con sb e
                                Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                let! id = cmd.ExecuteScalarAsync()
                                CommonTasks.checkKey schemaCache.PrimaryKeys id e
                                e._State <- Unchanged
                            }
                        | Modified fields ->
                            task {
                                let cmd = createUpdateCommand con sb e fields
                                Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                let! c = cmd.ExecuteNonQueryAsync()
                                e._State <- Unchanged
                            }
                        | Delete ->
                            task {
                                let cmd = createDeleteCommand con sb e
                                Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                let! c = cmd.ExecuteNonQueryAsync()
                                // remove the pk to prevent this attempting to be used again
                                e.SetPkColumnOptionSilent(schemaCache.PrimaryKeys.[e.Table.FullName], None)
                                e._State <- Deleted
                            }
                        | Deleted | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!"

                    do! Utilities.executeOneByOne handleEntity (entities.Keys|>Seq.toList)
                    if scope<>null then scope.Complete()

                finally
                    con.Close()
            }

