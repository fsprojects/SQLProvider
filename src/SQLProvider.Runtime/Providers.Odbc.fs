namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Data
open System.Data.Odbc
open FSharp.Data.Sql
open FSharp.Data.Sql.Transactions
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common
#if NETSTANDARD
open StandardExtensions
#endif

type internal OdbcProvider(contextSchemaPath, quotechar : OdbcQuoteCharacter) =
    let schemaCache = SchemaCache.LoadOrEmpty(contextSchemaPath)
    let myLock = new Object()

    let mutable typeMappings = []
    let mutable findClrType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"

    let quotes =
        if quotechar = OdbcQuoteCharacter.NO_QUOTES then ' ',' '
        elif quotechar = OdbcQuoteCharacter.GRAVE_ACCENT then '`', '`'
        elif quotechar = OdbcQuoteCharacter.SQUARE_BRACKETS then '[', ']'
        elif quotechar = OdbcQuoteCharacter.DOUBLE_QUOTES then '"', '"'
        elif quotechar = OdbcQuoteCharacter.APHOSTROPHE then ''', '''
        else '`', '`'

    let mutable cOpen = fst quotes //char separator in query for table aliases `alias` or [alias]
    let mutable cClose = snd quotes

    let createTypeMappings (con:OdbcConnection) =

        if quotechar = OdbcQuoteCharacter.DEFAULT_QUOTE && con.Driver.Contains("sql") then
            cOpen <- '['
            cClose <- ']'

        let dt = con.GetSchema("DataTypes")

        let getDbType(providerType:int) =
            let p = OdbcParameter()
            p.OdbcType <- (Enum.ToObject(typeof<OdbcType>, providerType) :?> OdbcType)
            p.DbType

        let getClrType (input:string) = Utilities.getType(input).ToString()

        let mappings =
            [
                for r in dt.Rows do
                    
                    if not(String.IsNullOrEmpty(r.["DataType"].ToString()) || String.IsNullOrEmpty(r.["TypeName"].ToString()) || String.IsNullOrEmpty(r.["ProviderDbType"].ToString())) then
                        let clrType = getClrType (string r.["DataType"])
                        let oleDbType = string r.["TypeName"]
                        let providerType = unbox<int> r.["ProviderDbType"]
                        let dbType = getDbType providerType
                        yield { ProviderTypeName = ValueSome oleDbType; ClrType = clrType; DbType = dbType; ProviderType = ValueSome providerType; }
                    
                    if (not(isNull r.Table)) && r.Table.Columns.Contains("DataType")  && r.Table.Columns.Contains("TypeName")  && r.Table.Columns.Contains("ProviderDbType") then
                        let clrType = getClrType (r.Table.Columns.["DataType"].DataType.ToString())
                        let oleDbType = r.Table.Columns.["TypeName"].DataType.ToString()
                        let providerType = unbox<int> r.Table.Columns.["ProviderDbType"].Ordinal
                        let dbType = getDbType providerType
                        yield { ProviderTypeName = ValueSome oleDbType; ClrType = clrType; DbType = dbType; ProviderType = ValueSome providerType; }
                yield { ProviderTypeName = ValueSome "cursor"; ClrType = (typeof<SqlEntity[]>).ToString(); DbType = DbType.Object; ProviderType = ValueNone; }
            ]

        let clrMappings =
            mappings
            |> List.map (fun m -> m.ClrType, m)
            |> Map.ofList

        let dbMappings =
            mappings
            |> List.map (fun m -> (match m.ProviderTypeName with ValueSome x -> x | ValueNone -> ""), m)
            |> Map.ofList

        typeMappings <- mappings
        findClrType <- clrMappings.TryFind
        findDbType <- dbMappings.TryFind

    let createInsertCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = new OdbcCommand()
        cmd.Connection <- con :?> OdbcConnection
        let columnNames, values =
            (([],0),entity.ColumnValues)
            ||> Seq.fold(fun (out,i) (key,value) ->
                let name = sprintf "@param%i" i
                let p = OdbcParameter(name,value)
                (key,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray
            |> Array.unzip

        sb.Clear() |> ignore
        ~~(sprintf "INSERT INTO %c%s%c (%s) VALUES (%s);"
            cOpen (entity :> IColumnHolder).Table.Name cClose
            (String.Join(",",columnNames))
            (String.Join(",",values |> Array.map(fun _ -> "?"))))
        cmd.Parameters.AddRange(values)
        cmd.CommandText <- sb.ToString()
        cmd

    let lastInsertId (con:IDbConnection) =
        let cmd = new OdbcCommand()
        cmd.Connection <- con :?> OdbcConnection
        cmd.CommandText <- "SELECT @@IDENTITY AS id;"
        cmd

    let createUpdateCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) (changedColumns: string list) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = new OdbcCommand()
        cmd.Connection <- con :?> OdbcConnection
        let pk =
            match schemaCache.PrimaryKeys.TryGetValue (entity :> IColumnHolder).Table.FullName with
            | true, pk -> pk
            | false, _ -> []
        sb.Clear() |> ignore

        match pk with
        | [x] when changedColumns |> List.exists ((=)x)
            -> failwith "Error - you cannot change the primary key of an entity."
        | _ -> ()

        let pkValues =
            match (entity :> IColumnHolder).GetPkColumnOption<obj> pk with
            | [] -> failwith ("Error - you cannot update an entity that does not have a primary key. (" + (entity :> IColumnHolder).Table.FullName + ")")
            | v -> v

        let data =
            (([],0),changedColumns)
            ||> List.fold(fun (out,i) col ->
                let p =
                    match (entity :> IColumnHolder).GetColumnOption<obj> col with
                    | Some v -> OdbcParameter(null,v)
                    | None -> OdbcParameter(null,DBNull.Value)
                (col,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray

        match pk with
        | [] -> ()
        | [x] ->
            ~~(sprintf "UPDATE %c%s%c SET %s WHERE %s = ?;"
                cOpen (entity :> IColumnHolder).Table.Name cClose
                (String.concat "," (data |> Array.map(fun (c,_) -> sprintf "%c%s%c = %s" cOpen c cClose "?" ) ))
                x)
        | ks -> 
            // TODO: What is the ?-mark parameter? Look from other providers how this is done.
            failwith ("Composite key items update is not Supported in Odbc. (" + (entity :> IColumnHolder).Table.FullName + ")")

        cmd.Parameters.AddRange(data |> Array.map snd)

        pkValues |> List.iteri(fun i pkValue ->
            let pkParam = OdbcParameter(null, pkValue)
            cmd.Parameters.Add pkParam |> ignore
            )
        cmd.CommandText <- sb.ToString()
        cmd

    let createDeleteCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = new OdbcCommand()
        cmd.Connection <- con :?> OdbcConnection
        sb.Clear() |> ignore
        let pk =
            match schemaCache.PrimaryKeys.TryGetValue (entity :> IColumnHolder).Table.FullName with
            | true, pk -> pk
            | false, _ -> []
        sb.Clear() |> ignore
        let pkValues =
            match (entity :> IColumnHolder).GetPkColumnOption<obj> pk with
            | [] -> failwith ("Error - you cannot delete an entity that does not have a primary key. (" + (entity :> IColumnHolder).Table.FullName + ")")
            | v -> v

        pkValues |> List.iteri(fun i pkValue ->
            cmd.Parameters.AddWithValue("@id"+i.ToString(),pkValue) |> ignore)

        match pk with
        | [] -> ()
        | [k] -> ~~(sprintf "DELETE FROM %c%s%c WHERE %s = ?;" cOpen (entity :> IColumnHolder).Table.Name cClose k )
        | ks -> 
            // TODO: What is the ?-mark parameter? Look from other providers how this is done.
            failwith ("Composite key items deletion is not Supported in Odbc. (" + (entity :> IColumnHolder).Table.FullName + ")")
        cmd.CommandText <- sb.ToString()
        cmd

    interface ISqlProvider with
        member __.CloseConnectionAfterQuery = true
        member __.DesignConnection = true
        member __.StoredProcedures = true
        member __.GetLockObject() = myLock
        member __.GetTableDescription(con,tableName) = 
            let t = tableName.Substring(tableName.LastIndexOf('.')+1)
            let table = (con:?>OdbcConnection).GetSchema("Tables",[|null;null;t.Replace("\"", "")|]).AsEnumerable()
            let view = (con:?>OdbcConnection).GetSchema("Views",[|null;null;t.Replace("\"", "")|]).AsEnumerable() 
            let desc = 
                Seq.append table view
                |> Seq.map(fun row ->
                    try 
                        let remarks = row.["REMARKS"].ToString()
                        let endpos = remarks.IndexOf('\000')
                        if endpos = -1 then remarks else remarks.Substring(0, endpos-1) 
                    with :? KeyNotFoundException -> 
                    try row.["DESCRIPTION"].ToString() with :? KeyNotFoundException -> 
                    try row.["COMMENTS"].ToString() with :? KeyNotFoundException -> ""
                ) |> Seq.toList
            match desc with
            | [x] -> x
            | _ -> ""

        member __.GetColumnDescription(con,tableName,columnName) = 
            let t = tableName.Substring(tableName.LastIndexOf('.')+1) 
            let desc = 
                (con:?>OdbcConnection).GetSchema("Columns",[|null;null;t.Replace("\"", "");columnName|]).AsEnumerable() 
                |> Seq.map(fun row ->
                    try 
                        let remarks = row.["REMARKS"].ToString()
                        let endpos = remarks.IndexOf('\000')
                        if endpos = -1 then remarks else remarks.Substring(0, endpos-1) 
                    with :? KeyNotFoundException -> 
                    try row.["DESCRIPTION"].ToString() with :? KeyNotFoundException -> 
                    try row.["COMMENTS"].ToString() with :? KeyNotFoundException -> ""
                ) |> Seq.toList
            match desc with
            | [x] -> x
            | _ -> ""

        member __.CreateConnection(connectionString) = upcast new OdbcConnection(connectionString)
        member __.CreateCommand(connection,commandText) = upcast new OdbcCommand(commandText, connection:?>OdbcConnection)

        member __.CreateCommandParameter(param, value) =
            let p = OdbcParameter()
            p.Value <- value
            p.ParameterName <- param.Name
            p.DbType <- param.TypeMapping.DbType
            p.Direction <- param.Direction
            ValueOption.iter (fun l -> p.Size <- l) param.Length
            upcast p

        /// Not implemented yet
        member __.ExecuteSprocCommand(com,inputParams,_,_) =
            let odbcCommand = new OdbcCommand("{call " + com.CommandText + " (?)}")
            let inputParameters = inputParams |> Array.filter (fun p -> p.Direction = ParameterDirection.Input)
            odbcCommand.CommandType <- CommandType.StoredProcedure;
            inputParameters |> Array.iter (odbcCommand.Parameters.Add >> ignore)
            odbcCommand.ExecuteNonQuery() |> ignore
            ReturnValueType.Unit

        /// Not implemented yet
        member __.ExecuteSprocCommandAsync(com,inputParams,_,_) =
            task {
                let odbcCommand = new OdbcCommand("{call " + com.CommandText + " (?)}")
                let inputParameters = inputParams |> Array.filter (fun p -> p.Direction = ParameterDirection.Input)
                odbcCommand.CommandType <- CommandType.StoredProcedure;
                inputParameters |> Array.iter (odbcCommand.Parameters.Add >> ignore)
                let! r = odbcCommand.ExecuteNonQueryAsync()
                return ReturnValueType.Unit
            }

        member __.CreateTypeMappings(con) = createTypeMappings (con:?>OdbcConnection)
        member __.GetSchemaCache() = schemaCache

        member __.GetTables(con,_) =
            let con = con :?> OdbcConnection
            if con.State <> ConnectionState.Open then con.Open()
            let dataTables = con.GetSchema("Tables").Rows |> Seq.cast<DataRow> |> Seq.map (fun i -> i.ItemArray)
            let dataViews = con.GetSchema("Views").Rows |> Seq.cast<DataRow> |> Seq.map (fun i -> i.ItemArray)
            [ for dataTable in Seq.append dataTables dataViews do
                let schema = 
                    if String.IsNullOrEmpty(dataTable.[1].ToString()) then
                        "dbo"
                    else string dataTable.[1]

                let table ={ Schema = schema ; Name = string dataTable.[2] ; Type=(string dataTable.[3]).ToLower() }
                yield schemaCache.Tables.GetOrAdd(table.FullName,table)
                ] |> List.toArray

        member __.GetPrimaryKey(table) =
            match schemaCache.PrimaryKeys.TryGetValue table.FullName with
            | true, [v] -> Some v
            | _ -> None

        member __.GetColumns(con,table) =
            match schemaCache.Columns.TryGetValue table.FullName with
            | (true,data) when data.Count > 0 -> data
            | _ ->
                let con = con :?> OdbcConnection
                if con.State <> ConnectionState.Open then con.Open()
                let primaryKey =
                    if table.Type = "table"
                    then con.GetSchema("Indexes", [| null; null; table.Name |]).Rows |> Seq.cast<DataRow> |> Seq.map (fun i -> i.ItemArray) |> Array.ofSeq
                    else Array.empty
                let dataTable = con.GetSchema("Columns", [| null; null; table.Name; null|]).Rows |> Seq.cast<DataRow> |> Seq.map (fun i -> i.ItemArray)
                let columns =
                    [ for i in dataTable do
                        let dt = i.[5] :?> string
                        match findDbType dt with
                        | Some(m) ->
                            let name = i.[3] :?> string
                            let maxlen = if i.[6] = box(DBNull.Value) then 0 else i.[6] :?> int
                            
                            let pkColumn = (Array.isEmpty primaryKey |> not) && primaryKey.[0].[8] = box name
                            let col =
                                { Column.Name = name
                                  TypeMapping = m
                                  IsNullable = let b = i.[17] :?> string in b = "YES"
                                  IsPrimaryKey = pkColumn
                                  IsAutonumber = pkColumn
                                  HasDefault = false
                                  IsComputed = false
                                  TypeInfo = 
                                    if maxlen < 1 then ValueSome dt
                                    else ValueSome (dt + "(" + maxlen.ToString() + ")")
                                  }
                            if col.IsPrimaryKey then 
                                schemaCache.PrimaryKeys.AddOrUpdate(table.FullName, [col.Name], fun key old ->
                                    match col.Name with 
                                    | "" -> old 
                                    | x -> match old with
                                           | [] -> [x]
                                           | os -> x::os |> Seq.distinct |> Seq.toList |> List.sort
                                ) |> ignore
                            yield (col.Name,col)
                        | _ -> ()]
                    |> Map.ofList
                schemaCache.Columns.AddOrUpdate(table.FullName, columns, fun x old -> match columns.Count with 0 -> old | x -> columns)

        member __.GetRelationships(_,_) = ([||],[||]) // The ODBC type provider does not currently support GetRelationships operations.
        member __.GetSprocs(_) = []

        member __.GetIndividualsQueryText(table,_) =
            sprintf "SELECT * FROM %c%s%c" cOpen table.Name cClose

        member __.GetIndividualQueryText(table,column) =
            let separator = (sprintf "%c.%c" cClose cOpen).Trim()
            sprintf "SELECT * FROM %c%s%c WHERE %c%s%s%s%c = ?" 
                        cOpen table.Name cClose cOpen table.Name separator column cClose

        member this.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns,isDeleteScript, con) =
            let separator = (sprintf "%c.%c" cClose cOpen).Trim()

            let parameters = ResizeArray<_>()
            let createParam (columnDataType:DbType voption) (value:obj) =
                let paramName = "?"
                let p = OdbcParameter(paramName,value):> IDbDataParameter
                match columnDataType with
                | ValueNone -> ()
                | ValueSome colType -> p.DbType <- colType
                p

            let rec fieldNotation (al:alias) (c:SqlColumnType) =
                let buildf (c:Condition)= 
                    let sb = System.Text.StringBuilder()
                    let (~~) (t:string) = sb.Append t |> ignore
                    filterBuilder (~~) [c]
                    sb.ToString()
                let colSprint =
                    match String.IsNullOrEmpty(al) with
                    | true -> fun col -> sprintf "%c%s%c" cOpen col cClose
                    | false -> fun col -> sprintf "%c%s%s%s%c" cOpen al separator col cClose
                match c with
                // Custom database spesific overrides for canonical function:
                | SqlColumnType.CanonicalOperation(cf,col) ->
                    let column = fieldNotation al col
                    match cf with
                    // String functions
                    | Replace(SqlConstant searchItm,SqlCol(al2, col2)) -> sprintf "REPLACE(%s,%s,%s)" column (Utilities.fieldConstant searchItm) (fieldNotation al2 col2)
                    | Replace(SqlCol(al2, col2), SqlConstant toItm) -> sprintf "REPLACE(%s,%s,%s)" column (fieldNotation al2 col2) (Utilities.fieldConstant toItm)
                    | Replace(SqlCol(al2, col2),SqlCol(al3, col3)) -> sprintf "REPLACE(%s,%s,%s)" column (fieldNotation al2 col2) (fieldNotation al3 col3)
                    | Replace(SqlConstant searchItm, SqlConstant toItm) -> sprintf "REPLACE(%s,%s,%s)" column (Utilities.fieldConstant searchItm) (Utilities.fieldConstant toItm)
                    | Substring(SqlConstant startPos) -> sprintf "SUBSTRING(%s, %s)" column (Utilities.fieldConstant startPos)
                    | Substring(SqlCol(al2, col2)) -> sprintf "SUBSTRING(%s, %s)" column (fieldNotation al2 col2)
                    | SubstringWithLength(SqlConstant startPos, SqlConstant strLen) -> sprintf "SUBSTRING(%s, %s, %s)" column (Utilities.fieldConstant startPos) (Utilities.fieldConstant strLen)
                    | SubstringWithLength(SqlConstant startPos,SqlCol(al2, col2)) -> sprintf "SUBSTRING(%s, %s, %s)" column (Utilities.fieldConstant startPos) (fieldNotation al2 col2)
                    | SubstringWithLength(SqlCol(al2, col2), SqlConstant strLen) -> sprintf "SUBSTRING(%s, %s, %s)" column (fieldNotation al2 col2) (Utilities.fieldConstant strLen)
                    | SubstringWithLength(SqlCol(al2, col2),SqlCol(al3, col3)) -> sprintf "SUBSTRING(%s, %s, %s)" column (fieldNotation al2 col2) (fieldNotation al3 col3)
                    | Trim -> sprintf "LTRIM(RTRIM(%s))" column
                    | Length -> sprintf "LENGTH(%s)" column // ODBC 1.0, works with strings only
                    //| Length -> sprintf "CHARACTER_LENGTH(%s)" column // ODBC 3.0, works with all columns
                    | IndexOf(SqlConstant search) -> sprintf "LOCATE(%s,%s)" (Utilities.fieldConstant search) column
                    | IndexOf(SqlCol(al2, col2)) -> sprintf "LOCATE(%s,%s)" (fieldNotation al2 col2) column
                    | IndexOfStart(SqlConstant search,(SqlConstant startPos)) -> sprintf "LOCATE(%s,%s,%s)" (Utilities.fieldConstant search) column (Utilities.fieldConstant startPos)
                    | IndexOfStart(SqlConstant search,SqlCol(al2, col2)) -> sprintf "LOCATE(%s,%s,%s)" (Utilities.fieldConstant search) column (fieldNotation al2 col2)
                    | IndexOfStart(SqlCol(al2, col2),(SqlConstant startPos)) -> sprintf "LOCATE(%s,%s,%s)" (fieldNotation al2 col2) column (Utilities.fieldConstant startPos)
                    | IndexOfStart(SqlCol(al2, col2),SqlCol(al3, col3)) -> sprintf "LOCATE(%s,%s,%s)" (fieldNotation al2 col2) column (fieldNotation al3 col3)
                    | ToUpper -> sprintf "UCASE(%s)" column
                    | ToLower -> sprintf "LCASE(%s)" column
                    | CastVarchar -> sprintf "CONVERT(%s, SQL_VARCHAR)" column
                    | CastInt -> sprintf "CONVERT(%s, INT)" column
                    // Date functions
                    | Date -> sprintf "CONVERT(%s, SQL_DATE)" column
                    | Year -> sprintf "YEAR(%s)" column
                    | Month -> sprintf "MONTH(%s)" column
                    | Day -> sprintf "DAYOFMONTH(%s)" column
                    | Hour -> sprintf "HOUR(%s)" column
                    | Minute -> sprintf "MINUTE(%s)" column
                    | Second -> sprintf "SECOND(%s)" column
                    | DateDiffDays(SqlCol(al2, col2)) -> sprintf "DATEDIFF('d', %s, %s)" (fieldNotation al2 col2) column
                    | DateDiffSecs(SqlCol(al2, col2)) -> sprintf "DATEDIFF('s', %s, %s)" (fieldNotation al2 col2) column
                    | DateDiffDays(SqlConstant x) -> sprintf "DATEDIFF('d', %s, %s)" (Utilities.fieldConstant x) column
                    | DateDiffSecs(SqlConstant x) -> sprintf "DATEDIFF('s', %s, %s)" (Utilities.fieldConstant x) column
                    // Date additions not supported by standard ODBC
                    // Math functions
                    | Truncate -> sprintf "TRUNCATE(%s)" column
                    | BasicMathOfColumns(o, a, c) when o="||" -> sprintf "CONCAT(%s, %s)" column (fieldNotation a c)
                    | BasicMathOfColumns(o, a, c) -> sprintf "(%s %s %s)" column o (fieldNotation a c)
                    | BasicMath(o, par) when (par :? String || par :? Char) -> sprintf "CONCAT(%s, '%O')" column par
                    | BasicMathLeft(o, par) when (par :? String || par :? Char) -> sprintf "CONCAT('%O', %s)" par column 
                    | Greatest(SqlConstant x) -> sprintf "GREATEST(%s, %s)" column (Utilities.fieldConstant x)
                    | Greatest(SqlCol(al2, col2)) -> sprintf "GREATEST(%s, %s)" column (fieldNotation al2 col2)
                    | Least(SqlConstant x) -> sprintf "LEAST(%s, %s)" column (Utilities.fieldConstant x)
                    | Least(SqlCol(al2, col2)) -> sprintf "LEAST(%s, %s)" column (fieldNotation al2 col2)
                    | Pow(SqlCol(al2, col2)) -> sprintf "POWER(%s, %s)" column (fieldNotation al2 col2)
                    | Pow(SqlConstant x) -> sprintf "POWER(%s, %s)" column (Utilities.fieldConstant x)
                    | PowConst(SqlConstant x) -> sprintf "POWER(%s, %s)" (Utilities.fieldConstant x) column
                    //if-then-else
                    | CaseSql(f, SqlCol(al2, col2)) -> sprintf "CASE WHEN %s THEN %s ELSE %s END " (buildf f) column (fieldNotation al2 col2)
                    | CaseSql(f, SqlConstant itm) -> sprintf "CASE WHEN %s THEN %s ELSE %s END " (buildf f) column (Utilities.fieldConstant itm)
                    | CaseNotSql(f, SqlConstant itm) -> sprintf "CASE WHEN %s THEN %s ELSE %s END " (buildf f) (Utilities.fieldConstant itm) column
                    | CaseSqlPlain(Condition.ConstantTrue, itm, _) -> sprintf " %s " (Utilities.fieldConstant itm)
                    | CaseSqlPlain(Condition.ConstantFalse, _, itm2) -> sprintf " %s " (Utilities.fieldConstant itm2)
                    | CaseSqlPlain(f, itm, itm2) -> sprintf "CASE WHEN %s THEN %s ELSE %s END " (buildf f) (Utilities.fieldConstant itm) (Utilities.fieldConstant itm2)
                    | _ -> Utilities.genericFieldNotation (fieldNotation al) colSprint c
                | _ -> Utilities.genericFieldNotation (fieldNotation al) colSprint c

            and filterBuilder (~~) (f:Condition list) =
                // make this nicer later.. just try and get the damn thing to work properly (well, at all) for now :D

                let rec filterBuilder' = function
                    | [] -> ()
                    | (cond::conds) ->
                        let build op preds (rest:Condition list option) =
                            ~~ "("
                            preds |> List.iteri( fun i (alias,col,operator,data) ->
                                    let columnDataType = CommonTasks.searchDataTypeFromCache (this:>ISqlProvider) con sqlQuery baseAlias baseTable alias col
                                    let column = fieldNotation alias col
                                    let extractData data =
                                            match data with
                                            | Some(x) when (box x :? System.Linq.IQueryable) -> [||]
                                            | Some(x) when (box x :? obj array) ->
                                                // in and not in operators pass an array
                                                let strings = box x :?> obj array
                                                strings |> Array.map (createParam columnDataType)
                                            | Some(x) -> [|createParam columnDataType (box x)|]
                                            | None ->    [|createParam columnDataType DBNull.Value|]

                                    let prefix = if i>0 then (sprintf " %s " op) else ""
                                    let paras = extractData data
                                    ~~(sprintf "%s%s" prefix <|
                                        match operator with
                                        | FSharp.Data.Sql.IsNull -> sprintf "%s IS NULL" column
                                        | FSharp.Data.Sql.NotNull -> sprintf "%s IS NOT NULL" column
                                        | FSharp.Data.Sql.In ->
                                            if Array.isEmpty paras then
                                                " (1=0) " // nothing is in the empty set
                                            else
                                                let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                                Array.iter parameters.Add paras
                                                sprintf "%s IN (%s)" column text
                                        | FSharp.Data.Sql.NestedIn when data.IsSome ->
                                            let innersql, innerpars = data.Value |> box :?> string * IDbDataParameter[]
                                            Array.iter parameters.Add innerpars
                                            sprintf "%s IN (%s)" column innersql
                                        | FSharp.Data.Sql.NotIn ->
                                            if Array.isEmpty paras then
                                                " (1=1) "
                                            else
                                                let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                                Array.iter parameters.Add paras
                                                sprintf "%s NOT IN (%s)" column text
                                        | FSharp.Data.Sql.NestedNotIn when data.IsSome ->
                                            let innersql, innerpars = data.Value |> box :?> string * IDbDataParameter[]
                                            Array.iter parameters.Add innerpars
                                            sprintf "%s NOT IN (%s)" column innersql
                                        | FSharp.Data.Sql.NestedExists when data.IsSome ->
                                            let innersql, innerpars = data.Value |> box :?> string * IDbDataParameter[]
                                            Array.iter parameters.Add innerpars
                                            sprintf "EXISTS (%s)" innersql
                                        | FSharp.Data.Sql.NestedNotExists when data.IsSome ->
                                            let innersql, innerpars = data.Value |> box :?> string * IDbDataParameter[]
                                            Array.iter parameters.Add innerpars
                                            sprintf "NOT EXISTS (%s)" innersql
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
                                | [x] when preds.Length > 0 ->
                                    ~~ (sprintf " %s " op)
                                    filterBuilder' [x]
                                | [x] -> filterBuilder' [x]
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

            let fieldNotationAlias(al:alias,col:SqlColumnType) =
                let aliasSprint =
                    match String.IsNullOrEmpty(al) with
                    | true -> fun c -> sprintf "%c%s%c" cOpen c cClose
                    | false -> fun c -> sprintf "%c%s_%s%c" cOpen al c cClose
                Utilities.genericAliasNotation aliasSprint col

            let sb = System.Text.StringBuilder()
            let (~~) (t:string) = sb.Append t |> ignore

            let getTable x =
                match sqlQuery.Aliases.TryFind x with
                | Some(a) -> a
                | None -> baseTable

            let singleEntity = sqlQuery.Aliases.Count = 0

            let selectcolumns =
                if projectionColumns |> Seq.isEmpty then "1" else
                (String.concat ","
                    [|for KeyValue(k,v) in projectionColumns do
                        let cols = (getTable k).FullName
                        let k = if k <> "" then k elif baseAlias <> "" then baseAlias else baseTable.Name
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in schemaCache.Columns.[cols] |> Seq.map (fun c -> c.Key) do
                                if singleEntity then yield sprintf "%c%s%c" cOpen col cClose
                                else 
                                    yield sprintf "%c%s%s%s%c as %c%s_%s%c" cOpen k separator col cClose cOpen k col cClose
                        else
                            for colp in v |> Seq.distinct do
                                match colp with
                                | EntityColumn col ->
                                    if singleEntity then yield sprintf "%c%s%c" cOpen col cClose
                                    else
                                        yield sprintf "%c%s%s%s%c as %c%s_%s%c" cOpen k separator col cClose cOpen k col cClose // F# makes this so easy :)
                                | OperationColumn(n,op) ->
                                    yield sprintf "%s as %c%s%c" (fieldNotation k op) cOpen n cClose|])

            // Cache select-params to match group-by params
            let tmpGrpParams = Dictionary<(alias*SqlColumnType), string>()

            // Create sumBy, minBy, maxBy, ... field columns
            let columns =
                let extracolumns =
                    match sqlQuery.Grouping with
                    | [] -> FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation fieldNotationAlias sqlQuery.AggregateOp
                    | g  -> 
                        let keys = g |> List.collect fst |> List.map(fun (a,c) ->
                            let fn = fieldNotation a c
                            if not (tmpGrpParams.ContainsKey (a,c)) then
                                tmpGrpParams.Add((a,c), fn)
                            if sqlQuery.Aliases.Count < 2 then fn
                            else sprintf "%s as '%s'" fn fn)
                        let aggs = g |> List.collect snd
                        let res2 = FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation fieldNotationAlias aggs |> List.toSeq
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
                    ~~  (sprintf "%s %c%s%c as %c%s%c on "
                            joinType cOpen destTable.Name cClose cOpen destAlias cClose)
                    ~~  (String.concat " AND " ((List.zip data.ForeignKey data.PrimaryKey) |> List.map(fun (foreignKey,primaryKey) ->
                        sprintf "%s = %s "
                            (fieldNotation (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias) foreignKey)
                            (fieldNotation (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias) primaryKey)
                            ))))

            let groupByBuilder groupkeys =
                groupkeys
                |> List.iteri(fun i (alias,column) ->
                    let cname =
                        match tmpGrpParams.TryGetValue((alias,column)) with
                        | true, x -> x
                        | false, _ -> fieldNotation alias column
                    if i > 0 then ~~ ", "
                    ~~ cname)

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) ->
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "%s %s" (fieldNotation alias column) (if not desc then "DESC " else "")))

            // Certain ODBC drivers (excel) don't like special characters in aliases, so we need to strip them
            // or else it will fail
            let stripSpecialCharacters (s:string) =
                String(s.ToCharArray() |> Array.filter(fun c -> Char.IsLetterOrDigit c || c = ' ' || c = '_'))

            if isDeleteScript then
                ~~(sprintf "DELETE FROM %c%s%c " cOpen (baseTable.Name.Replace("\"", "")) cClose)
            else 
                // SELECT
                let columnsFixed = if String.IsNullOrEmpty columns then "*" else columns
                if sqlQuery.Distinct && sqlQuery.Count then
                    let colsAggrs = columns.Split([|" as "|], StringSplitOptions.None)
                    let distColumns =
                        if colsAggrs.Length <= 2 then colsAggrs.[0]
                        else
                            let rec concats h1 tails =
                                match tails with
                                | [] -> h1
                                | h::t -> sprintf "CONCAT(%s,%s)" h1 (concats h t)

                            let rest = colsAggrs |> Seq.filter(fun c -> c.Contains ",") |> Seq.map(fun c -> c.Substring(c.IndexOf(',')+1)) |> Seq.toList
                            concats colsAggrs.[0] rest
                    ~~(sprintf "SELECT COUNT(DISTINCT %s) " distColumns)
                elif sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s " columnsFixed)
                elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
                else  ~~(sprintf "SELECT %s " columnsFixed)
                // FROM
                let bal = if baseAlias = "" then baseTable.Name else baseAlias
                ~~(sprintf "FROM %c%s%c as %c%s%c " cOpen (baseTable.Name.Replace("\"", "")) cClose cOpen (stripSpecialCharacters bal) cClose)
                sqlQuery.CrossJoins |> Seq.iter(fun (a,t) -> ~~(sprintf ", %c%s%c as %c%s%c " cOpen (t.Name.Replace("\"", "")) cClose cOpen (stripSpecialCharacters a) cClose))
            fromBuilder()
            // WHERE
            if sqlQuery.Filters.Length > 0 then
                // each filter is effectively the entire contents of each where clause in the linq query,
                // of which there can be many. Simply turn them all into one big AND expression as that is the
                // only logical way to deal with them.
                let f = [And([],Some sqlQuery.Filters)]
                ~~"WHERE "
                filterBuilder (~~) f

            // GROUP BY
            if sqlQuery.Grouping.Length > 0 then
                let groupkeys = sqlQuery.Grouping |> List.collect fst
                if groupkeys.Length > 0 then
                    ~~" GROUP BY "
                    groupByBuilder groupkeys

            if sqlQuery.HavingFilters.Length > 0 then
                let keys = sqlQuery.Grouping |> List.collect fst

                let f = [And([],Some (sqlQuery.HavingFilters |> CommonTasks.parseHaving fieldNotation keys))]
                ~~" HAVING "
                filterBuilder (~~) f

            // ORDER BY
            if sqlQuery.Ordering.Length > 0 then
                ~~"ORDER BY "
                orderByBuilder()

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

            let sql = sb.ToString()
            (sql,parameters)

        member this.ProcessUpdates(con, entities, transactionOptions, timeout) =
            let sb = Text.StringBuilder()

            CommonTasks.``ensure columns have been loaded`` (this :> ISqlProvider) con entities

            if entities.Count = 0 then 
                ()
            else

            if con.State <> ConnectionState.Open then con.Open()

            use scope = TransactionUtils.ensureTransaction transactionOptions
            try
                // close the connection first otherwise it won't get enlisted into the transaction
                if con.State = ConnectionState.Open then con.Close()
                con.Open()
                // initially supporting update/create/delete of single entities, no hierarchies yet
                CommonTasks.sortEntities entities
                |> Seq.iter(fun e ->
                    match e._State with
                    | Created ->
                        use cmd = createInsertCommand con sb e
                        Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        cmd.ExecuteNonQuery() |> ignore
                        let id = (lastInsertId con).ExecuteScalar()
                        CommonTasks.checkKey schemaCache.PrimaryKeys id e
                        e._State <- Unchanged
                    | Modified fields ->
                        use cmd = createUpdateCommand con sb e fields
                        Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        cmd.ExecuteNonQuery() |> ignore
                        e._State <- Unchanged
                    | Delete ->
                        use cmd = createDeleteCommand con sb e
                        Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        cmd.ExecuteNonQuery() |> ignore
                        // remove the pk to prevent this attempting to be used again
                        (e :> IColumnHolder).SetPkColumnOptionSilent(schemaCache.PrimaryKeys.[(e :> IColumnHolder).Table.FullName], None)
                        e._State <- Deleted
                    | Deleted | Unchanged -> failwithf "Unchanged entity encountered in update list - this should not be possible! (%O)" e)
                if not(isNull scope) then scope.Complete()

            finally
                con.Close()

        member this.ProcessUpdatesAsync(con, entities, transactionOptions, timeout) =
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
                                use cmd = createInsertCommand con sb e
                                Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                let! c = cmd.ExecuteNonQueryAsync()
                                let id = (lastInsertId con).ExecuteScalar()
                                CommonTasks.checkKey schemaCache.PrimaryKeys id e
                                e._State <- Unchanged
                            }
                        | Modified fields ->
                            task {
                                use cmd = createUpdateCommand con sb e fields
                                Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                let! c = cmd.ExecuteNonQueryAsync()
                                e._State <- Unchanged
                            }
                        | Delete ->
                            task {
                                use cmd = createDeleteCommand con sb e
                                Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                let! c = cmd.ExecuteNonQueryAsync()
                                // remove the pk to prevent this attempting to be used again
                                (e :> IColumnHolder).SetPkColumnOptionSilent(schemaCache.PrimaryKeys.[(e :> IColumnHolder).Table.FullName], None)
                                e._State <- Deleted
                            }
                        | Deleted | Unchanged -> failwithf "Unchanged entity encountered in update list - this should not be possible! (%O)" e
                    let! _ = Sql.evaluateOneByOne handleEntity (CommonTasks.sortEntities entities |>Seq.toList)
                    if not(isNull scope) then scope.Complete()

                finally
                    con.Close()
            }
