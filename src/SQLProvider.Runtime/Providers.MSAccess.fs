namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Data
open System.Data.OleDb
open System.IO

open FSharp.Data.Sql
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common
#if NETSTANDARD
open StandardExtensions
#endif

type internal MSAccessProvider(contextSchemaPath) =
    let schemaCache = SchemaCache.LoadOrEmpty(contextSchemaPath)
    let myLock = new Object()

    let mutable typeMappings = []
    let mutable findClrType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbTypeByEnum : (int -> TypeMapping option)  = fun _ -> failwith "!"

    let fieldNotationAlias(al:alias,col:SqlColumnType) =
        let aliasSprint =
            match String.IsNullOrEmpty(al) with
            | true -> sprintf "[%s]"
            | false -> sprintf "[%s_%s]" al
        Utilities.genericAliasNotation aliasSprint col

    let createTypeMappings (con:OleDbConnection) =
        if con.State <> ConnectionState.Open then con.Open()
        let dt = con.GetSchema("DataTypes")

        let getDbType(providerType:int) =
            let p = OleDbParameter()
            p.OleDbType <- (Enum.ToObject(typeof<OleDbType>, providerType) :?> OleDbType)
            p.DbType

        let getClrType (input:string) = Utilities.getType(input).ToString()
        let mappings =
            [
                for r in dt.Rows do
                    let clrType = getClrType (string r.["DataType"])
                    let oleDbType = string r.["NativeDataType"]
                    let providerType = unbox<int> r.["ProviderDbType"]
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
            |> List.filter (fun m -> m.ProviderTypeName.IsSome)
            |> List.map (fun m -> (match m.ProviderTypeName with ValueSome x -> x | ValueNone -> ""), m)
            |> Map.ofList

        let enumMappings =
            mappings
            |> List.filter (fun m -> m.ProviderType.IsSome)
            |> List.map (fun m -> m.ProviderType.Value, m)
            |> Map.ofList

        typeMappings <- mappings
        findClrType <- clrMappings.TryFind
        findDbType <- dbMappings.TryFind
        findDbTypeByEnum <- enumMappings.TryFind

    let createInsertCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore

        let cmd = new OleDbCommand()
        cmd.Connection <- con :?> OleDbConnection

        let columnNames, values =
            (([],0),entity.ColumnValues)
            ||> Seq.fold(fun (out,i) (k,v) ->
                let name = sprintf "@param%i" i
                let p = OleDbParameter(name,v)
                (k,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray
            |> Array.unzip

        sb.Clear() |> ignore
        ~~(sprintf "INSERT INTO [%s] (%s) VALUES (%s)"//; SELECT @@IDENTITY;"
            (entity :> IColumnHolder).Table.Name
            (String.concat "," columnNames)
            (String.concat "," (values |> Array.map(fun p -> p.ParameterName))))
        cmd.Parameters.AddRange(values)
        cmd.CommandText <- sb.ToString()
        cmd

    let createUpdateCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) (changedColumns: string list) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = new OleDbCommand()
        cmd.Connection <- con :?> OleDbConnection
        let pk =
            match schemaCache.PrimaryKeys.TryGetValue (entity :> IColumnHolder).Table.FullName with
            | false, _ ->
                failwith("Can't update entity: Table doesn't have a primary key: " + (entity :> IColumnHolder).Table.FullName)
            | true, pkv -> pkv
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
                let name = sprintf "@param%i" i
                let p =
                    match (entity :> IColumnHolder).GetColumnOption<obj> col with
                    | Some v -> OleDbParameter(name,v)
                    | None -> OleDbParameter(name,DBNull.Value)
                (col,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray

        match pk with
        | [] -> ()
        | ks -> 
            ~~(sprintf "UPDATE [%s] SET %s WHERE "
                ((entity :> IColumnHolder).Table.Name.Replace("\"", ""))
                ((String.concat "," (data |> Array.map(fun (c,p) -> sprintf "%s = %s" c p.ParameterName )) )))
            ~~(String.concat " AND " (ks |> List.mapi(fun i k -> (sprintf "%s = @pk%i" k i))))

        cmd.Parameters.AddRange(data |> Array.map snd)
        pkValues |> List.iteri(fun i pkValue ->
            let pkParam = OleDbParameter(("@pk"+i.ToString()), pkValue)
            cmd.Parameters.Add pkParam |> ignore)

        cmd.CommandText <- sb.ToString()
        cmd

    let createDeleteCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = new OleDbCommand()
        cmd.Connection <- con :?> OleDbConnection
        sb.Clear() |> ignore
        let haspk = schemaCache.PrimaryKeys.ContainsKey((entity :> IColumnHolder).Table.FullName)
        let pk = if haspk then schemaCache.PrimaryKeys.[(entity :> IColumnHolder).Table.FullName] else []
        sb.Clear() |> ignore
        let pkValues =
            match (entity :> IColumnHolder).GetPkColumnOption<obj> pk with
            | [] -> failwith ("Error - you cannot delete an entity that does not have a primary key. (" + (entity :> IColumnHolder).Table.FullName + ")")
            | v -> v

        pkValues |> List.iteri(fun i pkValue ->
            cmd.Parameters.AddWithValue(("@id"+i.ToString()),pkValue) |> ignore)

        match pk with
        | [] -> ()
        | ks -> 
            ~~(sprintf "DELETE FROM [%s] WHERE " ((entity :> IColumnHolder).Table.Name.Replace("\"", "")))
            ~~(String.concat " AND " (ks |> List.mapi(fun i k -> (sprintf "%s = @id%i" k i))))

        cmd.CommandText <- sb.ToString()
        cmd

    interface ISqlProvider with
        member __.CloseConnectionAfterQuery = false
        member __.DesignConnection = true
        member __.StoredProcedures = true
        member __.GetLockObject() = myLock
        member __.GetTableDescription(con,tableName) = 
            let t = tableName.Substring(tableName.LastIndexOf('.')+1) 
            let desc = 
                (con:?>OleDbConnection).GetSchema("Tables",[|null;null;t.Replace("\"", "")|]).AsEnumerable() 
                |> Seq.map(fun row ->row.["DESCRIPTION"].ToString()) |> Seq.toList
            match desc with
            | [x] -> x
            | _ -> ""

        member __.GetColumnDescription(con,tableName,columnName) = 
            let t = tableName.Substring(tableName.LastIndexOf('.')+1) 
            let desc = 
                (con:?>OleDbConnection).GetSchema("Columns",[|null;null;t.Replace("\"", "");columnName|]).AsEnumerable() 
                |> Seq.map(fun row ->row.["DESCRIPTION"].ToString())
                |> Seq.toList
            match desc with
            | [x] -> x
            | _ -> ""

        member __.CreateConnection(connectionString) = 
            // Access connections shouldn't ever be closed as that leads to Unspecified Error.
            let con = new OleDbConnection(connectionString)
            upcast con

        member __.CreateCommand(connection,commandText) = upcast new OleDbCommand(commandText,connection:?>OleDbConnection)

        member __.CreateCommandParameter(param, value) =
            let p = OleDbParameter(param.Name,value)
            p.DbType <- param.TypeMapping.DbType
            p.Direction <- param.Direction
            ValueOption.iter (fun l -> p.Size <- l) param.Length
            upcast p

        member __.ExecuteSprocCommand(_,_,_,_) =  raise(NotImplementedException())
        member __.ExecuteSprocCommandAsync(_,_,_,_) =  raise(NotImplementedException())
        member __.CreateTypeMappings(con) = createTypeMappings (con:?>OleDbConnection)
        member __.GetSchemaCache() = schemaCache

        member __.GetTables(con,_) =
            if con.State <> ConnectionState.Open then con.Open()
            let con = con:?>OleDbConnection
            let tables =
                con.GetSchema("Tables").AsEnumerable()
                |> Seq.filter (fun row -> ["TABLE";"VIEW";"LINK"] |> List.exists (fun typ -> typ = row.["TABLE_TYPE"].ToString())) // = "TABLE" || row.["TABLE_TYPE"].ToString() = "VIEW" || row.["TABLE_TYPE"].ToString() = "LINK")  //The text file specification 'A Link Specification' does not exist. You cannot import, export, or link using the specification.
                |> Seq.map (fun row -> let table ={ Schema = Path.GetFileNameWithoutExtension(con.DataSource); Name = row.["TABLE_NAME"].ToString() ; Type=row.["TABLE_TYPE"].ToString() }
                                       schemaCache.Tables.GetOrAdd(table.FullName,table)
                                       )
                |> Array.ofSeq
            tables

        member __.GetPrimaryKey(table) =
            match schemaCache.PrimaryKeys.TryGetValue table.FullName with
            | true, [v] -> Some v
            | _ -> None

        member __.GetColumns(con,table) =
            match schemaCache.Columns.TryGetValue table.FullName with
            | (true,data) when data.Count > 0 -> data
            | _ ->
                if con.State <> ConnectionState.Open then con.Open()
                let pks =
                    (con:?>OleDbConnection).GetSchema("Indexes",[|null;null;null;null;table.Name.Replace("\"", "")|]).AsEnumerable()
                    |> Seq.filter (fun idx ->  bool.Parse(idx.["PRIMARY_KEY"].ToString()))
                    |> Seq.map (fun idx -> idx.["COLUMN_NAME"].ToString())
                    |> Seq.toList

                let columns =
                    (con:?>OleDbConnection).GetSchema("Columns",[|null;null;table.Name.Replace("\"", "");null|]).AsEnumerable()
                    |> Seq.map (fun row ->
                        match row.["DATA_TYPE"].ToString() |> findDbType with
                        |Some(m) ->
                            let pkColumn = pks |> List.exists (fun idx -> idx = row.["COLUMN_NAME"].ToString())
                            let col =
                                { Column.Name = row.["COLUMN_NAME"].ToString();
                                  TypeMapping = m
                                  IsPrimaryKey = pkColumn
                                  IsNullable = bool.Parse(row.["IS_NULLABLE"].ToString())
                                  IsAutonumber = row.["DATA_TYPE"].ToString() = "AutoNumber"
                                  HasDefault = not (row.IsNull "COLUMN_DEFAULT");
                                  IsComputed = false
                                  TypeInfo = 
                                    try 
                                        let ti = 
                                            if row.IsNull("CHARACTER_MAXIMUM_LENGTH") then ""
                                            else row.["CHARACTER_MAXIMUM_LENGTH"].ToString()
                                        if String.IsNullOrEmpty ti then ValueNone
                                        else ValueSome ("Max length: " + ti)
                                    with :? KeyNotFoundException -> ValueNone
                                }
                            (col.Name,col)
                        |_ -> failwith "failed to map datatypes")
                    |> Map.ofSeq

                // only add to PK lookup if it's a single pk - no support for composite keys yet
                match pks with
                | [] -> ()
                | c -> 
                    schemaCache.PrimaryKeys.AddOrUpdate(table.FullName, (c |> List.sort), fun key old -> 
                                match pks.Length with 0 -> old | _ -> (c |> List.sort)) |> ignore
                schemaCache.Columns.AddOrUpdate(table.FullName, columns, fun x old -> match columns.Count with 0 -> old | x -> columns)


        member __.GetRelationships(con,table) =
          schemaCache.Relationships.GetOrAdd(table.FullName, fun name ->
            if con.State <> ConnectionState.Open then con.Open()
            let rels =
                (con:?>OleDbConnection).GetOleDbSchemaTable(OleDbSchemaGuid.Foreign_Keys,[|null|]).AsEnumerable()
            let children = rels |> Seq.filter (fun r -> r.["PK_TABLE_NAME"].ToString() = table.Name)
                                |> Seq.map    (fun r -> let pktableName = table.FullName
                                                        let fktableName = sprintf "[%s].[%s]" table.Schema  (r.["FK_TABLE_NAME"].ToString())
                                                        let name = sprintf "FK_%s_%s" (r.["FK_TABLE_NAME"].ToString()) (r.["PK_TABLE_NAME"].ToString())
                                                        {Name=name;PrimaryTable = pktableName;PrimaryKey=r.["PK_COLUMN_NAME"].ToString();ForeignTable=fktableName;ForeignKey=r.["FK_COLUMN_NAME"].ToString()})
                                |> Array.ofSeq
            let parents  = rels |> Seq.filter (fun r -> r.["FK_TABLE_NAME"].ToString() = table.Name)
                                |> Seq.map    (fun r -> let pktableName = sprintf "[%s].[%s]" table.Schema  (r.["PK_TABLE_NAME"].ToString())
                                                        let fktableName = table.FullName
                                                        let name = sprintf "FK_%s_%s" (r.["FK_TABLE_NAME"].ToString()) (r.["PK_TABLE_NAME"].ToString())
                                                        {Name=name;PrimaryTable = pktableName;PrimaryKey=r.["PK_COLUMN_NAME"].ToString();ForeignTable=fktableName;ForeignKey=r.["FK_COLUMN_NAME"].ToString()})
                                |> Array.ofSeq
            (children,parents))

        member __.GetSprocs(_) = []
        member __.GetIndividualsQueryText(table,amount) = sprintf "SELECT TOP %i * FROM [%s]" amount table.Name
        member __.GetIndividualQueryText(table,column) = sprintf "SELECT * FROM [%s] WHERE [%s] = @id" table.Name column

        member this.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns,isDeleteScript, con) =
            let parameters = ResizeArray<_>()
            // make this nicer later.. just try and get the damn thing to work properly (well, at all) for now :D
            // NOTE: really need to assign the parameters their correct SQL types
            let param = ref 0
            let nextParam() =
                incr param
                sprintf "@param%i" !param

            let createParam (columnDataType:DbType voption) (value:obj) =
                let paramName = nextParam()
                let valu = match value with
                            | :? DateTime as dt -> dt.ToOADate() |> box
                            | _           -> value
                let p = OleDbParameter(paramName,valu):> IDbDataParameter
                match columnDataType with
                | ValueNone -> ()
                | ValueSome colType -> p.DbType <- colType
                p

            let fieldParam (value:obj) =
                let p = createParam ValueNone value
                parameters.Add p
                p.ParameterName

            let rec fieldNotation (al:alias) (c:SqlColumnType) =
                let buildf (c:Condition)= 
                    let sb = System.Text.StringBuilder()
                    let (~~) (t:string) = sb.Append t |> ignore
                    filterBuilder (~~) [c]
                    sb.ToString()
                let colSprint =
                    match String.IsNullOrEmpty(al) with
                    | true -> sprintf "[%s]"
                    | false -> sprintf "[%s].[%s]" al
                match c with
                // Custom database spesific overrides for canonical function:
                | SqlColumnType.CanonicalOperation(cf,col) ->
                    let column = fieldNotation al col
                    match cf with
                    // String functions
                    | Replace(SqlConstant searchItm,SqlCol(al2, col2)) -> sprintf "REPLACE(%s,%s,%s)" column (fieldParam searchItm) (fieldNotation al2 col2)
                    | Replace(SqlCol(al2, col2), SqlConstant toItm) -> sprintf "REPLACE(%s,%s,%s)" column (fieldNotation al2 col2) (fieldParam toItm)
                    | Replace(SqlCol(al2, col2),SqlCol(al3, col3)) -> sprintf "REPLACE(%s,%s,%s)" column (fieldNotation al2 col2) (fieldNotation al3 col3)
                    | Replace(SqlConstant searchItm, SqlConstant toItm) -> sprintf "REPLACE(%s,%s,%s)" column (fieldParam searchItm) (fieldParam toItm)
                    | Substring(SqlConstant startPos) -> sprintf "Mid(%s, %s)" column (fieldParam startPos)
                    | Substring(SqlCol(al2, col2)) -> sprintf "Mid(%s, %s)" column (fieldNotation al2 col2)
                    | SubstringWithLength(SqlConstant startPos, SqlConstant strLen) -> sprintf "Mid(%s, %s, %s)" column (fieldParam startPos) (fieldParam strLen)
                    | SubstringWithLength(SqlConstant startPos,SqlCol(al2, col2)) -> sprintf "Mid(%s, %s, %s)" column (fieldParam startPos) (fieldNotation al2 col2)
                    | SubstringWithLength(SqlCol(al2, col2), SqlConstant strLen) -> sprintf "Mid(%s, %s, %s)" column (fieldNotation al2 col2) (fieldParam strLen)
                    | SubstringWithLength(SqlCol(al2, col2), SqlCol(al3, col3)) -> sprintf "Mid(%s, %s, %s)" column (fieldNotation al2 col2) (fieldNotation al3 col3)
                    | Trim -> sprintf "Trim(%s)" column
                    | Length -> sprintf "Len(%s)" column
                    | IndexOf(SqlConstant search) -> sprintf "InStr(%s,%s)" (fieldParam search) column
                    | IndexOf(SqlCol(al2, col2)) -> sprintf "InStr(%s,%s)" (fieldNotation al2 col2) column
                    | IndexOfStart(SqlConstant search, SqlConstant startPos) -> sprintf "InStr(%s,%s,%s)" (fieldParam startPos) (fieldParam search) column
                    | IndexOfStart(SqlConstant search, SqlCol(al2, col2)) -> sprintf "InStr(%s,%s,%s)" (fieldNotation al2 col2) (fieldParam search) column
                    | IndexOfStart(SqlCol(al2, col2), SqlConstant startPos) -> sprintf "InStr(%s,%s,%s)" (fieldParam startPos) (fieldNotation al2 col2) column
                    | IndexOfStart(SqlCol(al2, col2), SqlCol(al3, col3)) -> sprintf "InStr(%s,%s,%s)" (fieldNotation al3 col3) (fieldNotation al2 col2) column
                    | ToUpper -> sprintf "UCase(%s)" column
                    | ToLower -> sprintf "LCase(%s)" column
                    | CastVarchar -> sprintf "CStr(%s)" column
                    | CastInt -> sprintf "Val(%s)" column
                    // Date functions
                    | Date -> sprintf "DateValue(Format(%s, \"yyyy-mm-dd\"))" column
                    | Year -> sprintf "Year(%s)" column
                    | Month -> sprintf "Month(%s)" column
                    | Day -> sprintf "Day(%s)" column
                    | Hour -> sprintf "Hour(%s)" column
                    | Minute -> sprintf "Minute(%s)" column
                    | Second -> sprintf "Second(%s)" column
                    | AddYears(SqlConstant x) -> sprintf "DateAdd(\"yyyy\", %s, %s)" (fieldParam x) column
                    | AddYears(SqlCol(al2, col2)) -> sprintf "DateAdd(\"yyyy\", %s, %s)" (fieldNotation al2 col2) column
                    | AddMonths x -> sprintf "DateAdd(\"m\", %d, %s)" x column
                    | AddDays(SqlConstant x) -> sprintf "DateAdd(\"d\", %s, %s)" (fieldParam x) column // SQL ignores decimal part :-(
                    | AddDays(SqlCol(al2, col2)) -> sprintf "DateAdd(\"d\", %s, %s)" (fieldNotation al2 col2) column
                    | AddHours x -> sprintf "DateAdd(\"h\", %f, %s)" x column
                    | AddMinutes(SqlConstant x) -> sprintf "DateAdd(\"n\", %s, %s)" (fieldParam x) column
                    | AddMinutes(SqlCol(al2, col2)) -> sprintf "DateAdd(\"n\", %s, %s)" (fieldNotation al2 col2) column
                    | AddSeconds x -> sprintf "DateAdd(\"s\", %f, %s)" x column
                    | DateDiffDays(SqlCol(al2, col2)) -> sprintf "DateDiff('d',%s,%s)" (fieldNotation al2 col2) column
                    | DateDiffSecs(SqlCol(al2, col2)) -> sprintf "DateDiff('s',%s,%s)" (fieldNotation al2 col2) column
                    | DateDiffDays(SqlConstant x) -> sprintf "DateDiff('d',%s,%s)" (fieldParam x) column
                    | DateDiffSecs(SqlConstant x) -> sprintf "DateDiff('s',%s,%s)" (fieldParam x) column
                    // Math functions
                    | Truncate -> sprintf "Fix(%s)" column
                    | Ceil -> sprintf "Fix(%s)+1" column
                    | Floor -> sprintf "Int(%s)" column
                    | Sqrt -> sprintf "Sqr(%s)" column
                    | ATan -> sprintf "Atn(%s)" column
                    | ASin -> sprintf "Atn(%s / Sqr(1 - %s * %s))" column column column
                    | ACos -> sprintf "Atn(-%s / Sqr(-%s * %s + 1)) + 2 * Atn(1)" column column column
                    | BasicMathOfColumns(o, a, c) -> sprintf "(%s %s %s)" column (o.Replace("||", "&")) (fieldNotation a c)
                    | BasicMath(o, par) when (par :? String || par :? Char) -> sprintf "(%s %s %s)" column (o.Replace("||", "&")) (fieldParam par)
                    | BasicMathLeft(o, par) when (par :? String || par :? Char) -> sprintf "(%s %s %s)" (fieldParam par) (o.Replace("||", "&")) column
                    | Greatest(SqlConstant x) -> sprintf "(iif(%s > %s, %s, %s))" (fieldParam x) column (fieldParam x) column
                    | Greatest(SqlCol(al2, col2)) -> sprintf "(iif(%s > %s, %s, %s))" (fieldNotation al2 col2) column (fieldNotation al2 col2) column
                    | Least(SqlConstant x) -> sprintf "(iif(%s < %s, %s, %s)" (fieldParam x) column (fieldParam x) column
                    | Least(SqlCol(al2, col2)) -> sprintf "(iif(%s < %s, %s, %s))" (fieldNotation al2 col2) column (fieldNotation al2 col2) column
                    | Pow(SqlConstant x) -> sprintf "(%s^%s)" column (fieldParam x)
                    | Pow(SqlCol(al2, col2)) -> sprintf "(%s^%s)" column (fieldNotation al2 col2)
                    | PowConst(SqlConstant x) -> sprintf "(%s^%s)" (fieldParam x) column
                    //if-then-else
                    | CaseSql(f, SqlCol(al2, col2)) -> sprintf "iif(%s, %s, %s)" (buildf f) column (fieldNotation al2 col2)
                    | CaseSql(f, SqlConstant itm) -> sprintf "iif(%s, %s, %s)" (buildf f) column (fieldParam itm)
                    | CaseNotSql(f, SqlConstant itm) -> sprintf "iif(%s, %s, %s)" (buildf f) (fieldParam itm) column
                    | CaseSqlPlain(Condition.ConstantTrue, itm, _) -> sprintf " %s " (fieldParam itm)
                    | CaseSqlPlain(Condition.ConstantFalse, _, itm2) -> sprintf " %s " (fieldParam itm2)
                    | CaseSqlPlain(f, itm, itm2) -> sprintf "iif(%s,%s,%s)" (buildf f) (fieldParam itm) (fieldParam itm2)
                    | _ -> Utilities.genericFieldNotation (fieldNotation al) colSprint c
                | GroupColumn (StdDevOp key, KeyColumn _) -> sprintf "STDEV(%s)" (colSprint key)
                | GroupColumn (StdDevOp _,x) -> sprintf "STDEV(%s)" (fieldNotation al x)
                | GroupColumn (VarianceOp key, KeyColumn _) -> sprintf "DVar(%s)" (colSprint key)
                | GroupColumn (VarianceOp _,x) -> sprintf "DVar(%s)" (fieldNotation al x)
                | _ -> Utilities.genericFieldNotation (fieldNotation al) colSprint c
        
            and filterBuilder (~~) (f:Condition list) =
                // the filter expressions
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
                                            let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                            Array.iter parameters.Add paras
                                            sprintf "%s IN (%s)" column text
                                        | FSharp.Data.Sql.NestedIn when data.IsSome ->
                                            let innersql, innerpars = data.Value |> box :?> string * IDbDataParameter[]
                                            Array.iter parameters.Add innerpars
                                            sprintf "%s IN (%s)" column innersql
                                        | FSharp.Data.Sql.NotIn ->
                                            let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                            Array.iter parameters.Add paras
                                            sprintf "%s NOT IN (%s)" column text
                                        | FSharp.Data.Sql.NestedNotIn when data.IsSome ->
                                            let innersql, innerpars = data.Value |> box :?> string * IDbDataParameter[]
                                            Array.iter parameters.Add innerpars
                                            sprintf "%s NOT IN (%s)" column innersql
                                        | FSharp.Data.Sql.NestedExists -> 
                                            let innersql, innerpars = data.Value |> box :?> string * IDbDataParameter[]
                                            Array.iter parameters.Add innerpars
                                            sprintf "EXISTS (%s)" innersql
                                        | FSharp.Data.Sql.NestedNotExists -> 
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

            let sb = System.Text.StringBuilder()
            let (~~) (t:string) = sb.Append t |> ignore

            let getTable x =
                match sqlQuery.Aliases.TryFind x with
                | Some(a) -> a
                | None -> baseTable

            let singleEntity = sqlQuery.Aliases.Count = 0

            // build the select statement, this is easy ...
            let selectcolumns =
                if projectionColumns |> Seq.isEmpty then "1" else
                (String.concat ","
                    [|for KeyValue(k,v) in projectionColumns do
                        let cols = (getTable k).FullName
                        let k = if k <> "" then k elif baseAlias <> "" then baseAlias else baseTable.Name
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in schemaCache.Columns.[cols] |> Seq.map (fun c -> c.Key) do
                                if singleEntity then yield sprintf "[%s].[%s] as [%s]" k col col
                                else yield sprintf "[%s].[%s] as [%s_%s]" k col k col
                        else
                            for colp in v |> Seq.distinct do
                                match colp with
                                | EntityColumn col ->
                                    if singleEntity then yield sprintf "[%s].[%s] as [%s]" k col col
                                    else yield sprintf "[%s].[%s] as [%s_%s]" k col k col // F# makes this so easy :)
                                | OperationColumn(n,op) ->
                                    yield sprintf "%s as [%s]" (fieldNotation k op) n|])

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
                            else sprintf "%s as [%s]" fn fn)
                        let aggs = g |> List.collect snd
                        let res2 = FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation fieldNotationAlias aggs |> List.toSeq
                        [String.Join(", ", keys) + (if List.isEmpty aggs || List.isEmpty keys then ""  else ", ") + String.Join(", ", res2)] 
                match extracolumns with
                | [] when String.IsNullOrEmpty(selectcolumns) -> "*"
                | [] -> selectcolumns
                | h::t -> h

            // next up is the FROM statement which includes joins ..
            let fromBuilder(numLinks:int) =
                sqlQuery.Links
                |> List.iter(fun (fromAlias, data, destAlias)  ->
                    let joinType = if data.OuterJoin then "LEFT JOIN " else "INNER JOIN "
                    let destTable = getTable destAlias
                    ~~  (sprintf "%s [%s] as [%s] on "
                            joinType destTable.Name destAlias)
                    ~~  (String.concat " AND " ((List.zip data.ForeignKey data.PrimaryKey) |> List.map(fun (foreignKey,primaryKey) ->
                        sprintf "%s = %s"
                            (fieldNotation (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias) foreignKey)
                            (fieldNotation (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias) primaryKey)
                            )))
                    if (numLinks > 0)  then ~~ ")")//append close paren after each JOIN, if necessary

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

            //add in 'numLinks' open parens, after FROM, closing each after each JOIN statement
            let numLinks = sqlQuery.Links.Length
            if isDeleteScript then
                ~~(sprintf "DELETE FROM %s[%s] " (String('(',numLinks)) (baseTable.Name.Replace("\"","")))
            else 
                // SELECT
                if sqlQuery.Distinct && sqlQuery.Count then
                    let colsAggrs = columns.Split([|" as "|], StringSplitOptions.None)
                    let distColumns = colsAggrs.[0] + (if colsAggrs.Length = 2 then "" else " & ',' & " + String.Join(" & ',' & ", colsAggrs |> Seq.filter(fun c -> c.Contains ",") |> Seq.map(fun c -> c.Substring(c.IndexOf(',')+1))))
                    ~~(sprintf "SELECT COUNT(DISTINCT %s) " distColumns)
                elif sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s%s " (match sqlQuery.Take with ValueSome v -> sprintf "TOP %i " v | ValueNone -> "") columns)
                elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
                else  ~~(sprintf "SELECT %s%s " (match sqlQuery.Take with ValueSome v -> sprintf "TOP %i " v | ValueNone -> "")   columns)
                // FROM

                let bal = if baseAlias = "" then baseTable.Name else baseAlias
                ~~(sprintf "FROM %s[%s] as [%s] " (String('(',numLinks)) (baseTable.Name.Replace("\"","")) bal)
                sqlQuery.CrossJoins |> Seq.iter(fun (a,t) -> ~~(sprintf ", [%s] as [%s] " (t.Name.Replace("\"","")) a))

            fromBuilder(numLinks)
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

            entities.Keys |> Seq.iter (fun e -> printfn "entity - %A" e.ColumnValues)
            CommonTasks.``ensure columns have been loaded`` (this :> ISqlProvider) con entities

            if entities.Count = 0 then 
                ()
            else

            if con.State = ConnectionState.Closed then con.Open()

            try
                // close the connection first otherwise it won't get enlisted into the transaction
                // ...but if access connection is ever closed, it will start to give unknown errors!
                // if con.State = ConnectionState.Open then con.Close()
                use trnsx = con.BeginTransaction()
                try
                    // initially supporting update/create/delete of single entities, no hierarchies yet
                    (CommonTasks.sortEntities entities |> Seq.toList)
                    |> Seq.iter(fun e ->
                        match e._State with
                        | Created ->
                            use cmd = createInsertCommand con sb e
                            cmd.Transaction <- trnsx :?> OleDbTransaction
                            Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                            if timeout.IsSome then
                                cmd.CommandTimeout <- timeout.Value
                            let id = cmd.ExecuteScalar()
                            CommonTasks.checkKey schemaCache.PrimaryKeys id e
                            e._State <- Unchanged
                        | Modified fields ->
                            use cmd = createUpdateCommand con sb e fields
                            cmd.Transaction <- trnsx :?> OleDbTransaction
                            Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                            if timeout.IsSome then
                                cmd.CommandTimeout <- timeout.Value
                            cmd.ExecuteNonQuery() |> ignore
                            e._State <- Unchanged
                        | Delete ->
                            use cmd = createDeleteCommand con sb e
                            cmd.Transaction <- trnsx :?> OleDbTransaction
                            Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                            if timeout.IsSome then
                                cmd.CommandTimeout <- timeout.Value
                            cmd.ExecuteNonQuery() |> ignore
                            // remove the pk to prevent this attempting to be used again
                            (e :> IColumnHolder).SetPkColumnOptionSilent(schemaCache.PrimaryKeys.[(e :> IColumnHolder).Table.FullName], None)
                            e._State <- Deleted
                        | Deleted | Unchanged -> failwithf "Unchanged entity encountered in update list - this should not be possible! (%O)" e)
                    trnsx.Commit()

                with _ ->
                    trnsx.Rollback()
            finally
                ()
                //con.Close()

        member this.ProcessUpdatesAsync(con, entities, transactionOptions, timeout) =
            let sb = Text.StringBuilder()

            entities.Keys |> Seq.iter (fun e -> printfn "entity - %A" e.ColumnValues)
            CommonTasks.``ensure columns have been loaded`` (this :> ISqlProvider) con entities

            if entities.Count = 0 then 
                task { () }
            else

            try
                // close the connection first otherwise it won't get enlisted into the transaction
                // ...but if access connection is ever closed, it will start to give unknown errors!
                // if con.State = ConnectionState.Open then con.Close()
                task {
                    if con.State <> ConnectionState.Open then
                        do! con.OpenAsync()
                    use trnsx = con.BeginTransaction()
                    try
                        // initially supporting update/create/delete of single entities, no hierarchies yet
                        let handleEntity (e: SqlEntity) =
                            match e._State with
                            | Created ->
                                task {
                                    use cmd = createInsertCommand con sb e
                                    cmd.Transaction <- trnsx :?> OleDbTransaction
                                    Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                                    if timeout.IsSome then
                                        cmd.CommandTimeout <- timeout.Value
                                    let! id = cmd.ExecuteScalarAsync()
                                    CommonTasks.checkKey schemaCache.PrimaryKeys id e
                                    e._State <- Unchanged
                                }
                            | Modified fields ->
                                task {
                                    use cmd = createUpdateCommand con sb e fields
                                    cmd.Transaction <- trnsx :?> OleDbTransaction
                                    Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                                    if timeout.IsSome then
                                        cmd.CommandTimeout <- timeout.Value
                                    let! c = cmd.ExecuteNonQueryAsync()
                                    e._State <- Unchanged
                                }
                            | Delete ->
                                task {
                                    use cmd = createDeleteCommand con sb e
                                    cmd.Transaction <- trnsx :?> OleDbTransaction
                                    Common.QueryEvents.PublishSqlQueryCol con.ConnectionString cmd.CommandText cmd.Parameters
                                    if timeout.IsSome then
                                        cmd.CommandTimeout <- timeout.Value
                                    let! c = cmd.ExecuteNonQueryAsync()
                                    // remove the pk to prevent this attempting to be used again
                                    (e :> IColumnHolder).SetPkColumnOptionSilent(schemaCache.PrimaryKeys.[(e :> IColumnHolder).Table.FullName], None)
                                    e._State <- Deleted
                                }
                            | Deleted | Unchanged -> failwithf "Unchanged entity encountered in update list - this should not be possible! (%O)" e

                        let! _ = Sql.evaluateOneByOne handleEntity (CommonTasks.sortEntities entities |> Seq.toList)
                        trnsx.Commit()

                    with _ ->
                        trnsx.Rollback()
                }
            finally
                //con.Close()
                ()
