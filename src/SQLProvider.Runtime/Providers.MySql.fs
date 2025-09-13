namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Data
open FSharp.Data.Sql
open FSharp.Data.Sql.Transactions
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common
#if !REFLECTIONLOAD
#if MYSQLCONNECTOR
open MySqlConnector
#else
open MySql.Data.MySqlClient
#endif
#endif

module MySql =
    let mutable resolutionPath = String.Empty
    let mutable schemas : string[] = [||]
    let mutable referencedAssemblies = [||]

    let assemblyNames = [
        "MySql.Data.dll"; "MySqlConnector.dll"
    ]

    let assembly =
        lazy Reflection.tryLoadAssemblyFrom resolutionPath referencedAssemblies assemblyNames

    let findType name =
        match assembly.Value with
        | Choice1Of2(assembly) ->
            let types, err =
                try assembly.GetTypes(), None
                with | :? System.Reflection.ReflectionTypeLoadException as e ->
                    let msgs = e.LoaderExceptions |> Seq.map(fun e -> e.GetBaseException().Message) |> Seq.distinct
                    let details = "Details: " + Environment.NewLine + String.Join(Environment.NewLine, msgs)
                    let platform = Reflection.getPlatform(Reflection.execAssembly.Force())
                    let errmsg = (e.Message + Environment.NewLine + details + (if platform <> "" then Environment.NewLine +  "Current execution platform: " + platform else ""))
                    if e.Types.Length = 0 then
                        failwith errmsg
                    else e.Types, Some errmsg
            match types |> Array.tryFind(fun t -> (not (isNull t)) && t.Name = name) with
            | Some t -> t
            | None ->
                match err with
                | Some msg -> failwith msg
                | None ->
                    failwith ("Assembly " + assembly.FullName + " found, but it didn't contain expected type " + name +
                                 Environment.NewLine + "Tired to load a dll: " + assembly.CodeBase)

        | Choice2Of2(paths, errors) ->
           let details =
                match errors with
                | [] -> ""
                | x -> Environment.NewLine + "Details: " + Environment.NewLine + String.Join(Environment.NewLine, x)
           failwithf "Unable to resolve assemblies. One of %s (e.g. from Nuget package MySql.Data) must exist in the paths: %s %s %s"
                (String.Join(", ", assemblyNames |> List.toArray))
                Environment.NewLine
                (String.Join(Environment.NewLine, paths |> Seq.filter(String.IsNullOrEmpty >> not)))
                details

    let connectionType =  lazy (findType "MySqlConnection")
    let commandType =     lazy (findType "MySqlCommand")
    let parameterType =   lazy (findType "MySqlParameter")
    //let enumType =        lazy (findType "MySqlDbType")
    let getSchemaMethod = lazy (connectionType.Value.GetMethod("GetSchema",[|typeof<string>; typeof<string[]>|]))
    //let paramEnumCtor   = lazy parameterType.Value.GetConstructor([|typeof<string>;enumType.Value|])
    //let paramObjectCtor = lazy parameterType.Value.GetConstructor([|typeof<string>;typeof<obj>|])

    let getSchema (name:string) (args:string[]) (conn:IDbConnection) =
#if REFLECTIONLOAD && !NETSTANDARD
        getSchemaMethod.Value.Invoke(conn,[|name; args|]) :?> DataTable
#else
        // Initial version of MySQL .Net-Standard doesn't suppot GetSchema()
        try
#if REFLECTIONLOAD
            let cont = connectionType.Value
            cont.GetMethod("GetSchema",[|typeof<string>; typeof<string[]>|]).Invoke(conn,[|name; args|]) :?> DataTable
#else
            (conn :?> MySqlConnection).GetSchema(name, args)
#endif
        with
        | :?  System.Reflection.TargetInvocationException as re when ((not (isNull re.InnerException)) && re.InnerException :? System.NotSupportedException) ->
            let cont = connectionType.Value
            let schemacoll = cont.GetMethod("GetSchemaCollection",[|typeof<string>; typeof<string[]>|]).Invoke(conn,[|name; args|])
            let collType = schemacoll.GetType()
            let name = collType.GetProperty("Name").GetValue(schemacoll,null) :?> string
            let dt = new DataTable(name)
            let cols = collType.GetProperty("Columns").GetValue(schemacoll,null) :?> System.Collections.IEnumerable
            let rows = collType.GetProperty("Rows").GetValue(schemacoll,null) :?> System.Collections.IList
            let colType = findType "SchemaColumn"
            let rowType = findType "MySqlSchemaRow"
            for col in cols do
                dt.Columns.Add(
                    col.GetType().GetProperty("Name").GetValue(col,null) :?> string,
                    col.GetType().GetProperty("Type").GetValue(col,null) :?> Type) |> ignore
            for row in rows do
                let mutable idx = 0
                let xs =
                    let prop = rowType.GetMethod("GetValueForName", (Reflection.BindingFlags.NonPublic ||| Reflection.BindingFlags.Instance))
                    [| for c in cols do
                            idx <- idx + 1
                            let cn = c.GetType().GetProperty("Name").GetValue(c,null) :?> string
                            let r = prop.Invoke(row, [| cn |])
                            if not (isNull r) then yield r
                            else yield box(DBNull.Value)
                    |]
                dt.Rows.Add(xs) |> ignore
            dt
#endif

    let mutable typeMappings = []
    let mutable findClrType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"

    let createCommandParameter sprocCommand (param:QueryParameter) value =
        let mapping = if (not(isNull value)) && (not sprocCommand) then (findClrType (value.GetType().ToString())) else None
        let value = if isNull value then (box System.DBNull.Value) else value

#if REFLECTIONLOAD
        let parameterType =
            parameterType.Value
        let mySqlDbTypeSetter =
            parameterType.GetProperty("MySqlDbType").GetSetMethod()
        let p = Activator.CreateInstance(parameterType,[|box param.Name;value|]) :?> IDbDataParameter

        p.Direction <-  param.Direction

        p.DbType <- (defaultArg mapping param.TypeMapping).DbType
        param.TypeMapping.ProviderType |> ValueOption.iter (fun pt -> mySqlDbTypeSetter.Invoke(p, [|pt|]) |> ignore)
#else
        let pm = new MySqlParameter(param.Name, value)
        pm.Direction <-  param.Direction

        pm.DbType <- (defaultArg mapping param.TypeMapping).DbType
        
        param.TypeMapping.ProviderType |> ValueOption.iter (fun pt -> pm.MySqlDbType <- Enum.ToObject(typeof<MySqlDbType>, pt) :?> MySqlDbType)
        let p = pm :> IDbDataParameter
#endif

        ValueOption.iter (fun l -> p.Size <- l) param.Length
        p

    let createParam name i v =
        match v with
        | null -> QueryParameter.Create(name, i)
        | value ->
            match findClrType (value.GetType().FullName) with
            | None -> QueryParameter.Create(name, i)
            | Some typemap -> QueryParameter.Create(name, i, typemap)

    let fieldNotationAlias(al:alias,col:SqlColumnType) =
        let aliasSprint =
            match String.IsNullOrEmpty(al) with
            | true -> sprintf "'`%s`'"
            | false -> sprintf "'`%s`.`%s`'" al
        Utilities.genericAliasNotation aliasSprint col

    let ripQuotes (str:String) =
        (if str.Contains(" ") then str.Replace("\"","") else str)

    let createTypeMappings con =
        let dt = getSchema "DataTypes" [||] con

        let getDbType(providerType:int) =
#if REFLECTIONLOAD
            let parameterType = parameterType.Value
            let p = Activator.CreateInstance(parameterType,[||]) :?> IDbDataParameter
            let oracleDbTypeSetter = parameterType.GetProperty("MySqlDbType").GetSetMethod()
            let dbTypeGetter = parameterType.GetProperty("DbType").GetGetMethod()
            oracleDbTypeSetter.Invoke(p, [|providerType|]) |> ignore
            dbTypeGetter.Invoke(p, [||]) :?> DbType
#else
            let p = new MySqlParameter()
            p.MySqlDbType <- Enum.ToObject(typeof<MySqlDbType>, providerType) :?> MySqlDbType
            p.DbType
#endif

        let getClrType (input:string) = Utilities.getType(input).ToString()

        let mappings =
            [
                for r in dt.Rows do
                    let clrType = getClrType (string r.["DataType"])
                    let oleDbType =
                        let unsigned = r.["IsUnsigned"]
                        if (not(isNull unsigned)) && unsigned.ToString() <> "" && (unsigned:?>bool) then (string r.["TypeName"]).Replace(" ", "") + " UNSIGNED"
                        else string r.["TypeName"]
                    let providerType = unbox<int> r.["ProviderDbType"]
                    let dbType =
                        if providerType = -1 then // unsupported, like Boolean.
                            if clrType = "BOOL" then getDbType 2 //map to Int16
                            else (enum<DbType>) (-1)
                        else getDbType providerType
                    yield { ProviderTypeName = ValueSome oleDbType; ClrType = clrType; DbType = dbType; ProviderType = ValueSome providerType; }
                yield { ProviderTypeName = ValueSome "cursor"; ClrType = (typeof<SqlEntity[]>).ToString(); DbType = DbType.Object; ProviderType = ValueNone; }
            ]

        let clrMappings =
            mappings
            |> List.map (fun m -> m.ClrType, m)
            |> Map.ofList

        let dbMappings =
            mappings
            |> List.map (fun m -> (match m.ProviderTypeName with ValueSome x -> x | ValueNone -> "").ToLower(), m)
            |> Map.ofList

        typeMappings <- mappings
        findClrType <- clrMappings.TryFind
        findDbType <- dbMappings.TryFind

    let createConnection connectionString =
#if REFLECTIONLOAD
        try
            Activator.CreateInstance(connectionType.Value,[|box connectionString|]) :?> IDbConnection
        with
        | :? System.Reflection.ReflectionTypeLoadException as ex ->
            let errorfiles = ex.LoaderExceptions |> Array.map(fun e -> e.GetBaseException().Message) |> Seq.distinct |> Seq.toArray
            let msg = ex.GetBaseException().Message + "\r\n" + String.Join("\r\n", errorfiles)
            raise(System.Reflection.TargetInvocationException(msg, ex))
        | :? System.Reflection.TargetInvocationException as ex when ((not(isNull ex.InnerException)) && ex.InnerException :? DllNotFoundException) ->
            let platform = Reflection.getPlatform(Reflection.execAssembly.Force())
            let msg = ex.GetBaseException().Message + ", Path: " + (Reflection.listResolutionFullPaths resolutionPath) +
                        (if platform <> "" then Environment.NewLine +  "Current execution platform: " + platform else "")
            raise(System.Reflection.TargetInvocationException(msg, ex))
        | :? System.TypeInitializationException as te when (te.InnerException :? System.Reflection.TargetInvocationException) ->
            let platform = Reflection.getPlatform(Reflection.execAssembly.Force())
            let ex = te.InnerException :?> System.Reflection.TargetInvocationException
            let msg = ex.GetBaseException().Message + ", Path: " + (Reflection.listResolutionFullPaths resolutionPath) +
                        (if platform <> "" then Environment.NewLine +  "Current execution platform: " + platform else "")
            raise(System.Reflection.TargetInvocationException(msg, ex.InnerException))
        | :? System.TypeInitializationException as te when not(isNull te.InnerException) -> raise (te.GetBaseException())
#else
        new MySqlConnection(connectionString) :> IDbConnection
#endif

    let createCommand commandText (connection:IDbConnection) =
#if REFLECTIONLOAD
        Activator.CreateInstance(commandType.Value,[|box commandText;box connection|]) :?> IDbCommand
#else
        new MySqlCommand(commandText, (connection :?> MySqlConnection)) :> IDbCommand
#endif

    let getSprocReturnCols (sparams: QueryParameter list) =
        match sparams |> List.filter (fun p -> p.Direction <> ParameterDirection.Input) with
        | [] ->
            findDbType "cursor"
            |> Option.map (fun m -> QueryParameter.Create("ResultSet",0,m,ParameterDirection.Output))
            |> Option.fold (fun _ p -> [p]) []
        | a -> a

    let getSprocName (row:DataRow) =
        let sprocSchema =
            if row.Table.Columns.Contains("specific_schema") then row.["specific_schema"].ToString()
            elif row.Table.Columns.Contains("routine_schema") then row.["routine_schema"].ToString()
            elif schemas.Length = 1 then schemas |> Seq.head
            else ""
        let procName = (Sql.dbUnboxWithDefault<string> (Guid.NewGuid().ToString()) row.["specific_name"])
        { ProcName = procName; Owner = sprocSchema; PackageName = String.Empty; }

    let getSprocParameters (con:IDbConnection) (name:SprocName) =
        let createSprocParameters (row:DataRow) =
            let dataType = Sql.dbUnbox row.["data_type"]
            let argumentName = Sql.dbUnbox row.["parameter_name"]
            let maxLength =
                let r = Sql.dbUnboxWithDefault<int> -1 row.["character_maximum_length"]
                if r = -1 then ValueNone else ValueSome r

            findDbType dataType
            |> Option.map (fun m ->
                let ordinal_position = Sql.dbUnboxWithDefault<int> 0 row.["ORDINAL_POSITION"]
                let parameter_mode = Sql.dbUnbox<string> row.["PARAMETER_MODE"]
                let returnValue = isNull argumentName && ordinal_position = 0
                let direction =
                    match parameter_mode with
                    | "IN" -> ParameterDirection.Input
                    | "OUT" -> ParameterDirection.Output
                    | "INOUT" -> ParameterDirection.InputOutput
                    | null when returnValue -> ParameterDirection.ReturnValue
                    | a -> failwithf "Direction not supported %s %s" argumentName a
                { Name = if isNull argumentName then "ReturnValue" else argumentName
                  TypeMapping = m
                  Direction = direction
                  Length = maxLength
                  Ordinal = ordinal_position }
            )

        let dbName = (if Array.isEmpty schemas then [|con.Database|] else schemas) |> Array.map(fun s -> "'" + s + "'")

        //This could filter the query using the Sproc name passed in
        Sql.connect con (Sql.executeSqlAsDataTable createCommand (sprintf "SELECT * FROM information_schema.PARAMETERS where SPECIFIC_SCHEMA in (%s)" (String.Join(", ", dbName))))
        |> DataTable.groupBy (fun row -> getSprocName row, createSprocParameters row)
        |> Seq.filter (fun (n, _) -> n.ProcName = name.ProcName)
        |> Seq.collect (snd >> Seq.choose id)
        |> Seq.sortBy (fun x -> x.Ordinal)
        |> Seq.toList

    let getSprocs (con:IDbConnection) =
        getSchema "Procedures" [||] con
        |> DataTable.map (fun row ->
                            let name = getSprocName row
                            match (Sql.dbUnbox<string> row.["routine_type"]).ToUpperInvariant() with
                            | "FUNCTION" -> Root("Functions", Sproc({ Name = name; Params = (fun con -> getSprocParameters con name); ReturnColumns = (fun _ name -> getSprocReturnCols name) }))
                            | "PROCEDURE" ->  Root("Procedures", Sproc({ Name = name; Params = (fun con -> getSprocParameters con name); ReturnColumns = (fun _ name -> getSprocReturnCols name) }))
                            | _ -> Empty
                          )
        |> Seq.toList

    let readParameter (parameter:IDbDataParameter) =
        if isNull parameter then null else
            let par = parameter
            par.Value

    let processReturnColumn reader (outps:(int*IDbDataParameter)[]) (retCol:QueryParameter) =
        match retCol.TypeMapping.ProviderTypeName with
        | ValueSome "cursor" ->
            let result = ResultSet(retCol.Name, Sql.dataReaderToArray reader)
            reader.NextResult() |> ignore
            result
        | _ ->
            match outps |> Array.tryFind (fun (_,p) -> p.ParameterName = retCol.Name) with
            | Some(_,p) -> ScalarResultSet(p.ParameterName, readParameter p)
            | None -> failwithf "Excepted return column %s but could not find it in the parameter set" retCol.Name

    let processReturnColumnAsync reader (outps:(int*IDbDataParameter)[]) (retCol:QueryParameter) =
        task {
            match retCol.TypeMapping.ProviderTypeName with
            | ValueSome "cursor" ->
                let! r = Sql.dataReaderToArrayAsync reader
                let result = ResultSet(retCol.Name, r)
                let! _ = reader.NextResultAsync()
                return result
            | _ ->
                match outps |> Array.tryFind (fun (_,p) -> p.ParameterName = retCol.Name) with
                | Some(_,p) -> return ScalarResultSet(p.ParameterName, readParameter p)
                | None -> return failwithf "Excepted return column %s but could not find it in the parameter set" retCol.Name
        }

    let executeSprocCommandCommon (inputParams:QueryParameter []) (retCols:QueryParameter[]) (values:obj[]) =
        let inputParameters = inputParams |> Array.filter (fun p -> p.Direction = ParameterDirection.Input)

        let outps =
             retCols
             |> Array.map(fun ip ->
                 let p = createCommandParameter true ip null
                 (ip.Ordinal, p))

        let inps =
             inputParameters
             |> Array.mapi(fun i ip ->
                 let p = createCommandParameter true ip values.[i]
                 (ip.Ordinal,p))

        let allParams =
            Array.append outps inps
            |> Array.sortBy fst

        allParams, outps

    let executeSprocCommand (com:IDbCommand) (inputParams:QueryParameter[]) (retCols:QueryParameter[]) (values:obj[]) =

        let allParams, outps = executeSprocCommandCommon inputParams retCols values
        allParams |> Array.iter (fun (_,p) -> com.Parameters.Add(p) |> ignore)

        match retCols with
        | [||] -> com.ExecuteNonQuery() |> ignore; Unit
        | [|retCol|] ->
            use reader = com.ExecuteReader()
            match retCol.TypeMapping.ProviderTypeName with
            | ValueSome "cursor" ->
                let result = SingleResultSet(retCol.Name, Sql.dataReaderToArray reader)
                reader.NextResult() |> ignore
                result
            | _ ->
                match outps |> Array.tryFind (fun (_,p) -> p.ParameterName = retCol.Name) with
                | Some(_,p) -> Scalar(p.ParameterName, readParameter p)
                | None -> failwithf "Excepted return column %s but could not find it in the parameter set" retCol.Name
        | cols ->
            use reader = com.ExecuteReader()
            Set(cols |> Array.map (processReturnColumn reader outps))

    let executeSprocCommandAsync (com:System.Data.Common.DbCommand) (inputParams:QueryParameter[]) (retCols:QueryParameter[]) (values:obj[]) =
        task {
            let allParams, outps = executeSprocCommandCommon inputParams retCols values
            allParams |> Array.iter (fun (_,p) -> com.Parameters.Add(p) |> ignore)

            match retCols with
            | [||] -> let! r = com.ExecuteNonQueryAsync()
                      return Unit
            | [|retCol|] ->
                match retCol.TypeMapping.ProviderTypeName with
                | ValueSome "cursor" ->
                    use! reader = com.ExecuteReaderAsync()
                    let! r = Sql.dataReaderToArrayAsync reader
                    let result = SingleResultSet(retCol.Name, r)
                    let! _ = reader.NextResultAsync()
                    if not reader.IsClosed then reader.Close()
                    return result
                | _ ->
                    match outps |> Array.tryFind (fun (_,p) -> p.ParameterName = retCol.Name) with
                    | Some(_,p) -> return Scalar(p.ParameterName, readParameter p)
                    | None -> return failwithf "Excepted return column %s but could not find it in the parameter set" retCol.Name
            | cols ->
                use! reader = com.ExecuteReaderAsync()
                let! r = cols |> Array.toList |> Sql.evaluateOneByOne (processReturnColumnAsync reader outps)
                if not reader.IsClosed then reader.Close()
                return Set(r)
        }

type internal MySqlProvider(resolutionPath, contextSchemaPath, owner:string, referencedAssemblies) as this =
    let schemaCache = SchemaCache.LoadOrEmpty(contextSchemaPath)
    let myLock = new Object()

    let quotedTableName (table: Table) =
        let quotedFullName = table.QuotedFullName("`", "`")
        quotedFullName.Replace("\"","`").Replace("[","`").Replace("]","`").Replace("``","`")

    let createInsertCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore

        let cmd = (this :> ISqlProvider).CreateCommand(con,"")
        cmd.Connection <- con

        let columnNamesWithValues =
            (([],0),entity.ColumnValues)
            ||> Seq.fold(fun (out,i) (k,v) ->
                let name = sprintf "@param%i" i
                let p = (this :> ISqlProvider).CreateCommandParameter((MySql.createParam name i v),v)
                (k,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray

        let columnNames, values = columnNamesWithValues |> Array.unzip

        sb.Clear() |> ignore
        ~~(sprintf "INSERT INTO %s (%s) VALUES (%s)"
            ((entity :> IColumnHolder).Table |> quotedTableName)
            ("`" + (String.Join("`, `",columnNames)) + "`")
            (String.Join(",",values |> Array.map(fun p -> p.ParameterName))))

        match entity.OnConflict with
        | Throw -> ()
        | Update ->
          ~~(sprintf " ON DUPLICATE KEY UPDATE %s"
                (String.concat "," (columnNamesWithValues |> Array.map(fun (c,p) -> sprintf "`%s`=%s" c p.ParameterName))))
        | DoNothing ->
          ~~(sprintf " ON DUPLICATE KEY UPDATE %s"
                (String.concat "," (columnNamesWithValues |> Array.map(fun (c,_) -> sprintf "`%s`=`%s`" c c))))

        ~~"; SELECT LAST_INSERT_ID();"

        values |> Array.iter (cmd.Parameters.Add >> ignore)
        cmd.CommandText <- sb.ToString()
        cmd

    let createUpdateCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) (changedColumns: string list) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = (this :> ISqlProvider).CreateCommand(con,"")
        cmd.Connection <- con
        let pk =
            match schemaCache.PrimaryKeys.TryGetValue ((entity :> IColumnHolder).Table |> quotedTableName) with
            | true, pk -> pk
            | false, _ -> []
        sb.Clear() |> ignore

        match pk with
        | [x] when changedColumns |> List.exists ((=)x)
            -> failwith "Error - you cannot change the primary key of an entity."
        | _ -> ()

        let pkValues =
            match (entity :> IColumnHolder).GetPkColumnOption<obj> pk with
            | [] -> failwith ("Error - you cannot update an entity that does not have a primary key. (" + ((entity :> IColumnHolder).Table |> quotedTableName) + ")")
            | v -> v

        let data =
            (([],0),changedColumns)
            ||> List.fold(fun (out,i) col ->
                let name = sprintf "@param%i" i
                let p =
                    match (entity :> IColumnHolder).GetColumnOption<obj> col with
                    | Some v -> (this :> ISqlProvider).CreateCommandParameter((MySql.createParam name i v),v)
                    | None -> (this :> ISqlProvider).CreateCommandParameter(QueryParameter.Create(name, i), DBNull.Value)
                (col,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray

        match pk with
        | [] -> ()
        | ks ->
            ~~(sprintf "UPDATE %s SET %s WHERE "
                ((entity :> IColumnHolder).Table |> quotedTableName)
                (String.concat "," (data |> Array.map(fun (c,p) -> sprintf "`%s` = %s" c p.ParameterName ))))
            ~~(String.concat " AND " (ks |> List.mapi(fun i k -> (sprintf "`%s` = @pk%i" k i))) + ";")

        data |> Array.map snd |> Array.iter (cmd.Parameters.Add >> ignore)

        pkValues |> List.iteri(fun i pkValue ->
            let p = (this :> ISqlProvider).CreateCommandParameter((MySql.createParam ("@pk"+i.ToString()) i pkValue),pkValue)
            cmd.Parameters.Add(p) |> ignore)
        cmd.CommandText <- sb.ToString()
        cmd

    let createDeleteCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = (this :> ISqlProvider).CreateCommand(con,"")
        cmd.Connection <- con
        sb.Clear() |> ignore
        let pk =
            match schemaCache.PrimaryKeys.TryGetValue ((entity :> IColumnHolder).Table |> quotedTableName) with
            | true, pk -> pk
            | false, _ -> []
        sb.Clear() |> ignore
        let pkValues =
            match (entity :> IColumnHolder).GetPkColumnOption<obj> pk with
            | [] -> failwith ("Error - you cannot delete an entity that does not have a primary key. (" + ((entity :> IColumnHolder).Table |> quotedTableName) + ")")
            | v -> v

        pkValues |> List.iteri(fun i pkValue ->
            let p = (this :> ISqlProvider).CreateCommandParameter((MySql.createParam ("@id"+i.ToString()) i pkValue),pkValue)
            cmd.Parameters.Add(p) |> ignore)

        match pk with
        | [] -> ()
        | ks ->
            ~~(sprintf "DELETE FROM %s WHERE " ((entity :> IColumnHolder).Table |> quotedTableName))
            ~~(String.concat " AND " (ks |> List.mapi(fun i k -> (sprintf "%s = @id%i" k i))) + ";")
        cmd.CommandText <- sb.ToString()
        cmd

    do
        MySql.resolutionPath <- resolutionPath
        MySql.schemas <- owner.Split(';', ',', ' ', '\n', '\r') |> Array.filter (not << String.IsNullOrWhiteSpace)
#if REFLECTIONLOAD
        MySql.referencedAssemblies <- referencedAssemblies
#endif
    interface ISqlProvider with
        member __.CloseConnectionAfterQuery = true
        member __.DesignConnection = true
        member __.StoredProcedures = true
        member __.GetLockObject() = myLock
        member __.GetTableDescription(con,tableName) =
            let sn = tableName.Substring(0,tableName.LastIndexOf('.'))
            let tn = tableName.Substring(tableName.LastIndexOf('.')+1)
            let baseQuery = @"SELECT TABLE_COMMENT
                                FROM INFORMATION_SCHEMA.TABLES
                                WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table"
            use com = (this:>ISqlProvider).CreateCommand(con,baseQuery)
            com.Parameters.Add((this:>ISqlProvider).CreateCommandParameter(QueryParameter.Create("@schema", 0), sn)) |> ignore
            com.Parameters.Add((this:>ISqlProvider).CreateCommandParameter(QueryParameter.Create("@table", 1), (MySql.ripQuotes tn))) |> ignore
            if con.State <> ConnectionState.Open then con.Open()
            use reader = com.ExecuteReader()
            if reader.Read() then
                let comm = reader.GetString(0)
                if isNull comm then "" else comm
            else ""
        member __.GetColumnDescription(con,tableName,columnName) =
            let sn = tableName.Substring(0,tableName.LastIndexOf('.'))
            let tn = tableName.Substring(tableName.LastIndexOf('.')+1)
            let baseQuery = @"SELECT COLUMN_COMMENT
                                FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table AND COLUMN_NAME = @column"
            use com = (this:>ISqlProvider).CreateCommand(con,baseQuery)
            com.Parameters.Add((this:>ISqlProvider).CreateCommandParameter(QueryParameter.Create("@schema", 0), sn)) |> ignore
            com.Parameters.Add((this:>ISqlProvider).CreateCommandParameter(QueryParameter.Create("@table", 1), (MySql.ripQuotes tn))) |> ignore
            com.Parameters.Add((this:>ISqlProvider).CreateCommandParameter(QueryParameter.Create("@column", 2), (MySql.ripQuotes columnName))) |> ignore
            if con.State <> ConnectionState.Open then con.Open()
            use reader = com.ExecuteReader()
            if reader.Read() then
                let comm = reader.GetString(0)
                if isNull comm then "" else comm 
            else ""
        member __.CreateConnection(connectionString) = MySql.createConnection connectionString
        member __.CreateCommand(connection,commandText) = MySql.createCommand commandText connection
        member __.CreateCommandParameter(param, value) = MySql.createCommandParameter false param value
        member __.ExecuteSprocCommand(com,definition,retCols,values) = MySql.executeSprocCommand com definition retCols values
        member __.ExecuteSprocCommandAsync(com,definition,retCols,values) = MySql.executeSprocCommandAsync com definition retCols values
        member __.CreateTypeMappings(con) = Sql.connect con MySql.createTypeMappings
        member __.GetSchemaCache() = schemaCache

        member __.GetTables(con,cs) =
            let databases = con.Database.Split(';', ',', ' ', '\n', '\r')
            let dbName = (if String.IsNullOrEmpty owner then databases else owner.Split(';', ',', ' ', '\n', '\r'))
                            |> Array.filter (not << String.IsNullOrWhiteSpace)
                            |> Array.map(fun x -> "'" + x + "'")
            let caseChane =
                match cs with
                | Common.CaseSensitivityChange.TOUPPER -> "UPPER(TABLE_SCHEMA)"
                | Common.CaseSensitivityChange.TOLOWER -> "LOWER(TABLE_SCHEMA)"
                | _ -> "TABLE_SCHEMA"
            Sql.connect con (fun con ->
                let executeSql createCommand sql (con:IDbConnection) =
                    use com : IDbCommand = createCommand sql con
                    use reader = com.ExecuteReader()
                    [ while reader.Read() do
                        let table ={ Schema = reader.GetString(0); Name = reader.GetString(1); Type=reader.GetString(2) }
                        yield schemaCache.Tables.GetOrAdd(table |> quotedTableName,table) ] |> List.toArray
                executeSql MySql.createCommand (sprintf "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE from INFORMATION_SCHEMA.TABLES where %s in (%s)" caseChane ((String.concat "," dbName))) con)

        member __.GetPrimaryKey(table) =
            match schemaCache.PrimaryKeys.TryGetValue (table |> quotedTableName) with
            | true, [v] -> Some v
            | _ -> None

        member __.GetColumns(con,table) =
            match schemaCache.Columns.TryGetValue (table |> quotedTableName) with
            | (true,data) when data.Count > 0 -> data
            | _ ->
                // note this data can be obtained using con.GetSchema, but with an epic schema we only want to get the data
                // we are interested in on demand
                let baseQuery = @"SELECT DISTINCTROW c.COLUMN_NAME,c.DATA_TYPE, c.character_maximum_length, c.numeric_precision, c.is_nullable
				                                    ,CASE WHEN ku.COLUMN_NAME IS NOT NULL THEN 'PRIMARY KEY' ELSE '' END AS KeyType, c.COLUMN_TYPE, EXTRA,
                                                     COLUMN_DEFAULT, length(c.generation_expression) > 0
				                  FROM INFORMATION_SCHEMA.COLUMNS c
                                  left JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                                   on (c.TABLE_CATALOG = ku.TABLE_CATALOG OR ku.TABLE_CATALOG IS NULL)
							            AND c.TABLE_SCHEMA = ku.TABLE_SCHEMA
							            AND c.TABLE_NAME = ku.TABLE_NAME
							            AND c.COLUMN_NAME = ku.COLUMN_NAME
                                        and ku.CONSTRAINT_NAME='PRIMARY'
                                  WHERE c.TABLE_SCHEMA = @schema AND c.TABLE_NAME = @table"
                use com = (this:>ISqlProvider).CreateCommand(con,baseQuery)
                com.Parameters.Add((this:>ISqlProvider).CreateCommandParameter(QueryParameter.Create("@schema", 0), table.Schema)) |> ignore
                com.Parameters.Add((this:>ISqlProvider).CreateCommandParameter(QueryParameter.Create("@table", 1), (MySql.ripQuotes table.Name))) |> ignore
                if con.State <> ConnectionState.Open then con.Open()
                use reader = com.ExecuteReader()
                let columns =
                    [ while reader.Read() do
                        let dt = reader.GetString(1)
                        let maxlen =
                            if reader.IsDBNull(2) then ""
                            else reader.GetValue(2).ToString()
                        let isUnsigned = not(reader.IsDBNull 6) && reader.GetString(6).ToUpperInvariant().Contains("UNSIGNED")
                        let udt = if isUnsigned then dt + " unsigned" else dt
                        match MySql.findDbType udt with
                        | Some(m) ->
                            let col =
                                { Column.Name = reader.GetString(0)
                                  TypeMapping = m
                                  IsNullable = let b = reader.GetString(4) in b = "YES"
                                  IsPrimaryKey = reader.GetString(5) = "PRIMARY KEY"
                                  IsAutonumber = reader.GetString(7).Contains("auto_increment")
                                  HasDefault = not(reader.IsDBNull 8)
                                  IsComputed = not(reader.IsDBNull 9)
                                  TypeInfo = if String.IsNullOrEmpty maxlen then ValueSome dt else ValueSome (dt + "(" + maxlen + ")")}
                            if col.IsPrimaryKey then
                                schemaCache.PrimaryKeys.AddOrUpdate(table |> quotedTableName, [col.Name], fun key old ->
                                    match col.Name with
                                    | "" -> old
                                    | x -> match old with
                                           | [] -> [x]
                                           | os -> x::os |> Seq.distinct |> Seq.toList |> List.sort
                                ) |> ignore
                            yield (col.Name,col)
                        | _ -> ()]
                    |> Map.ofList
                con.Close()
                schemaCache.Columns.AddOrUpdate(table |> quotedTableName, columns, fun x old -> match columns.Count with 0 -> old | x -> columns)

        member __.GetRelationships(con,table) =
          schemaCache.Relationships.GetOrAdd(table |> quotedTableName, fun name ->
            let baseQuery = @"SELECT
                                 KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME
                                ,KCU1.TABLE_NAME AS FK_TABLE_NAME
                                ,KCU1.TABLE_SCHEMA AS FK_SCHEMA_NAME
                                ,KCU1.COLUMN_NAME AS FK_COLUMN_NAME
                                ,KCU1.REFERENCED_TABLE_NAME AS REFERENCED_TABLE_NAME
                                ,KCU1.REFERENCED_TABLE_SCHEMA AS REFERENCED_SCHEMA_NAME
                                ,KCU1.REFERENCED_COLUMN_NAME AS FK_CONSTRAINT_SCHEMA
                            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU1
                            WHERE POSITION_IN_UNIQUE_CONSTRAINT is not null"

            let res = Sql.connect con (fun con ->
                use com = (this:>ISqlProvider).CreateCommand(con,(sprintf "%s AND KCU1.TABLE_NAME = @table" baseQuery))
                com.Parameters.Add((this:>ISqlProvider).CreateCommandParameter(QueryParameter.Create("@table", 0), (MySql.ripQuotes table.Name))) |> ignore
                if con.State <> ConnectionState.Open then con.Open()
                use reader = com.ExecuteReader()
                let children =
                    [ while reader.Read() do
                        yield { Name = reader.GetString(0); PrimaryTable=Table.CreateQuotedFullName(reader.GetString(2),reader.GetString(1), "`", "`"); PrimaryKey=reader.GetString(3)
                                ForeignTable=Table.CreateQuotedFullName(reader.GetString(5),reader.GetString(4), "`", "`"); ForeignKey=reader.GetString(6) } ] |> List.toArray
                reader.Dispose()
                use com = (this:>ISqlProvider).CreateCommand(con,(sprintf "%s AND KCU1.REFERENCED_TABLE_NAME = @table" baseQuery))
                com.Parameters.Add((this:>ISqlProvider).CreateCommandParameter(QueryParameter.Create("@table", 0), (MySql.ripQuotes table.Name))) |> ignore
                if con.State <> ConnectionState.Open then con.Open()
                use reader = com.ExecuteReader()
                let parents =
                    [ while reader.Read() do
                        yield { Name = reader.GetString(0); PrimaryTable=Table.CreateQuotedFullName(reader.GetString(2),reader.GetString(1), "`", "`"); PrimaryKey=reader.GetString(3)
                                ForeignTable= Table.CreateQuotedFullName(reader.GetString(5),reader.GetString(4), "`", "`"); ForeignKey=reader.GetString(6) } ] |> List.toArray
                (children,parents))
            res)

        member __.GetSprocs(con) = Sql.connect con MySql.getSprocs
        member __.GetIndividualsQueryText(table,amount) = sprintf "SELECT * FROM %s LIMIT %i;" (table |> quotedTableName) amount
        member __.GetIndividualQueryText(table,column) = sprintf "SELECT * FROM `%s` WHERE `%s`.`%s` = @id" (table |> quotedTableName) (table |> quotedTableName) column

        member this.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns,isDeleteScript, con) =
            let parameters = ResizeArray<_>()
            // make this nicer later.. just try and get the damn thing to work properly (well, at all) for now :D
            // NOTE: really need to assign the parameters their correct sql types
            let param = ref 0
            let nextParam() =
                incr param
                sprintf "@param%i" !param

            let createParamet (columnDataType:DbType voption) (value:obj) =
                let paramName = nextParam()
                let p = MySql.createCommandParameter false (MySql.createParam paramName !param value) value
                match columnDataType with
                | ValueNone -> ()
                | ValueSome colType -> p.DbType <- colType
                p

            let fieldParam (value:obj) =
                let p = createParamet ValueNone value
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
                    | true -> sprintf "`%s`"
                    | false -> sprintf "`%s`.`%s`" al
                match c with
                // Custom database spesific overrides for canonical functions:
                | SqlColumnType.CanonicalOperation(cf,col) ->
                    let column = fieldNotation al col
                    match cf with
                    // String functions
                    | Replace(SqlConstant searchItm,SqlCol(al2, col2)) -> sprintf "REPLACE(%s,%s,%s)" column (fieldParam searchItm) (fieldNotation al2 col2)
                    | Replace(SqlCol(al2, col2), SqlConstant toItm) -> sprintf "REPLACE(%s,%s,%s)" column (fieldNotation al2 col2) (fieldParam toItm)
                    | Replace(SqlCol(al2, col2),SqlCol(al3, col3)) -> sprintf "REPLACE(%s,%s,%s)" column (fieldNotation al2 col2) (fieldNotation al3 col3)
                    | Replace(SqlConstant searchItm, SqlConstant toItm) -> sprintf "REPLACE(%s,%s,%s)" column (fieldParam searchItm) (fieldParam toItm)
                    | Substring(SqlConstant startPos) -> sprintf "MID(%s, %s)" column (fieldParam startPos)
                    | Substring(SqlCol(al2, col2)) -> sprintf "MID(%s, %s)" column (fieldNotation al2 col2)
                    | SubstringWithLength(SqlConstant startPos, SqlConstant strLen) -> sprintf "MID(%s, %s, %s)" column (fieldParam startPos) (fieldParam strLen)
                    | SubstringWithLength(SqlConstant startPos,SqlCol(al2, col2)) -> sprintf "MID(%s, %s, %s)" column (fieldParam startPos) (fieldNotation al2 col2)
                    | SubstringWithLength(SqlCol(al2, col2), SqlConstant strLen) -> sprintf "MID(%s, %s, %s)" column (fieldNotation al2 col2) (fieldParam strLen)
                    | SubstringWithLength(SqlCol(al2, col2),SqlCol(al3, col3)) -> sprintf "MID(%s, %s, %s)" column (fieldNotation al2 col2) (fieldNotation al3 col3)
                    | Trim -> sprintf "TRIM(%s)" column
                    | Length -> sprintf "CHAR_LENGTH(%s)" column
                    | IndexOf(SqlConstant search) -> sprintf "LOCATE(%s,%s)" (fieldParam search) column
                    | IndexOf(SqlCol(al2, col2)) -> sprintf "LOCATE(%s,%s)" (fieldNotation al2 col2) column
                    | IndexOfStart(SqlConstant search,(SqlConstant startPos)) -> sprintf "LOCATE(%s,%s,%s)" (fieldParam search) column (fieldParam startPos)
                    | IndexOfStart(SqlConstant search,SqlCol(al2, col2)) -> sprintf "LOCATE(%s,%s,%s)" (fieldParam search)  column (fieldNotation al2 col2)
                    | IndexOfStart(SqlCol(al2, col2),(SqlConstant startPos)) -> sprintf "LOCATE(%s,%s,%s)" (fieldNotation al2 col2) column (fieldParam startPos)
                    | IndexOfStart(SqlCol(al2, col2),SqlCol(al3, col3)) -> sprintf "LOCATE(%s,%s,%s)" (fieldNotation al2 col2) column (fieldNotation al3 col3)
                    | CastVarchar -> sprintf "CAST(%s AS CHAR)" column
                    | CastInt -> sprintf "CAST(%s AS INT)" column
                    // Date functions
                    | Date -> sprintf "DATE(%s)" column
                    | Year -> sprintf "YEAR(%s)" column
                    | Month -> sprintf "MONTH(%s)" column
                    | Day -> sprintf "DAY(%s)" column
                    | Hour -> sprintf "HOUR(%s)" column
                    | Minute -> sprintf "MINUTE(%s)" column
                    | Second -> sprintf "SECOND(%s)" column
                    | AddYears(SqlConstant x) -> sprintf "DATE_ADD(%s, INTERVAL %s YEAR)" column (fieldParam x)
                    | AddYears(SqlCol(al2, col2)) -> sprintf "DATE_ADD(%s, INTERVAL %s YEAR)" column (fieldNotation al2 col2)
                    | AddMonths x -> sprintf "DATE_ADD(%s, INTERVAL %d MONTH)" column x
                    | AddDays(SqlConstant x) -> sprintf "DATE_ADD(%s, INTERVAL %s DAY)" column (fieldParam x) // SQL ignores decimal part :-(
                    | AddDays(SqlCol(al2, col2)) -> sprintf "DATE_ADD(%s, INTERVAL %s DAY)" column (fieldNotation al2 col2)
                    | AddHours x -> sprintf "DATE_ADD(%s, INTERVAL %f HOUR)" column x
                    | AddMinutes(SqlConstant x) -> sprintf "DATE_ADD(%s, INTERVAL %s MINUTE)" column (fieldParam x)
                    | AddMinutes(SqlCol(al2, col2)) -> sprintf "DATE_ADD(%s, INTERVAL %s MINUTE)" column (fieldNotation al2 col2)
                    | AddSeconds x -> sprintf "DATE_ADD(%s, INTERVAL %f SECOND)" column x
                    | DateDiffDays(SqlCol(al2, col2)) -> sprintf "DATEDIFF(%s, %s)" column (fieldNotation al2 col2)
                    | DateDiffSecs(SqlCol(al2, col2)) -> sprintf "TIMESTAMPDIFF(SECOND, %s, %s)" column (fieldNotation al2 col2)
                    | DateDiffDays(SqlConstant x) -> sprintf "DATEDIFF(%s, %s)" column (fieldParam x)
                    | DateDiffSecs(SqlConstant x) -> sprintf "TIMESTAMPDIFF(SECOND, %s, %s)" column (fieldParam x)
                    // Math functions
                    | Truncate -> sprintf "TRUNCATE(%s)" column
                    | BasicMathOfColumns(o, a, c) when o="||" -> sprintf "CONCAT(%s, %s)" column (fieldNotation a c)
                    | BasicMath(o, par) when (par :? String || par :? Char) -> sprintf "CONCAT(%s, %s)" column (fieldParam par)
                    | BasicMathLeft(o, par) when (par :? String || par :? Char) -> sprintf "CONCAT(%s, %s)" (fieldParam par) column
                    | Greatest(SqlConstant x) -> sprintf "GREATEST(%s, %s)" column (fieldParam x)
                    | Greatest(SqlCol(al2, col2)) -> sprintf "GREATEST(%s, %s)" column (fieldNotation al2 col2)
                    | Least(SqlConstant x) -> sprintf "LEAST(%s, %s)" column (fieldParam x)
                    | Least(SqlCol(al2, col2)) -> sprintf "LEAST(%s, %s)" column (fieldNotation al2 col2)
                    | Pow(SqlCol(al2, col2)) -> sprintf "POWER(%s, %s)" column (fieldNotation al2 col2)
                    | Pow(SqlConstant x) -> sprintf "POWER(%s, %s)" column (fieldParam x)
                    | PowConst(SqlConstant x) -> sprintf "POWER(%s, %s)" (fieldParam x) column
                    //if-then-else
                    | CaseSql(f, SqlCol(al2, col2)) -> sprintf "IF(%s, %s, %s)" (buildf f) column (fieldNotation al2 col2)
                    | CaseSql(f, SqlConstant itm) -> sprintf "IF(%s, %s, %s)" (buildf f) column (fieldParam itm)
                    | CaseNotSql(f, SqlConstant itm) -> sprintf "IF(%s, %s, %s)" (buildf f) (fieldParam itm) column
                    | CaseSqlPlain(Condition.ConstantTrue, itm, _) -> sprintf " %s " (fieldParam itm)
                    | CaseSqlPlain(Condition.ConstantFalse, _, itm2) -> sprintf " %s " (fieldParam itm2)
                    | CaseSqlPlain(f, itm, itm2) -> sprintf "IF(%s,%s,%s)" (buildf f) (fieldParam itm) (fieldParam itm2)

                    | _ -> Utilities.genericFieldNotation (fieldNotation al) colSprint c
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
                                                let elements = box x :?> obj array
                                                Array.init (elements.Length) (elements.GetValue >> createParamet columnDataType)
                                            | Some(x) -> [|createParamet columnDataType (box x)|]
                                            | None ->    [|createParamet columnDataType DBNull.Value|]

                                    let operatorIn operator (array : IDbDataParameter[]) =
                                        if Array.isEmpty array then
                                            match operator with
                                            | FSharp.Data.Sql.In -> "FALSE" // nothing is in the empty set
                                            | FSharp.Data.Sql.NotIn -> "TRUE" // anything is not in the empty set
                                            | _ -> failwithf "Should not be called with any other operator (%O)" operator
                                        else
                                            let text = String.concat "," (array |> Array.map (fun p -> p.ParameterName))
                                            Array.iter parameters.Add array
                                            match operator with
                                            | FSharp.Data.Sql.In -> sprintf "%s IN (%s)" column text
                                            | FSharp.Data.Sql.NotIn -> sprintf "%s NOT IN (%s)" column text
                                            | _ -> failwithf "Should not be called with any other operator (%O)" operator

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
                                        | _ -> failwithf "Should not be called with any other operator (%O)" operator

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

            // to simplfy (ha!) the processing, all tables should be aliased.
            // the LINQ infrastructure will cause this will happen by default if the query includes more than one table
            // if it does not, then we first need to create an alias for the single table
            let getTable x =
                match sqlQuery.Aliases.TryFind x with
                | Some(a) -> a
                | _ -> baseTable

            let singleEntity = sqlQuery.Aliases.Count = 0



            // build the sql query from the simplified abstract query expression
            // working on the basis that we will alias everything to make my life eaiser
            // build the select statment, this is easy ...
            let selectcolumns =
                if projectionColumns |> Seq.isEmpty then "1" else
                (String.concat ","
                    [|for KeyValue(k,v) in projectionColumns do
                        let cols = (getTable k) |> quotedTableName
                        let k = if k <> "" then k elif baseAlias <> "" then baseAlias else baseTable.Name
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in schemaCache.Columns.[cols] |> Seq.map (fun c -> c.Key) do
                                if singleEntity then yield sprintf "`%s`.`%s` as `%s`" k col col
                                else yield sprintf "`%s`.`%s` as '`%s`.`%s`'" k col k col
                        else
                            for colp in v |> Seq.distinct do
                                match colp with
                                | EntityColumn col ->
                                    if singleEntity then yield sprintf "`%s`.`%s` as `%s`" k col col
                                    else yield sprintf "`%s`.`%s` as '`%s`.`%s`'" k col k col // F# makes this so easy :)
                                | OperationColumn(n,op) ->
                                    yield sprintf "%s as `%s`" (fieldNotation k op) n|])

            // Cache select-params to match group-by params
            let tmpGrpParams = Dictionary<(alias*SqlColumnType), string>()

            // Create sumBy, minBy, maxBy, ... field columns
            let columns =
                let extracolumns =
                    match sqlQuery.Grouping with
                    | [] -> FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation MySql.fieldNotationAlias sqlQuery.AggregateOp
                    | g  ->
                        let keys = g |> List.collect fst |> List.map(fun (a,c) ->
                            let fn = fieldNotation a c
                            if not (tmpGrpParams.ContainsKey (a,c)) then
                                tmpGrpParams.Add((a,c), fn)
                            if sqlQuery.Aliases.Count < 2 then fn
                            else sprintf "%s as '%s'" fn fn)
                        let aggs = g |> List.collect snd
                        let res2 = FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation MySql.fieldNotationAlias aggs |> List.toSeq
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
                    ~~  (sprintf "%s `%s`.`%s` as `%s` on "
                            joinType destTable.Schema destTable.Name destAlias)
                    ~~  (String.concat " AND " ((List.zip data.ForeignKey data.PrimaryKey) |> List.map(fun (foreignKey,primaryKey) ->
                        sprintf "%s = %s"
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

            let basetable = baseTable |> quotedTableName
            if isDeleteScript then
                ~~(sprintf "DELETE FROM %s " basetable)
            else
                // SELECT
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
                elif sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s " columns)
                elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
                else  ~~(sprintf "SELECT %s " columns)
                // FROM
                let bal = if baseAlias = "" then baseTable.Name else baseAlias
                ~~(sprintf "FROM %s as `%s` " basetable  bal)
                sqlQuery.CrossJoins |> Seq.iter(fun (a,t) -> ~~(sprintf ",  %s as `%s` " t.Name a))
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

            match sqlQuery.Take, sqlQuery.Skip with
            | ValueSome take, ValueSome skip ->  ~~(sprintf " LIMIT %i OFFSET %i;" take skip)
            | ValueSome take, ValueNone ->  ~~(sprintf " LIMIT %i;" take)
            | ValueNone, ValueSome skip -> ~~(sprintf " LIMIT %i OFFSET %i;" System.UInt64.MaxValue skip)
            | ValueNone, ValueNone -> ()

            let sql = sb.ToString()
            (sql,parameters)

        member this.ProcessUpdates(con, entities, transactionOptions, timeout) =
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
                CommonTasks.sortEntities entities
                |> Seq.iter(fun e ->
                    match e._State with
                    | Created ->
                        use cmd = createInsertCommand con sb e
                        Common.QueryEvents.PublishSqlQueryICol con.ConnectionString cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        let id = cmd.ExecuteScalar()
                        CommonTasks.checkKey schemaCache.PrimaryKeys id e
                        e._State <- Unchanged
                    | Modified fields ->
                        use cmd = createUpdateCommand con sb e fields
                        Common.QueryEvents.PublishSqlQueryICol con.ConnectionString cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        cmd.ExecuteNonQuery() |> ignore
                        e._State <- Unchanged
                    | Delete ->
                        use cmd = createDeleteCommand con sb e
                        Common.QueryEvents.PublishSqlQueryICol con.ConnectionString cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        cmd.ExecuteNonQuery() |> ignore
                        // remove the pk to prevent this attempting to be used again
                        (e :> IColumnHolder).SetPkColumnOptionSilent(schemaCache.PrimaryKeys.[(e :> IColumnHolder).Table |> quotedTableName], None)
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
                                use cmd = createInsertCommand con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol con.ConnectionString cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                let! id = cmd.ExecuteScalarAsync()
                                CommonTasks.checkKey schemaCache.PrimaryKeys id e
                                e._State <- Unchanged
                            }
                        | Modified fields ->
                            task {
                                use cmd = createUpdateCommand con sb e fields :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol con.ConnectionString cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                let! c = cmd.ExecuteNonQueryAsync()
                                e._State <- Unchanged
                            }
                        | Delete ->
                            task {
                                use cmd = createDeleteCommand con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol con.ConnectionString cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                let! c = cmd.ExecuteNonQueryAsync()
                                // remove the pk to prevent this attempting to be used again
                                (e :> IColumnHolder).SetPkColumnOptionSilent(schemaCache.PrimaryKeys.[(e :> IColumnHolder).Table |> quotedTableName], None)
                                e._State <- Deleted
                            }
                        | Deleted | Unchanged -> failwithf "Unchanged entity encountered in update list - this should not be possible! (%O)" e

                    let! _ = Sql.evaluateOneByOne handleEntity (CommonTasks.sortEntities entities |> Seq.toList)

                    if not(isNull scope) then scope.Complete()

                finally
                    con.Close()
            }
