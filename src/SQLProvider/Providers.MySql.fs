﻿namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Data
open FSharp.Data.Sql
open FSharp.Data.Sql.Transactions
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common

module MySql =
    let mutable resolutionPath = String.Empty
    let mutable owner = String.Empty
    let mutable referencedAssemblies = [||]

    let assemblyNames = [
        "MySql.Data.dll"
    ]

    let assembly =
        lazy Reflection.tryLoadAssemblyFrom resolutionPath referencedAssemblies assemblyNames

    let findType name =
        match assembly.Value with
        | Choice1Of2(assembly) -> assembly.GetTypes() |> Array.find(fun t -> t.Name = name)
        | Choice2Of2(paths, errors) ->
           let details = 
                match errors with 
                | [] -> "" 
                | x -> Environment.NewLine + "Details: " + Environment.NewLine + String.Join(Environment.NewLine, x)
           failwithf "Unable to resolve assemblies. One of %s (e.g. from Nuget package MySql.Data) must exist in the paths: %s %s %s"
                (String.Join(", ", assemblyNames |> List.toArray))
                Environment.NewLine
                (String.Join(Environment.NewLine, paths |> Seq.filter(fun p -> not(String.IsNullOrEmpty p))))
                details

    let connectionType =  lazy (findType "MySqlConnection")
    let commandType =     lazy (findType "MySqlCommand")
    let parameterType =   lazy (findType "MySqlParameter")
    let enumType =        lazy (findType "MySqlDbType")
    let getSchemaMethod = lazy (connectionType.Value.GetMethod("GetSchema",[|typeof<string>; typeof<string[]>|]))
    let paramEnumCtor   = lazy parameterType.Value.GetConstructor([|typeof<string>;enumType.Value|])
    let paramObjectCtor = lazy parameterType.Value.GetConstructor([|typeof<string>;typeof<obj>|])

    let getSchema name (args:string[]) (conn:IDbConnection) =
        getSchemaMethod.Value.Invoke(conn,[|name; args|]) :?> DataTable

    let mutable typeMappings = []
    let mutable findClrType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"

    let rec fieldNotation (al:alias) (c:SqlColumnType) =
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
            | Replace(SqlStr(searchItm),SqlStrCol(al2, col2)) -> sprintf "REPLACE(%s,'%s',%s)" column searchItm (fieldNotation al2 col2)
            | Replace(SqlStrCol(al2, col2),SqlStr(toItm)) -> sprintf "REPLACE(%s,%s,'%s')" column (fieldNotation al2 col2) toItm
            | Replace(SqlStrCol(al2, col2),SqlStrCol(al3, col3)) -> sprintf "REPLACE(%s,%s,%s)" column (fieldNotation al2 col2) (fieldNotation al3 col3)
            | Substring(SqlInt startPos) -> sprintf "MID(%s, %i)" column startPos
            | Substring(SqlIntCol(al2, col2)) -> sprintf "MID(%s, %s)" column (fieldNotation al2 col2)
            | SubstringWithLength(SqlInt startPos,SqlInt strLen) -> sprintf "MID(%s, %i, %i)" column startPos strLen
            | SubstringWithLength(SqlInt startPos,SqlIntCol(al2, col2)) -> sprintf "MID(%s, %i, %s)" column startPos (fieldNotation al2 col2)
            | SubstringWithLength(SqlIntCol(al2, col2),SqlInt strLen) -> sprintf "MID(%s, %s, %i)" column (fieldNotation al2 col2) strLen
            | SubstringWithLength(SqlIntCol(al2, col2),SqlIntCol(al3, col3)) -> sprintf "MID(%s, %s, %s)" column (fieldNotation al2 col2) (fieldNotation al3 col3)
            | Trim -> sprintf "TRIM(%s)" column
            | Length -> sprintf "CHAR_LENGTH(%s)" column
            | IndexOf(SqlStr search) -> sprintf "LOCATE('%s',%s)" search column
            | IndexOf(SqlStrCol(al2, col2)) -> sprintf "LOCATE(%s,%s)" (fieldNotation al2 col2) column
            | IndexOfStart(SqlStr(search),(SqlInt startPos)) -> sprintf "LOCATE('%s',%s,%d)" search column startPos
            | IndexOfStart(SqlStr(search),SqlIntCol(al2, col2)) -> sprintf "LOCATE('%s',%s,%s)" search column (fieldNotation al2 col2)
            | IndexOfStart(SqlStrCol(al2, col2),(SqlInt startPos)) -> sprintf "LOCATE(%s,%s,%d)" (fieldNotation al2 col2) column startPos
            | IndexOfStart(SqlStrCol(al2, col2),SqlIntCol(al3, col3)) -> sprintf "LOCATE(%s,%s,%s)" (fieldNotation al2 col2) column (fieldNotation al3 col3)
            // Date functions
            | Date -> sprintf "DATE(%s)" column
            | Year -> sprintf "YEAR(%s)" column
            | Month -> sprintf "MONTH(%s)" column
            | Day -> sprintf "DAY(%s)" column
            | Hour -> sprintf "HOUR(%s)" column
            | Minute -> sprintf "MINUTE(%s)" column
            | Second -> sprintf "SECOND(%s)" column
            | AddYears(SqlInt x) -> sprintf "DATE_ADD(%s, INTERVAL %d YEAR)" column x
            | AddYears(SqlIntCol(al2, col2)) -> sprintf "DATE_ADD(%s, INTERVAL %s YEAR)" column (fieldNotation al2 col2)
            | AddMonths x -> sprintf "DATE_ADD(%s, INTERVAL %d MONTH)" column x
            | AddDays(SqlFloat x) -> sprintf "DATE_ADD(%s, INTERVAL %f DAY)" column x // SQL ignores decimal part :-(
            | AddDays(SqlNumCol(al2, col2)) -> sprintf "DATE_ADD(%s, INTERVAL %s DAY)" column (fieldNotation al2 col2)
            | AddHours x -> sprintf "DATE_ADD(%s, INTERVAL %f HOUR)" column x
            | AddMinutes(SqlFloat x) -> sprintf "DATE_ADD(%s, INTERVAL %f MINUTE)" column x
            | AddMinutes(SqlNumCol(al2, col2)) -> sprintf "DATE_ADD(%s, INTERVAL %s MINUTE)" column (fieldNotation al2 col2)
            | AddSeconds x -> sprintf "DATE_ADD(%s, INTERVAL %f SECOND)" column x
            // Math functions
            | Truncate -> sprintf "TRUNCATE(%s)" column
            | BasicMathOfColumns(o, a, c) when o="||" -> sprintf "CONCAT(%s, %s)" column (fieldNotation a c)
            | BasicMath(o, par) when (par :? String || par :? Char) -> sprintf "CONCAT(%s, '%O')" column par
            | _ -> Utilities.genericFieldNotation (fieldNotation al) colSprint c
        | _ -> Utilities.genericFieldNotation (fieldNotation al) colSprint c

    let fieldNotationAlias(al:alias,col:SqlColumnType) =
        let aliasSprint =
            match String.IsNullOrEmpty(al) with
            | true -> sprintf "'`%s`'"
            | false -> sprintf "'`%s`.`%s`'" al
        Utilities.genericAliasNotation aliasSprint col

    let ripQuotes (str:String) = 
        (if str.Contains(" ") then str.Replace("\"","") else str)

    let createParam name i v = 
        match v with
        | null -> QueryParameter.Create(name, i) 
        | value -> 
            match findClrType (value.GetType().FullName) with
            | None -> QueryParameter.Create(name, i) 
            | Some typemap -> QueryParameter.Create(name, i, typemap)

    let createTypeMappings con =
        let dt = getSchema "DataTypes" [||] con

        let getDbType(providerType:int) =
            let parameterType = parameterType.Value
            let p = Activator.CreateInstance(parameterType,[||]) :?> IDbDataParameter
            let oracleDbTypeSetter = parameterType.GetProperty("MySqlDbType").GetSetMethod()
            let dbTypeGetter = parameterType.GetProperty("DbType").GetGetMethod()
            oracleDbTypeSetter.Invoke(p, [|providerType|]) |> ignore
            dbTypeGetter.Invoke(p, [||]) :?> DbType

        let getClrType (input:string) = Type.GetType(input).ToString()

        let mappings =
            [
                for r in dt.Rows do
                    let clrType = getClrType (string r.["DataType"])
                    let oleDbType =
                        let format = (string r.["CreateFormat"])
                        if format <> null && format.ToUpper().Contains("UNSIGNED") then format
                        else  string r.["TypeName"]
                    let providerType = unbox<int> r.["ProviderDbType"]
                    let dbType = getDbType providerType
                    yield { ProviderTypeName = Some oleDbType; ClrType = clrType; DbType = dbType; ProviderType = Some providerType; }
                yield { ProviderTypeName = Some "cursor"; ClrType = (typeof<SqlEntity[]>).ToString(); DbType = DbType.Object; ProviderType = None; }
            ]

        let clrMappings =
            mappings
            |> List.map (fun m -> m.ClrType, m)
            |> Map.ofList

        let dbMappings =
            mappings
            |> List.map (fun m -> m.ProviderTypeName.Value.ToLower(), m)
            |> Map.ofList

        typeMappings <- mappings
        findClrType <- clrMappings.TryFind
        findDbType <- dbMappings.TryFind

    let createConnection connectionString =
        try
            Activator.CreateInstance(connectionType.Value,[|box connectionString|]) :?> IDbConnection
        with
        | :? System.Reflection.ReflectionTypeLoadException as ex ->
            let errorfiles = ex.LoaderExceptions |> Array.map(fun e -> e.Message) |> Seq.distinct |> Seq.toArray
            let msg = ex.Message + "\r\n" + String.Join("\r\n", errorfiles)
            raise(new System.Reflection.TargetInvocationException(msg, ex))
        | :? System.Reflection.TargetInvocationException as ex when (ex.InnerException <> null && ex.InnerException :? DllNotFoundException) ->
            let msg = ex.InnerException.Message + ", Path: " + (System.IO.Path.GetFullPath resolutionPath)
            raise(new System.Reflection.TargetInvocationException(msg, ex))

    let createCommand commandText connection =
        Activator.CreateInstance(commandType.Value,[|box commandText;box connection|]) :?> IDbCommand

    let createCommandParameter sprocCommand (param:QueryParameter) value =
        let mapping = if value <> null && (not sprocCommand) then (findClrType (value.GetType().ToString())) else None
        let value = if value = null then (box System.DBNull.Value) else value

        let parameterType = parameterType.Value
        let mySqlDbTypeSetter =
            parameterType.GetProperty("MySqlDbType").GetSetMethod()

        let p = Activator.CreateInstance(parameterType,[|box param.Name;value|]) :?> IDbDataParameter

        p.Direction <-  param.Direction

        p.DbType <- (defaultArg mapping param.TypeMapping).DbType
        param.TypeMapping.ProviderType |> Option.iter (fun pt -> mySqlDbTypeSetter.Invoke(p, [|pt|]) |> ignore)

        Option.iter (fun l -> p.Size <- l) param.Length
        p

    let getSprocReturnCols (sparams: QueryParameter list) =
        match sparams |> List.filter (fun p -> p.Direction <> ParameterDirection.Input) with
        | [] ->
            findDbType "cursor"
            |> Option.map (fun m -> QueryParameter.Create("ResultSet",0,m,ParameterDirection.Output))
            |> Option.fold (fun _ p -> [p]) []
        | a -> a

    let getSprocName (row:DataRow) =
        let defaultValue =
            if row.Table.Columns.Contains("specific_schema") then row.["specific_schema"]
            else row.["routine_schema"]
        let owner = Sql.dbUnboxWithDefault<string> owner defaultValue
        let procName = (Sql.dbUnboxWithDefault<string> (Guid.NewGuid().ToString()) row.["specific_name"])
        { ProcName = procName; Owner = owner; PackageName = String.Empty; }

    let getSprocParameters (con:IDbConnection) (name:SprocName) =
        let createSprocParameters (row:DataRow) =
            let dataType = Sql.dbUnbox row.["data_type"]
            let argumentName = Sql.dbUnbox row.["parameter_name"]
            let maxLength =
                let r = Sql.dbUnboxWithDefault<int> -1 row.["character_maximum_length"]
                if r = -1 then None else Some r

            findDbType dataType
            |> Option.map (fun m ->
                let ordinal_position = Sql.dbUnboxWithDefault<int> 0 row.["ORDINAL_POSITION"]
                let parameter_mode = Sql.dbUnbox<string> row.["PARAMETER_MODE"]
                let returnValue = argumentName = null && ordinal_position = 0
                let direction =
                    match parameter_mode with
                    | "IN" -> ParameterDirection.Input
                    | "OUT" -> ParameterDirection.Output
                    | "INOUT" -> ParameterDirection.InputOutput
                    | null when returnValue -> ParameterDirection.ReturnValue
                    | a -> failwithf "Direction not supported %s %s" argumentName a
                { Name = if argumentName = null then "ReturnValue" else argumentName
                  TypeMapping = m
                  Direction = direction
                  Length = maxLength
                  Ordinal = ordinal_position }
            )

        let dbName = if String.IsNullOrEmpty owner then con.Database else owner

        //This could filter the query using the Sproc name passed in
        Sql.connect con (Sql.executeSqlAsDataTable createCommand (sprintf "SELECT * FROM information_schema.PARAMETERS where SPECIFIC_SCHEMA = '%s'" dbName))
        |> DataTable.groupBy (fun row -> getSprocName row, createSprocParameters row)
        |> Seq.filter (fun (n, _) -> n.ProcName = name.ProcName)
        |> Seq.collect (snd >> Seq.choose id)
        |> Seq.sortBy (fun x -> x.Ordinal)
        |> Seq.toList

    let getSprocs (con:IDbConnection) =
        getSchema "Procedures" [||] con
        |> DataTable.map (fun row ->
                            let name = getSprocName row
                            match (Sql.dbUnbox<string> row.["routine_type"]).ToUpper() with
                            | "FUNCTION" -> Root("Functions", Sproc({ Name = name; Params = (fun con -> getSprocParameters con name); ReturnColumns = (fun _ name -> getSprocReturnCols name) }))
                            | "PROCEDURE" ->  Root("Procedures", Sproc({ Name = name; Params = (fun con -> getSprocParameters con name); ReturnColumns = (fun _ name -> getSprocReturnCols name) }))
                            | _ -> Empty
                          )
        |> Seq.toList

    let readParameter (parameter:IDbDataParameter) =
        if parameter <> null then
            let par = parameter
            par.Value
        else null

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

        let processReturnColumn reader (retCol:QueryParameter) =
            match retCol.TypeMapping.ProviderTypeName with
            | Some "cursor" ->
                let result = ResultSet(retCol.Name, Sql.dataReaderToArray reader)
                reader.NextResult() |> ignore
                result
            | _ ->
                match outps |> Array.tryFind (fun (_,p) -> p.ParameterName = retCol.Name) with
                | Some(_,p) -> ScalarResultSet(p.ParameterName, readParameter p)
                | None -> failwithf "Excepted return column %s but could not find it in the parameter set" retCol.Name

        allParams, processReturnColumn, outps

    let executeSprocCommand (com:IDbCommand) (inputParams:QueryParameter[]) (retCols:QueryParameter[]) (values:obj[]) =

        let allParams, processReturnColumn, outps = executeSprocCommandCommon inputParams retCols values
        allParams |> Array.iter (fun (_,p) -> com.Parameters.Add(p) |> ignore)

        match retCols with
        | [||] -> com.ExecuteNonQuery() |> ignore; Unit
        | [|retCol|] ->
            use reader = com.ExecuteReader()
            match retCol.TypeMapping.ProviderTypeName with
            | Some "cursor" ->
                let result = SingleResultSet(retCol.Name, Sql.dataReaderToArray reader)
                reader.NextResult() |> ignore
                result
            | _ ->
                match outps |> Array.tryFind (fun (_,p) -> p.ParameterName = retCol.Name) with
                | Some(_,p) -> Scalar(p.ParameterName, readParameter p)
                | None -> failwithf "Excepted return column %s but could not find it in the parameter set" retCol.Name
        | cols ->
            use reader = com.ExecuteReader()
            Set(cols |> Array.map (processReturnColumn reader))

    let executeSprocCommandAsync (com:System.Data.Common.DbCommand) (inputParams:QueryParameter[]) (retCols:QueryParameter[]) (values:obj[]) =
        async {
            let allParams, processReturnColumn, outps = executeSprocCommandCommon inputParams retCols values
            allParams |> Array.iter (fun (_,p) -> com.Parameters.Add(p) |> ignore)

            match retCols with
            | [||] -> do! com.ExecuteNonQueryAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
                      return Unit
            | [|retCol|] ->
                use! reader = com.ExecuteReaderAsync() |> Async.AwaitTask
                match retCol.TypeMapping.ProviderTypeName with
                | Some "cursor" ->
                    let result = SingleResultSet(retCol.Name, Sql.dataReaderToArray reader)
                    reader.NextResult() |> ignore
                    return result
                | _ ->
                    match outps |> Array.tryFind (fun (_,p) -> p.ParameterName = retCol.Name) with
                    | Some(_,p) -> return Scalar(p.ParameterName, readParameter p)
                    | None -> return failwithf "Excepted return column %s but could not find it in the parameter set" retCol.Name
            | cols ->
                use! reader = com.ExecuteReaderAsync() |> Async.AwaitTask
                return Set(cols |> Array.map (processReturnColumn reader))
        }

type internal MySqlProvider(resolutionPath, owner, referencedAssemblies) as this =
    let pkLookup = ConcurrentDictionary<string,string list>()
    let tableLookup = ConcurrentDictionary<string,Table>()
    let columnLookup = ConcurrentDictionary<string,ColumnLookup>()
    let relationshipLookup = ConcurrentDictionary<string,Relationship list * Relationship list>()

    let createInsertCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore

        let cmd = (this :> ISqlProvider).CreateCommand(con,"")
        cmd.Connection <- con

        let columnNames, values =
            (([],0),entity.ColumnValues)
            ||> Seq.fold(fun (out,i) (k,v) ->
                let name = sprintf "@param%i" i
                let p = (this :> ISqlProvider).CreateCommandParameter((MySql.createParam name i v),v)
                (k,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray
            |> Array.unzip

        sb.Clear() |> ignore
        ~~(sprintf "INSERT INTO %s (%s) VALUES (%s); SELECT LAST_INSERT_ID();"
            (entity.Table.FullName.Replace("\"","`").Replace("[","`").Replace("]","`").Replace("``","`"))
            ("`" + (String.Join("`, `",columnNames)) + "`")
            (String.Join(",",values |> Array.map(fun p -> p.ParameterName))))

        values |> Array.iter (cmd.Parameters.Add >> ignore)
        cmd.CommandText <- sb.ToString()
        cmd

    let createUpdateCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) (changedColumns: string list) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = (this :> ISqlProvider).CreateCommand(con,"")
        cmd.Connection <- con
        let haspk = pkLookup.ContainsKey(entity.Table.FullName)
        let pk = if haspk then pkLookup.[entity.Table.FullName] else []
        sb.Clear() |> ignore

        match pk with
        | [x] when changedColumns |> List.exists ((=)x)
            -> failwith "Error - you cannot change the primary key of an entity."
        | _ -> ()

        let pkValues =
            match entity.GetPkColumnOption<obj> pk with
            | [] -> failwith ("Error - you cannot update an entity that does not have a primary key. (" + entity.Table.FullName + ")")
            | v -> v

        let data =
            (([],0),changedColumns)
            ||> List.fold(fun (out,i) col ->
                let name = sprintf "@param%i" i
                let p =
                    match entity.GetColumnOption<obj> col with
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
                (entity.Table.FullName.Replace("\"","`").Replace("[","`").Replace("]","`").Replace("``","`"))
                (String.Join(",", data |> Array.map(fun (c,p) -> sprintf "`%s` = %s" c p.ParameterName ))))
            ~~(String.Join(" AND ", ks |> List.mapi(fun i k -> (sprintf "`%s` = @pk%i" k i))) + ";")

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
        let haspk = pkLookup.ContainsKey(entity.Table.FullName)
        let pk = if haspk then pkLookup.[entity.Table.FullName] else []
        sb.Clear() |> ignore
        let pkValues =
            match entity.GetPkColumnOption<obj> pk with
            | [] -> failwith ("Error - you cannot delete an entity that does not have a primary key. (" + entity.Table.FullName + ")")
            | v -> v

        pkValues |> List.iteri(fun i pkValue ->
            let p = (this :> ISqlProvider).CreateCommandParameter((MySql.createParam ("@id"+i.ToString()) i pkValue),pkValue)
            cmd.Parameters.Add(p) |> ignore)

        match pk with
        | [] -> ()
        | ks -> 
            ~~(sprintf "DELETE FROM %s WHERE " (entity.Table.FullName.Replace("\"","`").Replace("[","`").Replace("]","`").Replace("``","`")))
            ~~(String.Join(" AND ", ks |> List.mapi(fun i k -> (sprintf "%s = @id%i" k i))) + ";")
        cmd.CommandText <- sb.ToString()
        cmd

    do
        MySql.resolutionPath <- resolutionPath
        MySql.owner <- owner
        MySql.referencedAssemblies <- referencedAssemblies

    interface ISqlProvider with
        member __.GetTableDescription(con,tableName) = 
            let sn = tableName.Substring(0,tableName.LastIndexOf(".")) 
            let tn = tableName.Substring(tableName.LastIndexOf(".")+1) 
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
                if comm <> null then comm else ""
            else ""
        member __.GetColumnDescription(con,tableName,columnName) = 
            let sn = tableName.Substring(0,tableName.LastIndexOf(".")) 
            let tn = tableName.Substring(tableName.LastIndexOf(".")+1) 
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
                if comm <> null then comm else ""
            else ""
        member __.CreateConnection(connectionString) = MySql.createConnection connectionString
        member __.CreateCommand(connection,commandText) = MySql.createCommand commandText connection
        member __.CreateCommandParameter(param, value) = MySql.createCommandParameter false param value
        member __.ExecuteSprocCommand(com,definition,retCols,values) = MySql.executeSprocCommand com definition retCols values
        member __.ExecuteSprocCommandAsync(com,definition,retCols,values) = MySql.executeSprocCommandAsync com definition retCols values
        member __.CreateTypeMappings(con) = Sql.connect con MySql.createTypeMappings

        member __.GetTables(con,cs) =
            let dbName = if String.IsNullOrEmpty owner then con.Database else owner
            let caseChane =
                match cs with
                | Common.CaseSensitivityChange.TOUPPER -> "UPPER(TABLE_SCHEMA)"
                | Common.CaseSensitivityChange.TOLOWER -> "LOWER(TABLE_SCHEMA)"
                | _ -> "TABLE_SCHEMA"
            Sql.connect con (fun con ->
                use reader = Sql.executeSql MySql.createCommand (sprintf "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE from INFORMATION_SCHEMA.TABLES where %s = '%s'" caseChane dbName) con
                [ while reader.Read() do
                    let table ={ Schema = reader.GetString(0); Name = reader.GetString(1); Type=reader.GetString(2) }
                    yield tableLookup.GetOrAdd(table.FullName,table) ])

        member __.GetPrimaryKey(table) =
            match pkLookup.TryGetValue table.FullName with
            | true, [v] -> Some v
            | _ -> None

        member __.GetColumns(con,table) =
            match columnLookup.TryGetValue table.FullName with
            | (true,data) when data.Count > 0 -> data
            | _ ->
                // note this data can be obtained using con.GetSchema, but with an epic schema we only want to get the data
                // we are interested in on demand
                let baseQuery = @"SELECT DISTINCTROW c.COLUMN_NAME,c.DATA_TYPE, c.character_maximum_length, c.numeric_precision, c.is_nullable
				                                    ,CASE WHEN ku.COLUMN_NAME IS NOT NULL THEN 'PRIMARY KEY' ELSE '' END AS KeyType, c.COLUMN_TYPE
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
                            else reader.GetString(2)
                        let isUnsigned = not(reader.IsDBNull(6)) && reader.GetString(6).ToUpper().Contains("UNSIGNED")
                        let udt = if isUnsigned then dt + " unsigned" else dt
                        match MySql.findDbType udt with
                        | Some(m) ->
                            let col =
                                { Column.Name = reader.GetString(0)
                                  TypeMapping = m
                                  IsNullable = let b = reader.GetString(4) in if b = "YES" then true else false
                                  IsPrimaryKey = if reader.GetString(5) = "PRIMARY KEY" then true else false 
                                  TypeInfo = if String.IsNullOrEmpty(maxlen) then Some dt else Some (dt + "(" + maxlen + ")")}
                            if col.IsPrimaryKey then 
                                pkLookup.AddOrUpdate(table.FullName, [col.Name], fun key old -> 
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
                columnLookup.AddOrUpdate(table.FullName, columns, fun x old -> match columns.Count with 0 -> old | x -> columns)

        member __.GetRelationships(con,table) =
          relationshipLookup.GetOrAdd(table.FullName, fun name ->
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
                        yield { Name = reader.GetString(0); PrimaryTable=Table.CreateFullName(reader.GetString(2),reader.GetString(1)); PrimaryKey=reader.GetString(3)
                                ForeignTable=Table.CreateFullName(reader.GetString(5),reader.GetString(4)); ForeignKey=reader.GetString(6) } ]
                reader.Dispose()
                use com = (this:>ISqlProvider).CreateCommand(con,(sprintf "%s AND KCU1.REFERENCED_TABLE_NAME = @table" baseQuery))
                com.Parameters.Add((this:>ISqlProvider).CreateCommandParameter(QueryParameter.Create("@table", 0), (MySql.ripQuotes table.Name))) |> ignore
                if con.State <> ConnectionState.Open then con.Open()
                use reader = com.ExecuteReader()
                let parents =
                    [ while reader.Read() do
                        yield { Name = reader.GetString(0); PrimaryTable=Table.CreateFullName(reader.GetString(2),reader.GetString(1)); PrimaryKey=reader.GetString(3)
                                ForeignTable= Table.CreateFullName(reader.GetString(5),reader.GetString(4)); ForeignKey=reader.GetString(6) } ]
                (children,parents)) 
            res)

        member __.GetSprocs(con) = Sql.connect con MySql.getSprocs
        member __.GetIndividualsQueryText(table,amount) = sprintf "SELECT * FROM %s LIMIT %i;" (table.FullName.Replace("\"","`").Replace("[","`").Replace("]","`").Replace("``","`")) amount
        member __.GetIndividualQueryText(table,column) = sprintf "SELECT * FROM `%s`.`%s` WHERE `%s`.`%s`.`%s` = @id" table.Schema (MySql.ripQuotes table.Name) table.Schema (MySql.ripQuotes table.Name) column

        member this.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns,isDeleteScript) =
            let sb = System.Text.StringBuilder()
            let parameters = ResizeArray<_>()
            let (~~) (t:string) = sb.Append t |> ignore

            // to simplfy (ha!) the processing, all tables should be aliased.
            // the LINQ infrastructure will cause this will happen by default if the query includes more than one table
            // if it does not, then we first need to create an alias for the single table
            let getTable x =
                match sqlQuery.Aliases.TryFind x with
                | Some(a) -> a
                | None -> baseTable

            let singleEntity = sqlQuery.Aliases.Count = 0


            // build the sql query from the simplified abstract query expression
            // working on the basis that we will alias everything to make my life eaiser
            // first build  the select statment, this is easy ...
            let selectcolumns =
                if projectionColumns |> Seq.isEmpty then "1" else
                String.Join(",",
                    [|for KeyValue(k,v) in projectionColumns do
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in columnLookup.[(getTable k).FullName] |> Seq.map (fun c -> c.Key) do
                                if singleEntity then yield sprintf "`%s`.`%s` as `%s`" k col col
                                else yield sprintf "`%s`.`%s` as '`%s`.`%s`'" k col k col
                        else
                            for col in v do
                                if singleEntity then yield sprintf "`%s`.`%s` as `%s`" k col col
                                else yield sprintf "`%s`.`%s` as '`%s`.`%s`'" k col k col|]) // F# makes this so easy :)

            // Create sumBy, minBy, maxBy, ... field columns
            let columns = 
                let extracolumns =
                    match sqlQuery.Grouping with
                    | [] -> FSharp.Data.Sql.Common.Utilities.parseAggregates MySql.fieldNotation MySql.fieldNotationAlias sqlQuery.AggregateOp
                    | g  -> 
                        let keys = g |> List.map(fst) |> List.concat |> List.map(fun (a,c) -> MySql.fieldNotation a c)
                        let aggs = g |> List.map(snd) |> List.concat
                        let res2 = FSharp.Data.Sql.Common.Utilities.parseAggregates MySql.fieldNotation MySql.fieldNotationAlias aggs |> List.toSeq
                        [String.Join(", ", keys) + (match aggs with [] -> "" | _ -> ", ") + String.Join(", ", res2)] 
                match extracolumns with
                | [] -> selectcolumns
                | h::t -> h

            // next up is the filter expressions
            // make this nicer later.. just try and get the damn thing to work properly (well, at all) for now :D
            // NOTE: really need to assign the parameters their correct sql types
            let param = ref 0
            let nextParam() =
                incr param
                sprintf "@param%i" !param

            let createParam (value:obj) =
                let paramName = nextParam()
                (this:>ISqlProvider).CreateCommandParameter((MySql.createParam paramName !param value), value)

            let rec filterBuilder = function
                | [] -> ()
                | (cond::conds) ->
                    let build op preds (rest:Condition list option) =
                        ~~ "("
                        preds |> List.iteri( fun i (alias,col,operator,data) ->
                                let column = MySql.fieldNotation alias col
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
                                        | FSharp.Data.Sql.In -> "FALSE" // nothing is in the empty set
                                        | FSharp.Data.Sql.NotIn -> "TRUE" // anything is not in the empty set
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
                                    | FSharp.Data.Sql.NestedIn -> sprintf "%s IN (%s)" column innersql
                                    | FSharp.Data.Sql.NestedNotIn -> sprintf "%s NOT IN (%s)" column innersql
                                    | _ -> failwith "Should not be called with any other operator"

                                ~~(sprintf "%s%s" prefix <|
                                    match operator with
                                    | FSharp.Data.Sql.IsNull -> sprintf "%s IS NULL" column
                                    | FSharp.Data.Sql.NotNull -> sprintf "%s IS NOT NULL" column
                                    | FSharp.Data.Sql.In 
                                    | FSharp.Data.Sql.NotIn -> operatorIn operator paras
                                    | FSharp.Data.Sql.NestedIn 
                                    | FSharp.Data.Sql.NestedNotIn -> operatorInQuery operator paras
                                    | _ ->

                                        let aliasformat = sprintf "%s %s %s" column
                                        match data with 
                                        | Some d when (box d :? alias * SqlColumnType) ->
                                            let alias2, col2 = box d :?> (alias * SqlColumnType)
                                            let alias2f = MySql.fieldNotation alias2 col2
                                            aliasformat (operator.ToString()) alias2f
                                        | _ ->
                                            parameters.Add paras.[0]
                                            aliasformat (operator.ToString()) paras.[0].ParameterName
                        ))
                        // there's probably a nicer way to do this
                        let rec aux = function
                            | x::[] when preds.Length > 0 ->
                                ~~ (sprintf " %s " op)
                                filterBuilder [x]
                            | x::[] -> filterBuilder [x]
                            | x::xs when preds.Length > 0 ->
                                ~~ (sprintf " %s " op)
                                filterBuilder [x]
                                ~~ (sprintf " %s " op)
                                aux xs
                            | x::xs ->
                                filterBuilder [x]
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

                    filterBuilder conds

            // next up is the FROM statement which includes joins ..
            let fromBuilder() =
                sqlQuery.Links
                |> List.iter(fun (fromAlias, data, destAlias)  ->
                    let joinType = if data.OuterJoin then "LEFT OUTER JOIN " else "INNER JOIN "
                    let destTable = getTable destAlias
                    ~~  (sprintf "%s `%s`.`%s` as `%s` on "
                            joinType destTable.Schema destTable.Name destAlias)
                    ~~  (String.Join(" AND ", (List.zip data.ForeignKey data.PrimaryKey) |> List.map(fun (foreignKey,primaryKey) ->
                        sprintf "%s = %s" 
                            (MySql.fieldNotation (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias) foreignKey)
                            (MySql.fieldNotation (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias) primaryKey)
                            ))))

            let groupByBuilder() =
                sqlQuery.Grouping |> List.map(fst) |> List.concat
                |> List.iteri(fun i (alias,column) ->
                    if i > 0 then ~~ ", "
                    ~~ (MySql.fieldNotation alias column))

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) ->
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "%s %s" (MySql.fieldNotation alias column) (if not desc then "DESC " else "")))

            let basetable = baseTable.FullName.Replace("\"","`").Replace("[","`").Replace("]","`").Replace("``","`")
            if isDeleteScript then
                ~~(sprintf "DELETE FROM %s " basetable)
            else 
                // SELECT
                if sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s " columns)
                elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
                else  ~~(sprintf "SELECT %s " columns)
                // FROM
                ~~(sprintf "FROM %s as `%s` " basetable  baseAlias)
            fromBuilder()
            // WHERE
            if sqlQuery.Filters.Length > 0 then
                // each filter is effectively the entire contents of each where clause in the linq query,
                // of which there can be many. Simply turn them all into one big AND expression as that is the
                // only logical way to deal with them.
                let f = [And([],Some sqlQuery.Filters)]
                ~~"WHERE "
                filterBuilder f

            // GROUP BY
            if sqlQuery.Grouping.Length > 0 then
                ~~" GROUP BY "
                groupByBuilder()

            if sqlQuery.HavingFilters.Length > 0 then
                let keys = sqlQuery.Grouping |> List.map(fst) |> List.concat

                let f = [And([],Some (sqlQuery.HavingFilters |> CommonTasks.parseHaving MySql.fieldNotation keys))]
                ~~" HAVING "
                filterBuilder f

            // ORDER BY
            if sqlQuery.Ordering.Length > 0 then
                ~~"ORDER BY "
                orderByBuilder()

            match sqlQuery.Union with
            | Some(UnionType.UnionAll, suquery) -> ~~(sprintf " UNION ALL %s " suquery)
            | Some(UnionType.NormalUnion, suquery) -> ~~(sprintf " UNION %s " suquery)
            | Some(UnionType.Intersect, suquery) -> ~~(sprintf " INTERSECT %s " suquery)
            | Some(UnionType.Except, suquery) -> ~~(sprintf " EXCEPT %s " suquery)
            | None -> ()

            match sqlQuery.Take, sqlQuery.Skip with
            | Some take, Some skip ->  ~~(sprintf " LIMIT %i OFFSET %i;" take skip)
            | Some take, None ->  ~~(sprintf " LIMIT %i;" take)
            | None, Some skip -> ~~(sprintf " LIMIT %i OFFSET %i;" System.UInt64.MaxValue skip)
            | None, None -> ()

            let sql = sb.ToString()
            (sql,parameters)

        member this.ProcessUpdates(con, entities, transactionOptions) =
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
                        Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                        let id = cmd.ExecuteScalar()
                        CommonTasks.checkKey pkLookup id e
                        e._State <- Unchanged
                    | Modified fields ->
                        let cmd = createUpdateCommand con sb e fields
                        Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                        cmd.ExecuteNonQuery() |> ignore
                        e._State <- Unchanged
                    | Delete ->
                        let cmd = createDeleteCommand con sb e
                        Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                        cmd.ExecuteNonQuery() |> ignore
                        // remove the pk to prevent this attempting to be used again
                        e.SetPkColumnOptionSilent(pkLookup.[e.Table.FullName], None)
                        e._State <- Deleted
                    | Deleted | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!")

                if scope<>null then scope.Complete()
                
            finally
                con.Close()

        member this.ProcessUpdatesAsync(con, entities, transactionOptions) =
            let sb = Text.StringBuilder()

            CommonTasks.``ensure columns have been loaded`` (this :> ISqlProvider) con entities

            if entities.Count = 0 then 
                async { () }
            else

            async {

                use scope = TransactionUtils.ensureTransaction transactionOptions
                try
                    // close the connection first otherwise it won't get enlisted into the transaction
                    if con.State = ConnectionState.Open then con.Close()
                    do! con.OpenAsync() |> Async.AwaitIAsyncResult |> Async.Ignore

                    // initially supporting update/create/delete of single entities, no hierarchies yet
                    let handleEntity (e: SqlEntity) =
                        match e._State with
                        | Created ->
                            async {
                                let cmd = createInsertCommand con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                                let! id = cmd.ExecuteScalarAsync() |> Async.AwaitTask
                                CommonTasks.checkKey pkLookup id e
                                e._State <- Unchanged
                            }
                        | Modified fields ->
                            async {
                                let cmd = createUpdateCommand con sb e fields :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                e._State <- Unchanged
                            }
                        | Delete ->
                            async {
                                let cmd = createDeleteCommand con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                // remove the pk to prevent this attempting to be used again
                                e.SetPkColumnOptionSilent(pkLookup.[e.Table.FullName], None)
                                e._State <- Deleted
                            }
                        | Deleted | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!"

                    do! Utilities.executeOneByOne handleEntity (entities.Keys|>Seq.toList)

                    if scope<>null then scope.Complete()

                finally
                    con.Close()
            }
