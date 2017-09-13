﻿namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Data
open System.Data.SqlClient
open FSharp.Data.Sql
open FSharp.Data.Sql.Transactions
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common

module MSSqlServer =
    let getSchema name (args:string[]) (con:IDbConnection) =
        let con = (con :?> SqlConnection)
        if con.State <> ConnectionState.Open then con.Open()
        let res = con.GetSchema(name, args)
        con.Close()
        res

    let mutable typeMappings = []
    let mutable findClrType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"

    let createTypeMappings (con:IDbConnection) =
        let dt = getSchema "DataTypes" [||] con

        let getDbType(providerType:int) =
            let p = new SqlParameter()
            if providerType = 31 || providerType = 32
            then p.SqlDbType <- SqlDbType.DateTime
            else p.SqlDbType <- (Enum.ToObject(typeof<SqlDbType>, providerType) :?> SqlDbType)
            p.DbType

        let getClrType (input:string) =
            let t = Type.GetType input
            if t <> null then t.ToString() else typeof<String>.ToString()

        let mappings =
            [
                for r in dt.Rows do
                    let oleDbType = string r.["TypeName"]
                    let clrType =
                        if oleDbType = "tinyint"
                        then typeof<byte>.ToString()
                        else if oleDbType = "date"
                        then  typeof<DateTime>.ToString()
                        else if oleDbType = "time"
                        then  typeof<DateTime>.ToString()
                        else getClrType (string r.["DataType"])
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
            |> List.map (fun m -> m.ProviderTypeName.Value, m)
            |> Map.ofList

        typeMappings <- mappings
        findClrType <- clrMappings.TryFind
        findDbType <- dbMappings.TryFind

    let createConnection connectionString = new SqlConnection(connectionString) :> IDbConnection

    let createCommand commandText (connection:IDbConnection) = new SqlCommand(commandText, downcast connection) :> IDbCommand

    let dbUnbox<'a> (v:obj) : 'a =
        if Convert.IsDBNull(v) then Unchecked.defaultof<'a> else unbox v

    let dbUnboxWithDefault<'a> def (v:obj) : 'a =
        if Convert.IsDBNull(v) then def else unbox v

    let connect (con:IDbConnection) f =
        if con.State <> ConnectionState.Open then con.Open()
        let result = f con
        con.Close(); result

    let executeSql sql (con:IDbConnection) =
        use com = new SqlCommand(sql,con:?>SqlConnection)
        com.ExecuteReader()

    let readParameter (parameter:IDbDataParameter) =
        if parameter <> null then
            let par = parameter :?> SqlParameter
            par.Value
        else null

    let isSQL2012Orlater (con:IDbConnection) =
        try
            let reader = executeSql "SELECT SERVERPROPERTY('productversion')" con
            let version = reader.GetSqlString(0)
            match version.Value.[0..1] |> Double.TryParse with
            | true, v -> v >= 11.0
            | _ -> false
        with _ -> false

    let createCommandParameter (param:QueryParameter) (value:obj) =
        let p = SqlParameter(param.Name,value)
        p.DbType <- param.TypeMapping.DbType
        Option.iter (fun (t:int) -> p.SqlDbType <- Enum.ToObject(typeof<SqlDbType>, t) :?> SqlDbType) param.TypeMapping.ProviderType
        p.Direction <- param.Direction
        Option.iter (fun l -> p.Size <- l) param.Length
        p :> IDbDataParameter

    let getSprocReturnCols (con: IDbConnection) (sname: SprocName) (sparams: QueryParameter list) =
        let parameterStr =
            String.Join(", ", sparams
                              |> List.filter (fun p -> p.Direction <> ParameterDirection.ReturnValue)
                              |> List.map(fun p -> p.Name + "= null")
                              |> List.toArray)
        let query = sprintf "SET NO_BROWSETABLE ON; SET FMTONLY ON; exec %s %s" sname.DbName parameterStr
        let derivedCols =
            let initialSchemas =
                connect con (fun con ->
                    try
                        let dr = executeSql query con
                        [ yield dr.GetSchemaTable();
                          while dr.NextResult() do yield dr.GetSchemaTable() ]
                    with
                    | ex ->
                        System.Diagnostics.Debug.WriteLine(sprintf "Failed to retrieve metadata for sproc %s\r\n : %s" sname.DbName (ex.ToString()))
                        []) //Just assumes the proc / func returns something and let the caller process the result, for now.
            initialSchemas
            |> List.mapi (fun i dt ->
                match dt with
                | null -> None
                | _ -> findDbType "cursor"
                       |> Option.map (fun m ->
                            let name = if i = 0 then "ResultSet" else "ResultSet_" + (string i)
                            QueryParameter.Create(name,i,m,ParameterDirection.Output)))
            |> List.choose id
        let retValueCols =
            sparams
            |> List.filter (fun x -> x.Direction = ParameterDirection.ReturnValue)
            |> List.mapi (fun i p ->
                if String.IsNullOrEmpty p.Name then
                    { p with Name = (if i = 0 then "ReturnValue" else "ReturnValue_" + (string i))}
                else p)
        derivedCols @ retValueCols

    let getSprocName (row:DataRow) =
        let owner = dbUnbox row.["specific_catalog"]
        let (procName, packageName) = (dbUnbox row.["specific_name"], dbUnbox row.["specific_schema"])
        { ProcName = procName; Owner = owner; PackageName = packageName; }

    let getSprocParameters (con : IDbConnection) (name : SprocName) =

        let createSprocParameters (row:DataRow) =
            let dataType = dbUnbox row.["data_type"]
            let argumentName = dbUnbox row.["parameter_name"]
            let maxLength = Some(dbUnboxWithDefault<int> -1 row.["character_maximum_length"])

            findDbType dataType
            |> Option.map (fun m ->
                let returnValue = dbUnbox<string> row.["is_result"] = "YES"
                let direction =
                    match dbUnbox<string> row.["parameter_mode"] with
                    | "IN" -> ParameterDirection.Input
                    | "OUT" when returnValue -> ParameterDirection.ReturnValue
                    | "OUT" -> ParameterDirection.Output
                    | "INOUT" -> ParameterDirection.InputOutput
                    | a -> failwithf "Direction not supported %s" a
                { Name = argumentName
                  TypeMapping = m
                  Direction = direction
                  Length = maxLength
                  Ordinal = dbUnbox<int> row.["ordinal_position"] }
            )

        getSchema "ProcedureParameters" [||] con
        |> DataTable.groupBy (fun row -> getSprocName row, createSprocParameters row)
        |> Seq.filter (fun (n, _) -> n.ProcName = name.ProcName)
        |> Seq.collect (snd >> Seq.choose id)
        |> Seq.sortBy (fun x -> x.Ordinal)
        |> Seq.toList

    let getSprocs (con: IDbConnection) =
        let con = (con :?> SqlConnection)

        let tableValued =
            Sql.executeSqlAsDataTable
                createCommand
                "SELECT DISTINCT ROUTINE_CATALOG, ROUTINE_SCHEMA, ROUTINE_NAME FROM [INFORMATION_SCHEMA].[ROUTINES] where [DATA_TYPE] = 'table'"
                con
            |> DataTable.map(fun row -> dbUnbox<string> row.["ROUTINE_CATALOG"], dbUnbox<string> row.["ROUTINE_SCHEMA"], dbUnbox<string> row.["ROUTINE_NAME"] )

        let haveUDTs =
            Sql.executeSqlAsDataTable
                createCommand
                "SELECT distinct [SPECIFIC_CATALOG]
                      ,[SPECIFIC_SCHEMA]
                      ,[SPECIFIC_NAME]
                  FROM [INFORMATION_SCHEMA].[PARAMETERS] where USER_DEFINED_TYPE_NAME <> ''"
                con
            |> DataTable.map(fun row -> dbUnbox<string> row.["SPECIFIC_CATALOG"], dbUnbox<string> row.["SPECIFIC_SCHEMA"], dbUnbox<string> row.["SPECIFIC_NAME"] )

        // table valued functions and stuff with user defined types are not currently supported.
        let toFilter = set(tableValued @ haveUDTs)

        getSchema "Procedures" [||] con
        |> DataTable.filter(fun row ->
            let name = dbUnbox<string> row.["SPECIFIC_CATALOG"], dbUnbox<string> row.["SPECIFIC_SCHEMA"], dbUnbox<string> row.["SPECIFIC_NAME"]
            not <| Set.contains name toFilter)
        |> DataTable.map (fun row -> getSprocName row, dbUnbox<string> row.["routine_type"])
        |> Seq.map (fun (name, routineType) ->
             match routineType.ToUpper() with
             | "FUNCTION" -> Root("Functions",  Sproc({ Name = name; Params = (fun con -> getSprocParameters con name); ReturnColumns = (fun con sparams -> getSprocReturnCols con name sparams) }))
             | "PROCEDURE" ->  Root("Procedures", Sproc({ Name = name; Params = (fun con -> getSprocParameters con name); ReturnColumns = (fun con sparams -> getSprocReturnCols con name sparams) }))
             | _ -> Empty
           )
        |> Seq.toList
    
    let executeSprocCommandCommon (inputParameters:QueryParameter []) (returnCols:QueryParameter[]) (values:obj[]) =
        let inputParameters = inputParameters |> Array.filter (fun p -> p.Direction = ParameterDirection.Input)

        let outps =
             returnCols
             |> Array.map(fun ip ->
                 let p = createCommandParameter ip null
                 if ip.Direction = ParameterDirection.ReturnValue
                 then
                    p.ParameterName <- "@RETURN_VALUE"
                    (-1, ip.Name, p)
                 else
                    (ip.Ordinal, ip.Name, p))

        let inps =
             inputParameters
             |> Array.mapi(fun i ip ->
                 let p = createCommandParameter ip values.[i]
                 (ip.Ordinal, ip.Name, p))

        let returnValues =
            outps |> Array.filter (fun (_,_,p) -> p.Direction = ParameterDirection.ReturnValue)

        let allParams =
            Array.append returnValues inps
            |> Array.sortBy (fun (x,_,_) -> x)
        
        let processReturnColumn reader (retCol:QueryParameter) =
            match retCol.TypeMapping.ProviderTypeName with
            | Some "cursor" ->
                let result = ResultSet(retCol.Name, Sql.dataReaderToArray reader)
                reader.NextResult() |> ignore
                result
            | _ ->
                match outps |> Array.tryFind (fun (_,_,p) -> p.ParameterName = retCol.Name) with
                | Some(_,_,p) -> ScalarResultSet(p.ParameterName, readParameter p)
                | None -> failwithf "Excepted return column %s but could not find it in the parameter set" retCol.Name

        allParams, processReturnColumn, outps

    let executeSprocCommand (com:IDbCommand) (inputParameters:QueryParameter []) (returnCols:QueryParameter[]) (values:obj[]) =

        let allParams, processReturnColumn, outps = executeSprocCommandCommon inputParameters returnCols values
        allParams |> Array.iter (fun (_,_,p) -> com.Parameters.Add(p) |> ignore)

        match returnCols with
        | [||] -> com.ExecuteNonQuery() |> ignore; Unit
        | [|retCol|] ->
            match retCol.TypeMapping.ProviderTypeName with
            | Some "cursor" ->
                use reader = com.ExecuteReader() :?> SqlDataReader
                let result = SingleResultSet(retCol.Name, Sql.dataReaderToArray reader)
                reader.NextResult() |> ignore
                result
            | _ ->
                com.ExecuteNonQuery() |> ignore
                match outps |> Array.tryFind (fun (_,_,p) -> p.Direction = ParameterDirection.ReturnValue) with
                | Some(_,name,p) -> Scalar(name, readParameter p)
                | None -> failwithf "Excepted return column %s but could not find it in the parameter set" retCol.Name
        | cols ->
            use reader = com.ExecuteReader() :?> SqlDataReader
            Set(cols |> Array.map (processReturnColumn reader))


    let executeSprocCommandAsync (com:System.Data.Common.DbCommand) (inputParameters:QueryParameter []) (returnCols:QueryParameter[]) (values:obj[]) =
        async {
            let allParams, processReturnColumn, outps = executeSprocCommandCommon inputParameters returnCols values
            allParams |> Array.iter (fun (_,_,p) -> com.Parameters.Add(p) |> ignore)

            match returnCols with
            | [||] -> do! com.ExecuteNonQueryAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
                      return Unit
            | [|retCol|] ->
                match retCol.TypeMapping.ProviderTypeName with
                | Some "cursor" ->
                    use! readera = com.ExecuteReaderAsync() |> Async.AwaitTask
                    let reader = readera :?> SqlDataReader
                    let result = SingleResultSet(retCol.Name, Sql.dataReaderToArray reader)
                    reader.NextResult() |> ignore
                    return result
                | _ ->
                    do! com.ExecuteNonQueryAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
                    match outps |> Array.tryFind (fun (_,_,p) -> p.Direction = ParameterDirection.ReturnValue) with
                    | Some(_,name,p) -> return Scalar(name, readParameter p)
                    | None -> return failwithf "Excepted return column %s but could not find it in the parameter set" retCol.Name
            | cols ->
                use! reader = com.ExecuteReaderAsync() |> Async.AwaitTask
                return Set(cols |> Array.map (processReturnColumn reader))
        }

type internal MSSqlServerProvider(tableNames:string) =
    let pkLookup = ConcurrentDictionary<string,string list>()
    let tableLookup = ConcurrentDictionary<string,Table>()
    let columnLookup = ConcurrentDictionary<string,ColumnLookup>()
    let relationshipLookup = ConcurrentDictionary<string,Relationship list * Relationship list>()

    let rec fieldNotation (al:alias) (c:SqlColumnType) = 
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
            | Replace(SqlStr(searchItm),SqlStrCol(al2, col2)) -> sprintf "REPLACE(%s,'%s',%s)" column searchItm (fieldNotation al2 col2)
            | Replace(SqlStrCol(al2, col2),SqlStr(toItm)) -> sprintf "REPLACE(%s,%s,'%s')" column (fieldNotation al2 col2) toItm
            | Replace(SqlStrCol(al2, col2),SqlStrCol(al3, col3)) -> sprintf "REPLACE(%s,%s,%s)" column (fieldNotation al2 col2) (fieldNotation al3 col3)
            | Substring(SqlInt startPos) -> sprintf "SUBSTRING(%s, %i)" column startPos
            | Substring(SqlIntCol(al2, col2)) -> sprintf "SUBSTRING(%s, %s)" column (fieldNotation al2 col2)
            | SubstringWithLength(SqlInt startPos,SqlInt strLen) -> sprintf "SUBSTRING(%s, %i, %i)" column startPos strLen
            | SubstringWithLength(SqlInt startPos,SqlIntCol(al2, col2)) -> sprintf "SUBSTRING(%s, %i, %s)" column startPos (fieldNotation al2 col2)
            | SubstringWithLength(SqlIntCol(al2, col2),SqlInt strLen) -> sprintf "SUBSTRING(%s, %s, %i)" column (fieldNotation al2 col2) strLen
            | SubstringWithLength(SqlIntCol(al2, col2),SqlIntCol(al3, col3)) -> sprintf "SUBSTRING(%s, %s, %s)" column (fieldNotation al2 col2) (fieldNotation al3 col3)
            | Trim -> sprintf "LTRIM(RTRIM(%s))" column
            | Length -> sprintf "DATALENGTH(%s)" column
            | IndexOf(SqlStr search) -> sprintf "CHARINDEX('%s',%s)" search column
            | IndexOf(SqlStrCol(al2, col2)) -> sprintf "CHARINDEX(%s,%s)" (fieldNotation al2 col2) column
            | IndexOfStart(SqlStr(search),(SqlInt startPos)) -> sprintf "CHARINDEX('%s',%s,%d)" search column startPos
            | IndexOfStart(SqlStr(search),SqlIntCol(al2, col2)) -> sprintf "CHARINDEX('%s',%s,%s)" search column (fieldNotation al2 col2)
            | IndexOfStart(SqlStrCol(al2, col2),(SqlInt startPos)) -> sprintf "CHARINDEX(%s,%s,%d)" (fieldNotation al2 col2) column startPos
            | IndexOfStart(SqlStrCol(al2, col2),SqlIntCol(al3, col3)) -> sprintf "CHARINDEX(%s,%s,%s)" (fieldNotation al2 col2) column (fieldNotation al3 col3)
            // Date functions
            | Date -> sprintf "CAST(%s AS DATE)" column
            | Year -> sprintf "YEAR(%s)" column
            | Month -> sprintf "MONTH(%s)" column
            | Day -> sprintf "DAY(%s)" column
            | Hour -> sprintf "DATEPART(HOUR, %s)" column
            | Minute -> sprintf "DATEPART(MINUTE, %s)" column
            | Second -> sprintf "DATEPART(SECOND, %s)" column
            | AddYears(SqlInt x) -> sprintf "DATEADD(YEAR, %d, %s)" x column
            | AddYears(SqlIntCol(al2, col2)) -> sprintf "DATEADD(YEAR, %s, %s)" (fieldNotation al2 col2) column
            | AddMonths x -> sprintf "DATEADD(MONTH, %d, %s)" x column
            | AddDays(SqlFloat x) -> sprintf "DATEADD(DAY, %f, %s)" x column // SQL ignores decimal part :-(
            | AddDays(SqlNumCol(al2, col2)) -> sprintf "DATEADD(DAY, %s, %s)" (fieldNotation al2 col2) column
            | AddHours x -> sprintf "DATEADD(HOUR, %f, %s)" x column
            | AddMinutes(SqlFloat x) -> sprintf "DATEADD(MINUTE, %f, %s)" x column
            | AddMinutes(SqlNumCol(al2, col2)) -> sprintf "DATEADD(MINUTE, %s, %s)" (fieldNotation al2 col2) column
            | AddSeconds x -> sprintf "DATEADD(SECOND, %f, %s)" x column
            // Math functions
            | Truncate -> sprintf "TRUNCATE(%s)" column
            | BasicMathOfColumns(o, a, c) -> sprintf "(%s %s %s)" column (o.Replace("||","+")) (fieldNotation a c)
            | BasicMath(o, par) when (par :? String || par :? Char) -> sprintf "(%s %s '%O')" column (o.Replace("||","+")) par
            | _ -> Utilities.genericFieldNotation (fieldNotation al) colSprint c
        | _ -> Utilities.genericFieldNotation (fieldNotation al) colSprint c

    let fieldNotationAlias(al:alias,col:SqlColumnType) = 
        let aliasSprint =
            match String.IsNullOrEmpty(al) with
            | true -> sprintf "'[%s]'"
            | false -> sprintf "'[%s].[%s]'" al
        Utilities.genericAliasNotation aliasSprint col

    let createInsertCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore

        let cmd = new SqlCommand()
        cmd.Connection <- con :?> SqlConnection
        let haspk = pkLookup.ContainsKey(entity.Table.FullName)
        let pk = if haspk then pkLookup.[entity.Table.FullName] else []
        let columnNames, values =
            (([],0),entity.ColumnValues)
            ||> Seq.fold(fun (out,i) (k,v) ->
                let name = sprintf "@param%i" i
                let p = SqlParameter(name,v)
                (sprintf "[%s]" k,p)::out,i+1)
            |> fst
            |> List.rev
            |> List.toArray
            |> Array.unzip

        sb.Clear() |> ignore
        match haspk, pk with
        | true, [itm] ->
            if values |> Array.isEmpty then
                ~~(sprintf "INSERT INTO [%s].[%s] OUTPUT inserted.%s DEFAULT VALUES;" entity.Table.Schema entity.Table.Name itm)
            else
                ~~(sprintf "INSERT INTO [%s].[%s] (%s) OUTPUT inserted.%s VALUES (%s);"
                    entity.Table.Schema entity.Table.Name
                    (String.Join(",",columnNames))
                    itm
                    (String.Join(",",values |> Array.map(fun p -> p.ParameterName))))
        | _ -> 
            if values |> Array.isEmpty then
                ~~(sprintf "INSERT INTO [%s].[%s] DEFAULT VALUES;" entity.Table.Schema entity.Table.Name )
            else
                ~~(sprintf "INSERT INTO [%s].[%s] (%s) VALUES (%s);"
                    entity.Table.Schema entity.Table.Name
                    (String.Join(",",columnNames))
                    (String.Join(",",values |> Array.map(fun p -> p.ParameterName))))

        cmd.Parameters.AddRange(values)
        cmd.CommandText <- sb.ToString()
        cmd

    let createUpdateCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) (changedColumns:string list) =
        let (~~) (t:string) = sb.Append t |> ignore

        let cmd = new SqlCommand()
        cmd.Connection <- con :?> SqlConnection
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
                    | Some v -> SqlParameter(name,v)
                    | None -> SqlParameter(name,DBNull.Value)
                (col,p)::out,i+1)
            |> fst
            |> List.rev
            |> List.toArray

        match pk with
        | [] -> ()
        | ks -> 
            ~~(sprintf "UPDATE [%s].[%s] SET %s WHERE "
                entity.Table.Schema entity.Table.Name
                (String.Join(",", data |> Array.map(fun (c,p) -> sprintf "[%s] = %s" c p.ParameterName ) )))
            ~~(String.Join(" AND ", ks |> List.mapi(fun i k -> (sprintf "[%s] = @pk%i" k i))))

        cmd.Parameters.AddRange(data |> Array.map snd)
        pkValues |> List.iteri(fun i pkValue ->
            let pkParam = SqlParameter(("@pk"+i.ToString()), pkValue)
            cmd.Parameters.Add pkParam |> ignore)
        cmd.CommandText <- sb.ToString()
        cmd

    let createDeleteCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore

        let cmd = new SqlCommand()
        cmd.Connection <- con :?> SqlConnection
        sb.Clear() |> ignore
        let haspk = pkLookup.ContainsKey(entity.Table.FullName)
        let pk = if haspk then pkLookup.[entity.Table.FullName] else []
        sb.Clear() |> ignore
        let pkValues =
            match entity.GetPkColumnOption<obj> pk with
            | [] -> failwith ("Error - you cannot delete an entity that does not have a primary key. (" + entity.Table.FullName + ")")
            | v -> v

        pkValues |> List.iteri(fun i pkValue ->
            cmd.Parameters.AddWithValue(("@id"+i.ToString()),pkValue) |> ignore)

        match pk with
        | [] -> ()
        | ks -> 
            ~~(sprintf "DELETE FROM [%s].[%s] WHERE " entity.Table.Schema entity.Table.Name)
            ~~(String.Join(" AND ", ks |> List.mapi(fun i k -> (sprintf "[%s] = @id%i" k i))))

        cmd.CommandText <- sb.ToString()
        cmd

    interface ISqlProvider with
        member __.GetTableDescription(con,tableName) = 
            let tn = tableName.Substring(tableName.LastIndexOf(".")+1) 
            let baseq =
                """select sep.value
                from sys.tables st
                left join sys.extended_properties sep on st.object_id = sep.major_id
                                                        and (sep.minor_id is null or sep.minor_id =0)
                                                        and sep.name = 'MS_Description'
                where st.name = @TableName"""
            use com = new SqlCommand(baseq,con:?>SqlConnection)
            com.Parameters.AddWithValue("@TableName",tn) |> ignore
            if con.State <> ConnectionState.Open then con.Open()
            use reader = com.ExecuteReader()
            if reader.Read() then
                let itm = reader.GetSqlString(0)
                if itm.Value <> null then
                    reader.GetSqlString(0).Value.ToString()
                else ""
            else ""

        member __.GetColumnDescription(con,tableName,columnName) =
            let tn = tableName.Substring(tableName.LastIndexOf(".")+1) 
            let baseq =
                """select sep.value
                from sys.tables st
                inner join sys.columns sc on st.object_id = sc.object_id
                left join sys.extended_properties sep on st.object_id = sep.major_id
                                                        and sc.column_id = sep.minor_id
                                                        and sep.name = 'MS_Description'
                where st.name = @TableName
                and sc.name = @ColumnName"""
            use com = new SqlCommand(baseq,con:?>SqlConnection)
            com.Parameters.AddWithValue("@TableName",tn) |> ignore
            com.Parameters.AddWithValue("@ColumnName",columnName) |> ignore
            if con.State <> ConnectionState.Open then con.Open()
            use reader = com.ExecuteReader()
            if reader.Read() then
                try
                    reader.GetSqlString(0).Value.ToString()
                with 
                | :? InvalidCastException -> ""
            else ""
        member __.CreateConnection(connectionString) = MSSqlServer.createConnection connectionString
        member __.CreateCommand(connection,commandText) = MSSqlServer.createCommand commandText connection
        member __.CreateCommandParameter(param, value) = MSSqlServer.createCommandParameter param value
        member __.ExecuteSprocCommand(con, inputParameters, returnCols, values:obj array) = MSSqlServer.executeSprocCommand con inputParameters returnCols values
        member __.ExecuteSprocCommandAsync(con, inputParameters, returnCols, values:obj array) = MSSqlServer.executeSprocCommandAsync con inputParameters returnCols values
        member __.CreateTypeMappings(con) = MSSqlServer.createTypeMappings con
        
        member __.GetTables(con,_) =
            let tableNamesFilter =
                match tableNames with 
                | "" -> ""
                | x -> " where 1=1 " + (SchemaProjections.buildTableNameWhereFilter "TABLE_NAME" tableNames)
            MSSqlServer.connect con (fun con ->
            use reader = MSSqlServer.executeSql ("select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE from INFORMATION_SCHEMA.TABLES" + tableNamesFilter) con
            [ while reader.Read() do
                let table ={ Schema = reader.GetSqlString(0).Value ; Name = reader.GetSqlString(1).Value ; Type=reader.GetSqlString(2).Value.ToLower() }
                yield tableLookup.GetOrAdd(table.FullName,table)
                ])

        member __.GetPrimaryKey(table) =
            match pkLookup.TryGetValue table.FullName with
            | true, [v] -> Some v
            | _ -> None

        member __.GetColumns(con,table) =
            match columnLookup.TryGetValue table.FullName with
            | (true,data) when data.Count > 0 -> data
            | _ ->
               // note this data can be obtained using con.GetSchema, and i didn't know at the time about the restrictions you can
               // pass in to filter by table name etc - we should probably swap this code to use that instead at some point
               // but hey, this works
               let baseQuery = @"SELECT c.COLUMN_NAME,c.DATA_TYPE, c.character_maximum_length, c.numeric_precision, c.is_nullable
                                              ,CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'PRIMARY KEY' ELSE '' END AS KeyType
                                 FROM INFORMATION_SCHEMA.COLUMNS c
                                 LEFT JOIN (
                                             SELECT ku.TABLE_CATALOG,ku.TABLE_SCHEMA,ku.TABLE_NAME,ku.COLUMN_NAME
                                             FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                                             INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                                                 ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                                                 AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                                          )   pk
                                 ON  c.TABLE_CATALOG = pk.TABLE_CATALOG
                                             AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA
                                             AND c.TABLE_NAME = pk.TABLE_NAME
                                             AND c.COLUMN_NAME = pk.COLUMN_NAME
                                 WHERE c.TABLE_SCHEMA = @schema AND c.TABLE_NAME = @table
                                 ORDER BY c.TABLE_SCHEMA,c.TABLE_NAME, c.ORDINAL_POSITION"
               use com = new SqlCommand(baseQuery,con:?>SqlConnection)
               com.Parameters.AddWithValue("@schema",table.Schema) |> ignore
               com.Parameters.AddWithValue("@table",table.Name) |> ignore
               if con.State <> ConnectionState.Open then con.Open()
               use reader = com.ExecuteReader()
               let columns =
                   [ while reader.Read() do
                       let dt = reader.GetSqlString(1).Value
                       let maxlen = 
                            let x = reader.GetSqlInt32(2)
                            if x.IsNull then 0 else (x.Value)
                       match MSSqlServer.findDbType dt with
                       | Some(m) ->
                           let col =
                             { Column.Name = reader.GetSqlString(0).Value;
                               TypeMapping = m
                               IsNullable = let b = reader.GetString(4) in if b = "YES" then true else false
                               IsPrimaryKey = if reader.GetSqlString(5).Value = "PRIMARY KEY" then true else false
                               TypeInfo = if maxlen<>0 then Some (dt + "(" + maxlen.ToString() + ")") else Some dt }
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
            // mostly stolen from
            // http://msdn.microsoft.com/en-us/library/aa175805(SQL.80).aspx
            let baseQuery = @"SELECT
                                 KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME
                                ,KCU1.TABLE_NAME AS FK_TABLE_NAME
                                ,KCU1.COLUMN_NAME AS FK_COLUMN_NAME
                                ,KCU1.ORDINAL_POSITION AS FK_ORDINAL_POSITION
                                ,KCU2.CONSTRAINT_NAME AS REFERENCED_CONSTRAINT_NAME
                                ,KCU2.TABLE_NAME AS REFERENCED_TABLE_NAME
                                ,KCU2.COLUMN_NAME AS REFERENCED_COLUMN_NAME
                                ,KCU2.ORDINAL_POSITION AS REFERENCED_ORDINAL_POSITION
                                ,KCU1.CONSTRAINT_SCHEMA AS FK_CONSTRAINT_SCHEMA
                                ,KCU2.CONSTRAINT_SCHEMA AS PK_CONSTRAINT_SCHEMA
                            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC

                            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU1
                                ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG
                                AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA
                                AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME

                            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU2
                                ON KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG
                                AND KCU2.CONSTRAINT_SCHEMA = RC.UNIQUE_CONSTRAINT_SCHEMA
                                AND KCU2.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME
                                AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION "

            let res = MSSqlServer.connect con (fun con ->
                let baseq1 = sprintf "%s WHERE KCU2.TABLE_NAME = @tblName" baseQuery 
                use com1 = new SqlCommand(baseq1,con:?>SqlConnection)
                com1.Parameters.AddWithValue("@tblName",table.Name) |> ignore
                if con.State <> ConnectionState.Open then con.Open()
                use reader = com1.ExecuteReader()
                let children =
                    [ while reader.Read() do
                        yield { Name = reader.GetSqlString(0).Value; PrimaryTable=Table.CreateFullName(reader.GetSqlString(9).Value, reader.GetSqlString(5).Value); PrimaryKey=reader.GetSqlString(6).Value
                                ForeignTable= Table.CreateFullName(reader.GetSqlString(8).Value, reader.GetSqlString(1).Value); ForeignKey=reader.GetSqlString(2).Value } ]
                reader.Dispose()
                let baseq2 = sprintf "%s WHERE KCU1.TABLE_NAME = @tblName" baseQuery
                use com2 = new SqlCommand(baseq2,con:?>SqlConnection)
                com2.Parameters.AddWithValue("@tblName",table.Name) |> ignore
                if con.State <> ConnectionState.Open then con.Open()
                use reader = com2.ExecuteReader()
                let parents =
                    [ while reader.Read() do
                        yield { Name = reader.GetSqlString(0).Value; PrimaryTable=Table.CreateFullName(reader.GetSqlString(9).Value, reader.GetSqlString(5).Value); PrimaryKey=reader.GetSqlString(6).Value
                                ForeignTable=Table.CreateFullName(reader.GetSqlString(8).Value, reader.GetSqlString(1).Value); ForeignKey=reader.GetSqlString(2).Value } ]
                (children,parents))
            res)

        member __.GetSprocs(con) = MSSqlServer.connect con MSSqlServer.getSprocs
        member __.GetIndividualsQueryText(table,amount) = sprintf "SELECT TOP %i * FROM %s" amount table.FullName
        member __.GetIndividualQueryText(table,column) = sprintf "SELECT * FROM [%s].[%s] WHERE [%s].[%s].[%s] = @id" table.Schema table.Name table.Schema table.Name column

        member __.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns,isDeleteScript) =
            let sb = System.Text.StringBuilder()
            let parameters = ResizeArray<_>()
            let (~~) (t:string) = sb.Append t |> ignore

            match sqlQuery.Take, sqlQuery.Skip, sqlQuery.Ordering with
            | Some _, Some _, [] -> failwith "skip and take paging requries an orderBy clause."
            | _ -> ()

            let getTable x =
                match sqlQuery.Aliases.TryFind x with
                | Some(a) -> a
                | None -> baseTable

            let singleEntity = sqlQuery.Aliases.Count = 0

            // first build  the select statement, this is easy ...
            let selectcolumns =
                if projectionColumns |> Seq.isEmpty then "1" else
                String.Join(",",
                    [|for KeyValue(k,v) in projectionColumns do
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in columnLookup.[(getTable k).FullName] |> Seq.map (fun c -> c.Key) do
                                if singleEntity then yield sprintf "[%s].[%s] as '%s'" k col col
                                else yield sprintf "[%s].[%s] as '[%s].[%s]'" k col k col
                        else
                            for col in v do
                                if singleEntity then yield sprintf "[%s].[%s] as '%s'" k col col
                                else yield sprintf "[%s].[%s] as '[%s].[%s]'" k col k col|])

            // Create sumBy, minBy, maxBy, ... field columns
            let columns =
                let extracolumns =
                    match sqlQuery.Grouping with
                    | [] -> FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation fieldNotationAlias sqlQuery.AggregateOp
                    | g  -> 
                        let keys = g |> List.map(fst) |> List.concat |> List.map(fun (a,c) -> (fieldNotation a c))
                        let aggs = g |> List.map(snd) |> List.concat
                        let res2 = FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation fieldNotationAlias aggs |> List.toSeq
                        [String.Join(", ", keys) + (match aggs with [] -> "" | _ -> ", ") + String.Join(", ", res2)] 
                match extracolumns with
                | [] -> selectcolumns
                | h::t -> h

            // next up is the filter expressions
            // make this nicer later..
            let param = ref 0
            let nextParam() =
                incr param
                sprintf "@param%i" !param

            let createParam (value:obj) =
                let paramName = nextParam()
                SqlParameter(paramName,value):> IDbDataParameter

            let rec filterBuilder = function
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
                    ~~  (sprintf "%s [%s].[%s] as [%s] on "
                            joinType destTable.Schema destTable.Name destAlias)
                    ~~  (String.Join(" AND ", (List.zip data.ForeignKey data.PrimaryKey) |> List.map(fun (foreignKey,primaryKey) ->
                        sprintf "%s = %s"
                            (fieldNotation (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias) foreignKey)
                            (fieldNotation (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias) primaryKey)
                        ))))

            let groupByBuilder() =
                sqlQuery.Grouping |> List.map(fst) |> List.concat
                |> List.iteri(fun i (alias,column) ->
                    if i > 0 then ~~ ", "
                    ~~ (fieldNotation alias column))

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) ->
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "%s %s" (fieldNotation alias column) (if not desc then "DESC " else "")))

            if isDeleteScript then
                ~~(sprintf "DELETE FROM [%s].[%s] " baseTable.Schema baseTable.Name)
            else 
                // SELECT
                if sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s%s " (if sqlQuery.Take.IsSome then sprintf "TOP %i " sqlQuery.Take.Value else "")   columns)
                elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
                else
                    match sqlQuery.Skip, sqlQuery.Take with
                    | None, Some take -> ~~(sprintf "SELECT TOP %i %s " take columns)
                    | _ -> ~~(sprintf "SELECT %s " columns)
                // FROM
                ~~(sprintf "FROM [%s].[%s] as [%s] " baseTable.Schema baseTable.Name baseAlias)
            fromBuilder()
            // WHERE
            if sqlQuery.Filters.Length > 0 then
                // each filter is effectively the entire contents of each where clause in the LINQ query,
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

                let f = [And([],Some (sqlQuery.HavingFilters |> CommonTasks.parseHaving fieldNotation keys))]
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

            match sqlQuery.Skip, sqlQuery.Take with
            | Some skip, Some take ->
                // Note: this only works in >=SQL2012
                ~~ (sprintf "OFFSET %i ROWS FETCH NEXT %i ROWS ONLY" skip take)
            | Some skip, None ->
                // Note: this only works in >=SQL2012
                ~~ (sprintf "OFFSET %i ROWS FETCH NEXT %i ROWS ONLY" skip System.UInt32.MaxValue)
            | _ -> ()

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
                        Common.QueryEvents.PublishSqlQueryCol cmd.CommandText cmd.Parameters
                        let id = cmd.ExecuteScalar()
                        CommonTasks.checkKey pkLookup id e
                        e._State <- Unchanged
                    | Modified fields ->
                        let cmd = createUpdateCommand con sb e fields
                        Common.QueryEvents.PublishSqlQueryCol cmd.CommandText cmd.Parameters
                        cmd.ExecuteNonQuery() |> ignore
                        e._State <- Unchanged
                    | Delete ->
                        let cmd = createDeleteCommand con sb e
                        Common.QueryEvents.PublishSqlQueryCol cmd.CommandText cmd.Parameters
                        cmd.ExecuteNonQuery() |> ignore
                        // remove the pk to prevent this attempting to be used again
                        e.SetPkColumnOptionSilent(pkLookup.[e.Table.FullName], None)
                        e._State <- Deleted
                    | Deleted | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!")
                                   // but is possible if you try to use same context on multiple threads. Don't do that.
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
                                let cmd = createInsertCommand con sb e
                                Common.QueryEvents.PublishSqlQueryCol cmd.CommandText cmd.Parameters
                                let! id = cmd.ExecuteScalarAsync() |> Async.AwaitTask
                                CommonTasks.checkKey pkLookup id e
                                e._State <- Unchanged
                            }
                        | Modified fields ->
                            async {
                                let cmd = createUpdateCommand con sb e fields
                                Common.QueryEvents.PublishSqlQueryCol cmd.CommandText cmd.Parameters
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                e._State <- Unchanged
                            }
                        | Delete ->
                            async {
                                let cmd = createDeleteCommand con sb e
                                Common.QueryEvents.PublishSqlQueryCol cmd.CommandText cmd.Parameters
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
