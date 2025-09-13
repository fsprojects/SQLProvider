namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Data
open FSharp.Data.Sql
open FSharp.Data.Sql.Transactions
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Common.Utilities

module internal Oracle =
    let mutable resolutionPath = String.Empty
    let mutable referencedAssemblies : string array = [||]
    let mutable owner = String.Empty

    let assemblyNames =
        [
            "Oracle.ManagedDataAccess.dll"
            "Oracle.DataAccess.dll"
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
           failwithf "Unable to resolve assemblies. One of %s must exist in the paths: %s %s %s"
                (String.Join(", ", assemblyNames |> List.toArray))
                Environment.NewLine
                (String.Join(Environment.NewLine, paths |> Seq.filter(String.IsNullOrEmpty >> not)))
                details

    let systemNames =
        [
            "SYSTEM"; "SYS"; "XDB"
        ]

    let connectionType = lazy  (findType "OracleConnection")
    let commandType =  lazy   (findType "OracleCommand")
    let parameterType = lazy   (findType "OracleParameter")
    let oracleRefCursorType = lazy   (findType "OracleRefCursor")

    let getDataReaderForRefCursor = lazy (oracleRefCursorType.Value.GetMethod("GetDataReader",[||]))

    let getSchemaMethod = lazy (connectionType.Value.GetMethod("GetSchema",[|typeof<string>; typeof<string[]>|]))

    let getSchema name (args:string[]) (conn:IDbConnection) =
#if REFLECTIONLOAD
        getSchemaMethod.Value.Invoke(conn,[|name; args|]) :?> DataTable
#else
        (conn :?> Oracle.ManagedDataAccess.Client.OracleConnection).GetSchema(name, args)
#endif

    let mutable typeMappings = []
    let mutable findClrType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"

    let createCommandParameter (param:QueryParameter) value =
        let value = if isNull value then (box System.DBNull.Value) else value

#if REFLECTIONLOAD
        let parameterType = parameterType.Value
        let oracleDbTypeSetter =
            parameterType.GetProperty("OracleDbType").GetSetMethod()

        let p = Activator.CreateInstance(parameterType,[|box param.Name; box value|]) :?> IDbDataParameter

        p.Direction <- param.Direction

        match param.TypeMapping.ProviderTypeName with
        | ValueSome _ ->
            p.DbType <- param.TypeMapping.DbType
            param.TypeMapping.ProviderType |> ValueOption.iter (fun pt -> oracleDbTypeSetter.Invoke(p, [|pt|]) |> ignore)
        | ValueNone -> ()
#else
        let p1 = new Oracle.ManagedDataAccess.Client.OracleParameter()
        p1.Direction <- param.Direction
        match param.TypeMapping.ProviderTypeName with
        | ValueSome _ ->
            p1.DbType <- param.TypeMapping.DbType
            param.TypeMapping.ProviderType |> ValueOption.iter (fun pt -> 
                p1.OracleDbType <- Enum.ToObject(typeof<Oracle.ManagedDataAccess.Client.OracleDbType>, pt) :?> Oracle.ManagedDataAccess.Client.OracleDbType)
        | ValueNone -> ()
        let p = p1 :> IDbDataParameter
#endif

        match param.Length with
        | ValueSome(length) when length >= 0 -> p.Size <- length
        | _ ->
               match param.TypeMapping.DbType with
               | DbType.String -> p.Size <- 32767
               | _ -> ()
        p

    let fieldNotationAlias(al:alias,col:SqlColumnType) =
        let aliasSprint =
            match String.IsNullOrEmpty(al) with
            | true -> fun c -> sprintf "\"%s\"" c
            | false -> fun c -> sprintf "\"%s.%s\"" al c
        Utilities.genericAliasNotation aliasSprint col

    let createTypeMappings con =
        let getDbType(providerType:int) =
#if REFLECTIONLOAD
            let p = Activator.CreateInstance(parameterType.Value,[||]) :?> IDbDataParameter
            let oracleDbTypeSetter = parameterType.Value.GetProperty("OracleDbType").GetSetMethod()
            let dbTypeGetter = parameterType.Value.GetProperty("DbType").GetGetMethod()
            oracleDbTypeSetter.Invoke(p, [|providerType|]) |> ignore
            dbTypeGetter.Invoke(p, [||]) :?> DbType
#else
            let p = new Oracle.ManagedDataAccess.Client.OracleParameter()
            p.OracleDbType <- Enum.ToObject(typeof<Oracle.ManagedDataAccess.Client.OracleDbType>, providerType) :?> Oracle.ManagedDataAccess.Client.OracleDbType
            p.DbType
#endif

        let getClrType (input:string) =
            (match input.ToLower() with
            | "system.long"  -> typeof<System.Int64>
            | _ -> Utilities.getType(input)).ToString()

        let mappings =
            [
                let dt = getSchema "DataTypes" [||] con
                for r in dt.Rows do
                    let clrType = getClrType (string r.["DataType"])
                    let oracleType = string r.["TypeName"]
                    let providerType = unbox<int> r.["ProviderDbType"]
                    let dbType = getDbType providerType
                    yield { ProviderTypeName = ValueSome oracleType; ClrType = clrType; DbType = dbType; ProviderType = ValueSome providerType; }
                yield { ProviderTypeName = ValueSome "REF CURSOR"; ClrType = (typeof<SqlEntity[]>).ToString(); DbType = DbType.Object; ProviderType = ValueSome 121; }
            ]

        let clrMappings =
            mappings
            |> List.map (fun m -> m.ClrType, m)
            |> Map.ofList

        let oracleMappings =
            mappings
            |> List.map (fun m -> (match m.ProviderTypeName with ValueSome x -> x | ValueNone -> ""), m)
            |> Map.ofList

        typeMappings <- mappings
        findClrType <- clrMappings.TryFind
        findDbType <- oracleMappings.TryFind

    let tryReadValueProperty (instance:obj) =
        if isNull instance then None else
            let typ = instance.GetType()
            let isNullp = 
                let isNullProp = typ.GetProperty("IsNull")
                if isNull isNullProp then false
                else unbox<bool>(isNullProp.GetGetMethod().Invoke(instance, [||]))
            let prop = typ.GetProperty("Value")
            if not(isNullp || isNull prop)
            then prop.GetGetMethod().Invoke(instance, [||]) |> Some
            else None

    let createConnection connectionString =
#if REFLECTIONLOAD
        try
            Activator.CreateInstance(connectionType.Value,[|box connectionString|]) :?> IDbConnection
        with
        | :? System.Reflection.ReflectionTypeLoadException as ex ->
            let errorfiles = ex.LoaderExceptions |> Array.map(fun e -> e.GetBaseException().Message) |> Seq.distinct |> Seq.toArray
            let msg = ex.Message + "\r\n" + String.Join("\r\n", errorfiles)
            raise(System.Reflection.TargetInvocationException(msg, ex))
        | :? System.Reflection.TargetInvocationException as ex when ((not(isNull ex.InnerException)) && ex.InnerException :? DllNotFoundException) ->
            let platform = Reflection.getPlatform(Reflection.execAssembly.Force())
            let msg = ex.GetBaseException().Message + ", Path: " + (Reflection.listResolutionFullPaths resolutionPath) +
                        (if platform <> "" then Environment.NewLine +  "Current execution platform: " + platform else "")
            raise(System.Reflection.TargetInvocationException(msg, ex))
        | :? System.TypeInitializationException as te when (te.InnerException :? System.Reflection.TargetInvocationException) ->
            let ex = te.InnerException :?> System.Reflection.TargetInvocationException
            let platform = Reflection.getPlatform(Reflection.execAssembly.Force())
            let msg = ex.GetBaseException().Message + ", Path: " + (Reflection.listResolutionFullPaths resolutionPath) +
                        (if platform <> "" then Environment.NewLine +  "Current execution platform: " + platform else "")
            raise(System.Reflection.TargetInvocationException(msg, ex.InnerException)) 
        | :? System.TypeInitializationException as te when not(isNull te.InnerException) -> raise (te.GetBaseException())
#else
        new Oracle.ManagedDataAccess.Client.OracleConnection(connectionString) :> IDbConnection
#endif

    let createCommand commandText (connection:IDbConnection) =
#if REFLECTIONLOAD
        Activator.CreateInstance(commandType.Value,[|box commandText;box connection|]) :?> IDbCommand
#else
        new Oracle.ManagedDataAccess.Client.OracleCommand(commandText, (connection :?> Oracle.ManagedDataAccess.Client.OracleConnection) ) :> IDbCommand
#endif

    let readParameter (parameter:IDbDataParameter) =
#if REFLECTIONLOAD
        let parameterType = parameterType.Value
        let oracleDbTypeGetter =
            parameterType.GetProperty("OracleDbType").GetGetMethod()
        let pti = oracleDbTypeGetter.Invoke(parameter, [||]) :?> int
#else
        let pti = int (parameter :?> Oracle.ManagedDataAccess.Client.OracleParameter).OracleDbType
#endif

        match parameter.DbType, pti with
        | DbType.Object, 121 ->
             if isNull parameter.Value
             then null
             else
                let data =
                    Sql.dataReaderToArray (
#if REFLECTIONLOAD
                            getDataReaderForRefCursor.Value.Invoke(parameter.Value, [||]) :?> IDataReader
#else
                            (parameter.Value :?> Oracle.ManagedDataAccess.Types.OracleRefCursor).GetDataReader() :> IDataReader
#endif
                        )
                    |> Seq.ofArray
                data |> box
        | _, _ ->
            match tryReadValueProperty parameter.Value with
            | Some(obj) -> obj |> box
            | _ -> parameter.Value |> box

    let readParameterAsync (parameter:IDbDataParameter) =
        task {
#if REFLECTIONLOAD
            let parameterType = parameterType.Value
            let oracleDbTypeGetter =
                parameterType.GetProperty("OracleDbType").GetGetMethod()
            let pti = oracleDbTypeGetter.Invoke(parameter, [||]) :?> int
#else
            let pti = int (parameter :?> Oracle.ManagedDataAccess.Client.OracleParameter).OracleDbType
#endif

            match parameter.DbType, pti with
            | DbType.Object, 121 ->
                 if isNull parameter.Value
                 then return null
                 else
                    let! data =
                        Sql.dataReaderToArrayAsync (
#if REFLECTIONLOAD
                                getDataReaderForRefCursor.Value.Invoke(parameter.Value, [||]) :?> Common.DbDataReader
#else
                                (parameter.Value :?> Oracle.ManagedDataAccess.Types.OracleRefCursor).GetDataReader() :> Common.DbDataReader
#endif
                            )
                    return data |> Seq.ofArray |> box
            | _, _ ->
                match tryReadValueProperty parameter.Value with
                | Some(obj) -> return obj |> box
                | _ -> return parameter.Value |> box
        }

    let read conn f sql =
      seq { use cmd = createCommand sql conn
            use reader = cmd.ExecuteReader()
            while reader.Read()
              do yield f reader }

    let getIndexColumns tableNames conn = 
        let whereTableName = SchemaProjections.buildTableNameWhereFilter "table_name" tableNames
        let whereNotSystemOwner = 
          systemNames 
          |> List.map (sprintf "'%s'") 
          |> String.concat ","
          |> sprintf "index_owner not in (%s)"

        sprintf """select index_name, column_name
                   from all_ind_columns
                   where %s
                   %s'""" whereNotSystemOwner whereTableName
        |> read conn (fun row -> row.[0], row.[1]) 
        |> dict
               
    let getPrimaryKeys tableNames conn =
        let whereTableName = SchemaProjections.buildTableNameWhereFilter "a.table_name" tableNames
        sprintf """select c.constraint_name, a.table_name, a.column_name, c.index_name 
                   from all_cons_columns a
                   join all_constraints c on a.constraint_name = c.constraint_name
                   where c.constraint_type = 'P' and a.table_name not like 'BIN$%%'
                   %s""" whereTableName
        |> read conn (fun row -> 
            let tableName  = Sql.dbUnbox row.[1]
            let columnName = [Sql.dbUnbox row.[2]]
            tableName, columnName)
        |> dict

    let getTables tableNames conn = 
        let whereTableName = SchemaProjections.buildTableNameWhereFilter "table_name" tableNames
        sprintf """select owner, table_name, table_type
                   from all_catalog
                   where owner != 'System'
                     and table_type in ('TABLE', 'VIEW')
                   %s""" whereTableName
        |> read conn (fun row -> 
            { Schema = Sql.dbUnbox row.[0];
              Name   = Sql.dbUnbox row.[1];
              Type   = Sql.dbUnbox row.[2] })
        |> Seq.toArray

    let getColumns (primaryKeys:IDictionary<_,_>) (table : Table) conn = 
        sprintf """select data_type, nullable, column_name, data_length, data_default, virtual_column from all_tab_cols where table_name = '%s' and owner = '%s'""" table.Name table.Schema
        |> read conn (fun row ->
                let columnType : string = Sql.dbUnbox row.[0]
                // Remove precision specification from the column type name (can appear in TIMESTAMP and INTERVAL)
                // Example: 'TIMESTAMP(3) WITH TIMEZONE' must be transformed to 'TIMESTAMP WITH TIMEZONE'
                let columnType = 
                    match columnType.IndexOf('('), columnType.IndexOf(')') with
                    | x,y when x > 0 && y > 0 -> columnType.Substring(0,x) + columnType.Substring(y+1)
                    | _ -> columnType
                let nullable   = (Sql.dbUnbox row.[1]) = "Y"
                let columnName = Sql.dbUnbox row.[2]
                let typeinfo = 
                    let datalength = (Sql.dbUnbox row.[3]).ToString()
                    if datalength <> "0" then columnType
                    else columnType + "(" + datalength + ")"
                findDbType columnType
                |> Option.map (fun m ->
                    let pkColumn = primaryKeys.TryGetValue(table.Name) |> function | true, pks -> pks = [columnName] | false, _ -> false
                    { Name = columnName
                      TypeMapping = m
                      IsPrimaryKey = pkColumn
                      IsNullable = nullable
                      IsAutonumber = pkColumn
                      HasDefault = not (isNull row.[4])
                      IsComputed = (Sql.dbUnbox row.[5]) = "YES" && not (isNull row.[4])
                      TypeInfo = ValueSome typeinfo }
                ))
        |> Seq.choose id
        |> Seq.map (fun c -> c.Name, c)
        |> Map.ofSeq

    let getRelationships (primaryKeys:IDictionary<_,_>) table con =
        let foreignKeyCols =
            getSchema "ForeignKeyColumns" [|owner;table|] con
            |> DataTable.map (fun row -> (Sql.dbUnbox row.[1], Sql.dbUnbox row.[3]))
            |> Map.ofList
        let rels =
            getSchema "ForeignKeys" [|owner;table|] con
            |> DataTable.mapChoose (fun row ->
                let name = Sql.dbUnbox row.[4]
                match primaryKeys.TryGetValue(table) with
                | true, pks ->
                    match pks, foreignKeyCols.TryFind name with
                    | [pk], Some(fk) ->
                         { Name = name
                           PrimaryTable = Table.CreateFullName(Sql.dbUnbox row.[1],Sql.dbUnbox row.[2])
                           PrimaryKey = pk
                           ForeignTable = Table.CreateFullName(Sql.dbUnbox row.[3],Sql.dbUnbox row.[5])
                           ForeignKey = fk } |> Some
                    | _, Some(fk) -> None
                    | _, None -> None
                | false, _ -> None
            ) |> Seq.toArray
        let children =
            rels |> Array.map (fun x ->
                { Name = x.Name
                  PrimaryTable = x.ForeignTable
                  PrimaryKey = x.ForeignKey
                  ForeignTable = x.PrimaryTable
                  ForeignKey = x.PrimaryKey }
            )
        (children, rels)

    let getIndivdualsQueryText amount (table:Table) =
        sprintf "select * from ( select * from %s order by 1 desc) where ROWNUM <= %i" table.FullName amount

    let getIndivdualQueryText (table:Table) column =
        let tName = table.FullName
        sprintf "SELECT * FROM %s WHERE %s.%s = :id" tName tName (quoteWhiteSpace column)

    let getSprocReturnColumns (sparams: QueryParameter list) =
        sparams
        |> List.filter (fun x -> x.Direction <> ParameterDirection.Input)
        |> List.mapi (fun i p ->
            let name = if (String.IsNullOrEmpty p.Name) && (i > 0) then "ReturnValue" + (string i)
                       elif (String.IsNullOrEmpty p.Name) then "ReturnValue"
                       else p.Name
            QueryParameter.Create(name,p.Ordinal,p.TypeMapping,p.Direction))

    let getSprocName (row:DataRow) =
        let owner = Sql.dbUnbox row.["OWNER"]
        let procName = Sql.dbUnbox row.["OBJECT_NAME"]
        let packageName = 
            match row.Table.Columns.Contains("PACKAGE_NAME") with
            | true -> Sql.dbUnbox row.["PACKAGE_NAME"]
            | false -> ""
        { ProcName = procName; Owner = owner; PackageName = packageName }

    let getSprocParameters (con:IDbConnection) (name:SprocName) =
        let querySprocParameters packageName sprocName =
            let sql = 
                if String.IsNullOrWhiteSpace(packageName)
                then sprintf "SELECT * FROM SYS.ALL_ARGUMENTS WHERE OBJECT_NAME = '%s' AND (OVERLOAD = 1 OR OVERLOAD IS NULL) AND DATA_LEVEL = 0" sprocName
                else sprintf "SELECT * FROM SYS.ALL_ARGUMENTS WHERE OBJECT_NAME = '%s' AND PACKAGE_NAME = '%s' AND (OVERLOAD = 1 OR OVERLOAD IS NULL) AND DATA_LEVEL = 0" sprocName packageName 

            Sql.executeSqlAsDataTable createCommand sql con
        
        let createSprocParameters (row:DataRow) =
           let dataType = Sql.dbUnbox row.["DATA_TYPE"]
           let argumentName = Sql.dbUnbox row.["ARGUMENT_NAME"]
           let maxLength = ValueSome(int(Sql.dbUnboxWithDefault<decimal> -1M row.["DATA_LENGTH"]))

           findDbType dataType
           |> Option.map (fun m ->
               let direction =
                   match Sql.dbUnbox row.["IN_OUT"] with
                   | "IN" -> ParameterDirection.Input
                   | "OUT" when String.IsNullOrEmpty(argumentName) -> ParameterDirection.ReturnValue
                   | "OUT" -> ParameterDirection.Output
                   | "IN/OUT" -> ParameterDirection.InputOutput
                   | a -> failwithf "Direction not supported %s" a
               { Name = Sql.dbUnbox row.["ARGUMENT_NAME"]
                 TypeMapping = m
                 Direction = direction
                 Length = maxLength
                 Ordinal = int(Sql.dbUnbox<decimal> row.["POSITION"]) }
           )

        querySprocParameters name.PackageName name.ProcName
        |> DataTable.mapChoose createSprocParameters
        |> Seq.sortBy (fun x -> x.Ordinal)
        |> Seq.toList
    
    let getPackageSprocs (con:IDbConnection) packageName = 
        let sql = 
            sprintf """SELECT * 
                       FROM SYS.ALL_PROCEDURES 
                       WHERE 
                            OBJECT_TYPE = 'PACKAGE' 
                            AND OBJECT_NAME = '%s' 
                            AND PROCEDURE_NAME IS NOT NULL
                            AND (OVERLOAD = 1 OR OVERLOAD IS NULL) 
                   """ packageName

        let procs = 
            Sql.executeSqlAsDataTable createCommand sql con
            |> DataTable.map (fun row -> 
                let name = 
                    let owner = Sql.dbUnbox row.["OWNER"]
                    let procName = Sql.dbUnbox row.["PROCEDURE_NAME"]
                    let packageName = Sql.dbUnbox row.["OBJECT_NAME"]
                       
                    { ProcName = procName; Owner = owner; PackageName = packageName }

                { Name = name; Params = (fun con -> getSprocParameters con name); ReturnColumns = (fun _ sparams -> getSprocReturnColumns sparams) }
            
            )
        procs

    let getSprocs con =

        let buildDef classType row =
            let name = getSprocName row
            Root(classType, Sproc({ Name = name; Params = (fun con -> getSprocParameters con name); ReturnColumns = (fun _ sparams -> getSprocReturnColumns sparams) }))
        
        let buildPackageDef classType (row:DataRow) =
            let name = Sql.dbUnbox<string> row.["OBJECT_NAME"]
            Root(classType, Package(name, { Name = name; Sprocs = (fun con -> getPackageSprocs con name) }))

        let functions =
            (getSchema "Functions" [|owner|] con)
            |> DataTable.map (fun row -> buildDef "Functions" row)

        let procs =
            (getSchema "Procedures" [|owner|] con)
            |> DataTable.map (fun row -> buildDef "Procedures" row)

        let packages = 
            (getSchema "Packages" [|owner|] con)
            |> DataTable.map (fun row -> buildPackageDef "Packages" row)

        functions @ procs @ packages

    let executeSprocCommandCommon (inputParameters:QueryParameter []) (retCols:QueryParameter[]) (values:obj[]) =
        let inputParameters = inputParameters |> Array.filter (fun p -> p.Direction = ParameterDirection.Input)

        let outps =
             retCols
             |> Array.map(fun ip ->
                 let p = createCommandParameter ip null
                 (ip.Ordinal, p))

        let inps =
             inputParameters
             |> Array.mapi(fun i ip ->
                 let p = createCommandParameter ip values.[i]
                 (ip.Ordinal,p))

        let allParams =
            Array.append outps inps
            |> Array.sortBy fst

        allParams, outps

    let executeSprocCommand (com:IDbCommand) (inputParameters:QueryParameter[]) (retCols:QueryParameter[]) (values:obj[]) =

        let allParams, outps = executeSprocCommandCommon inputParameters retCols values
        allParams |> Array.iter (fun (_,p) -> com.Parameters.Add(p) |> ignore)

        let entities =
            match retCols with
            | [||] -> com.ExecuteNonQuery() |> ignore; Unit
            | [|col|] ->
                use reader = com.ExecuteReader()
                match col.TypeMapping.ProviderTypeName with
                | ValueSome "REF CURSOR" -> SingleResultSet(col.Name, Sql.dataReaderToArray reader)
                | _ ->
                    match outps |> Array.tryFind (fun (_,p) -> p.ParameterName = col.Name) with
                    | Some(_,p) -> Scalar(p.ParameterName, readParameter p)
                    | None -> failwithf "Excepted return column %s but could not find it in the parameter set" col.Name
            | cols ->
                com.ExecuteNonQuery() |> ignore
                let returnValues =
                    cols
                    |> Array.map (fun col ->
                        match outps |> Array.tryFind (fun (_,p) -> p.ParameterName = col.Name) with
                        | Some(_,p) ->
                            match col.TypeMapping.ProviderTypeName with
                            | ValueSome "REF CURSOR" -> ResultSet(col.Name, readParameter p :?> ResultSet)
                            | _ -> ScalarResultSet(col.Name, readParameter p)
                        | None -> failwithf "Excepted return column %s but could not find it in the parameter set" col.Name
                    )
                Set(returnValues)
        entities

    let executeSprocCommandAsync (com:System.Data.Common.DbCommand) (inputParameters:QueryParameter[]) (retCols:QueryParameter[]) (values:obj[]) =
        task {

            let allParams, outps = executeSprocCommandCommon inputParameters retCols values
            allParams |> Array.iter (fun (_,p) -> com.Parameters.Add(p) |> ignore)

            match retCols with
            | [||] -> let! r = com.ExecuteNonQueryAsync()
                      return Unit
            | [|col|] ->
                use! reader = com.ExecuteReaderAsync()
                match col.TypeMapping.ProviderTypeName with
                | ValueSome "REF CURSOR" -> 
                    let! r = Sql.dataReaderToArrayAsync reader
                    return SingleResultSet(col.Name, r)
                | _ ->
                    match outps |> Array.tryFind (fun (_,p) -> p.ParameterName = col.Name) with
                    | Some(_,p) -> 
                        let! r = readParameterAsync p
                        return Scalar(p.ParameterName, r)
                    | None -> return failwithf "Excepted return column %s but could not find it in the parameter set" col.Name
            | cols ->
                let! r = com.ExecuteNonQueryAsync()
                let! returnValues =
                    cols |> Array.toList
                    |> Sql.evaluateOneByOne (fun col ->
                        task {
                            match outps |> Array.tryFind (fun (_,p) -> p.ParameterName = col.Name) with
                            | Some(_,p) ->
                                let! r = readParameterAsync p
                                match col.TypeMapping.ProviderTypeName with
                                | ValueSome "REF CURSOR" -> return ResultSet(col.Name, r :?> ResultSet)
                                | _ -> return ScalarResultSet(col.Name, r)
                            | None -> return failwithf "Excepted return column %s but could not find it in the parameter set" col.Name
                        }
                    )
                return Set(returnValues)
        }

type internal OracleProvider(resolutionPath, contextSchemaPath, owner, referencedAssemblies, tableNames) =
    let schemaCache = SchemaCache.LoadOrEmpty(contextSchemaPath)
    let myLock = new Object()

    let isPrimaryKey tableName columnName = 
        match schemaCache.PrimaryKeys.TryGetValue tableName with
        | true, pk when pk = [columnName] -> true
        | _ -> false

    let createInsertCommand (provider:ISqlProvider) (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore

        let columnNames, values =
            (([],0),entity.ColumnValues)
            ||> Seq.fold(fun (out,i) (k,v) ->
                let name = sprintf ":param%i" i
                let p = provider.CreateCommandParameter(QueryParameter.Create(name,i), v)
                (k,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray
            |> Array.unzip

        sb.Clear() |> ignore
        ~~(sprintf "INSERT INTO %s (%s) VALUES (%s)"
            ((entity :> IColumnHolder).Table.FullName)
            (String.Join(",",columnNames))
            (String.Join(",",values |> Array.map(fun p -> p.ParameterName))))
        let cmd = provider.CreateCommand(con, sb.ToString())
        values |> Array.iter (cmd.Parameters.Add >> ignore)
        cmd

    let createUpdateCommand (provider:ISqlProvider) (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) (changedColumns: string list) =
        let (~~) (t:string) = sb.Append t |> ignore
        sb.Clear() |> ignore

        if changedColumns |> List.exists (isPrimaryKey (entity :> IColumnHolder).Table.Name) 
        then failwith "Error - you cannot change the primary key of an entity."

        let pk = schemaCache.PrimaryKeys.[(entity :> IColumnHolder).Table.Name]
        let pkValues =
            match (entity :> IColumnHolder).GetPkColumnOption<obj> pk with
            | [] -> failwith ("Error - you cannot update an entity that does not have a primary key. (" + (entity :> IColumnHolder).Table.FullName + ")")
            | v -> v

        let columns, parameters =
            (([],0),changedColumns)
            ||> List.fold(fun (out,i) col ->
                let name = sprintf ":param%i" i
                let p =
                    match (entity :> IColumnHolder).GetColumnOption<obj> col with
                    | Some v -> provider.CreateCommandParameter(QueryParameter.Create(name,i), v)
                    | None -> provider.CreateCommandParameter(QueryParameter.Create(name,i), DBNull.Value)
                (col,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray
            |> Array.unzip

        match pk with
        | [] -> ()
        | ks -> 
            ~~(sprintf "UPDATE %s SET (%s) = (SELECT %s FROM DUAL) WHERE "
                ((entity :> IColumnHolder).Table.FullName)
                ((String.concat "," columns))
                (String.concat "," (parameters |> Array.map (fun p -> p.ParameterName))))
            ~~(String.concat " AND " (ks |> List.mapi(fun i k -> (sprintf "\"%s\" = :pk%i" k i))))

        let cmd = provider.CreateCommand(con, sb.ToString())
        parameters |> Array.iter (cmd.Parameters.Add >> ignore)
        pkValues |> List.iteri(fun i pkValue ->
            let pkParam = provider.CreateCommandParameter(QueryParameter.Create((":pk"+i.ToString()),i), pkValue)
            cmd.Parameters.Add pkParam |> ignore
        )
        cmd

    let createDeleteCommand (provider:ISqlProvider) (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore
        let pk = schemaCache.PrimaryKeys.[(entity :> IColumnHolder).Table.Name]
        sb.Clear() |> ignore
        let pkValues =
            match (entity :> IColumnHolder).GetPkColumnOption<obj> pk with
            | [] -> failwith ("Error - you cannot delete an entity that does not have a primary key. (" + (entity :> IColumnHolder).Table.FullName + ")")
            | v -> v

        match pk with
        | [] -> ()
        | ks -> 
            ~~(sprintf "DELETE FROM %s WHERE " (entity :> IColumnHolder).Table.FullName)
            ~~(String.concat " AND " (ks |> List.mapi(fun i k -> (sprintf "\"%s\" = :pk%i" k i))))

        let cmd = provider.CreateCommand(con, sb.ToString())
        pkValues |> List.iteri(fun i pkValue ->
            let pkParam = provider.CreateCommandParameter(QueryParameter.Create((":pk"+i.ToString()),i), pkValue)
            cmd.Parameters.Add pkParam |> ignore
        )
        cmd

    do
        Oracle.owner <- owner
#if REFLECTIONLOAD
        Oracle.referencedAssemblies <- referencedAssemblies
#endif
        Oracle.resolutionPath <- resolutionPath

    interface ISqlProvider with
        member __.CloseConnectionAfterQuery = true
        member __.DesignConnection = true
        member __.StoredProcedures = true
        member __.GetLockObject() = myLock
        member __.GetTableDescription(con,tableName) = 
            let sn = tableName.Substring(0,tableName.LastIndexOf('.')) 
            let tn = tableName.Substring(tableName.LastIndexOf('.')+1) 
            Sql.connect con (fun con -> 
                let comment =
                    sprintf """SELECT COMMENTS 
                                FROM user_tab_comments 
                                WHERE TABLE_NAME = '%s'
                                AND COMMENTS <> '-'
                            """ tn 
                    |> Oracle.read con (fun row -> 
                        Sql.dbUnbox row.[0])
                    |> Seq.toList
                match comment with
                | [x] -> x
                | _ -> 
                    ""
            )
        member __.GetColumnDescription(con,tableName,columnName) = 
            let sn = tableName.Substring(0,tableName.LastIndexOf('.')) 
            let tn = tableName.Substring(tableName.LastIndexOf('.')+1) 
            Sql.connect con (fun con -> 
                let comment =
                    sprintf """SELECT COMMENTS 
                                FROM user_col_comments 
                                WHERE TABLE_NAME = '%s'
                                AND COLUMN_NAME = '%s'
                                AND COMMENTS <> '-'
                            """ tn columnName
                    |> Oracle.read con (fun row -> 
                        Sql.dbUnbox row.[0])
                    |> Seq.toList
                match comment with
                | [x] -> x
                | _ -> 
                    ""
            )
        member __.CreateConnection(connectionString) = Oracle.createConnection connectionString
        member __.CreateCommand(connection,commandText) =  Oracle.createCommand commandText connection
        member __.CreateCommandParameter(param, value) = Oracle.createCommandParameter param value
        member __.ExecuteSprocCommand(con, param ,retCols, values:obj array) = Oracle.executeSprocCommand con param retCols values
        member __.ExecuteSprocCommandAsync(con, param ,retCols, values:obj array) = Oracle.executeSprocCommandAsync con param retCols values
        member __.GetSchemaCache() = schemaCache

        member __.CreateTypeMappings(con) =
            Sql.connect con (fun con ->
                Oracle.createTypeMappings con
                Oracle.getPrimaryKeys tableNames con
                |> Seq.iter (fun pk -> schemaCache.PrimaryKeys.GetOrAdd(pk.Key, pk.Value) |> ignore))

        member __.GetTables(con,_) =
               if schemaCache.Tables.IsEmpty then
                    Sql.connect con (Oracle.getTables tableNames)
                    |> Array.map (fun t -> schemaCache.Tables.GetOrAdd(t.FullName, t))
               else schemaCache.Tables |> Seq.map (fun t -> t.Value) |> Seq.toArray

        member __.GetPrimaryKey(table) =
            match schemaCache.PrimaryKeys.TryGetValue table.Name with
            | true, v -> match v with [x] -> Some(x) | _ -> None
            | _ -> None

        member __.GetColumns(con,table) =
            match schemaCache.Columns.TryGetValue table.FullName  with
            | true, cols when cols.Count > 0 -> cols
            | _ ->
                let cols = Sql.connect con (Oracle.getColumns schemaCache.PrimaryKeys table)
                schemaCache.Columns.GetOrAdd(table.FullName, cols)

        member __.GetRelationships(con,table) =
            schemaCache.Relationships.GetOrAdd(table.FullName, fun name ->
                    let rels = Sql.connect con (Oracle.getRelationships schemaCache.PrimaryKeys table.Name)
                    rels)

        member __.GetSprocs(con) = Sql.connect con Oracle.getSprocs
        member __.GetIndividualsQueryText(table,amount) = Oracle.getIndivdualsQueryText amount table
        member __.GetIndividualQueryText(table,column) = Oracle.getIndivdualQueryText table column

        member this.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns,isDeleteScript, con) =
            let parameters = ResizeArray<_>()

            // NOTE: really need to assign the parameters their correct db types
            let param = ref 0
            let nextParam() =
                incr param
                sprintf ":param%i" !param

            let createParam (columnDataType:DbType voption) (value:obj) =
                let paramName = nextParam()
                let p = Oracle.createCommandParameter (QueryParameter.Create(paramName, !param)) value
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
                    | true -> quoteWhiteSpace
                    | false -> fun col -> sprintf "%s.%s" al (quoteWhiteSpace col)
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
                    | Substring(SqlConstant startPos) -> sprintf "SUBSTR(%s, %s)" column (fieldParam startPos)
                    | Substring(SqlCol(al2, col2)) -> sprintf "SUBSTR(%s, %s)" column (fieldNotation al2 col2)
                    | SubstringWithLength(SqlConstant startPos, SqlConstant strLen) -> sprintf "SUBSTR(%s, %s, %s)" column (fieldParam startPos) (fieldParam strLen)
                    | SubstringWithLength(SqlConstant startPos,SqlCol(al2, col2)) -> sprintf "SUBSTR(%s, %s, %s)" column (fieldParam startPos) (fieldNotation al2 col2)
                    | SubstringWithLength(SqlCol(al2, col2), SqlConstant strLen) -> sprintf "SUBSTR(%s, %s, %s)" column (fieldNotation al2 col2) (fieldParam strLen)
                    | SubstringWithLength(SqlCol(al2, col2),SqlCol(al3, col3)) -> sprintf "SUBSTR(%s, %s, %s)" column (fieldNotation al2 col2) (fieldNotation al3 col3)
                    | Trim -> sprintf "TRIM(%s)" column
                    | Length -> sprintf "LENGTH(%s)" column
                    | IndexOf(SqlConstant search) -> sprintf "INSTR(%s,%s)" column (fieldParam search)
                    | IndexOf(SqlCol(al2, col2)) -> sprintf "INSTR(%s,%s)" column (fieldNotation al2 col2)
                    | IndexOfStart(SqlConstant search,(SqlConstant startPos)) -> sprintf "INSTR(%s,%s,%s)" column (fieldParam search) (fieldParam startPos)
                    | IndexOfStart(SqlConstant search, SqlCol(al2, col2)) -> sprintf "INSTR(%s,%s,%s)" column (fieldParam search) (fieldNotation al2 col2)
                    | IndexOfStart(SqlCol(al2, col2),(SqlConstant startPos)) -> sprintf "INSTR(%s,%s,%s)" column (fieldNotation al2 col2) (fieldParam startPos)
                    | IndexOfStart(SqlCol(al2, col2),SqlCol(al3, col3)) -> sprintf "INSTR(%s,%s,%s)" column (fieldNotation al2 col2) (fieldNotation al3 col3)
                    | CastVarchar -> sprintf "CAST(%s AS VARCHAR)" column
                    | CastInt -> sprintf "CAST(%s AS INT)" column
                    // Date functions
                    | Date -> sprintf "TRUNC(%s)" column
                    | Year -> sprintf "EXTRACT(YEAR FROM %s)" column
                    | Month -> sprintf "EXTRACT(MONTH FROM %s)" column
                    | Day -> sprintf "EXTRACT(DAY FROM %s)" column
                    | Hour -> sprintf "EXTRACT(HOUR FROM %s)" column
                    | Minute -> sprintf "EXTRACT(MINUTE FROM %s)" column
                    | Second -> sprintf "EXTRACT(SECOND FROM %s)" column
                    //Todo: Check if these support parameters. If not, use Utilities.fieldConstant instead of fieldParam
                    | AddYears(SqlConstant x) -> sprintf "(%s + INTERVAL %s YEAR)" column (fieldParam x)
                    | AddYears(SqlCol(al2, col2)) -> sprintf "(%s + INTERVAL %s YEAR)" column (fieldNotation al2 col2)
                    | AddMonths x -> sprintf "(%s + INTERVAL '%d' MONTH)" column x
                    | AddDays(SqlConstant x) -> sprintf "(%s + INTERVAL %s DAY)" column (fieldParam x) // SQL ignores decimal part :-(
                    | AddDays(SqlCol(al2, col2)) -> sprintf "(%s + INTERVAL %s DAY)" column (fieldNotation al2 col2)
                    | AddHours x -> sprintf "(%s + INTERVAL '%f' HOUR)" column x
                    | AddMinutes(SqlConstant x) -> sprintf "(%s + INTERVAL %s MINUTE)" column (fieldParam x)
                    | AddMinutes(SqlCol(al2, col2)) -> sprintf "(%s + INTERVAL %s MINUTE)" column (fieldNotation al2 col2)
                    | AddSeconds x -> sprintf "(%s + INTERVAL '%f' SECOND)" column x
                    | DateDiffDays(SqlCol(al2, col2)) -> sprintf "(%s-%s)" column (fieldNotation al2 col2)
                    | DateDiffSecs(SqlCol(al2, col2)) -> sprintf "(%s-%s)*60*60*24" column (fieldNotation al2 col2)
                    | DateDiffDays(SqlConstant x) -> sprintf "(%s-%s)" column (fieldParam x)
                    | DateDiffSecs(SqlConstant x) -> sprintf "(%s-%s)*60*60*24" column (fieldParam x)
                    // Math functions
                    | Truncate -> sprintf "TRUNC(%s)" column
                    | Ceil -> sprintf "CEIL(%s)" column
                    | BasicMathOfColumns(o, a, c) -> sprintf "(%s %s %s)" column o (fieldNotation a c)
                    | BasicMath(o, par) when (par :? String || par :? Char) -> sprintf "(%s %s %s)" column o (fieldParam par)
                    | BasicMathLeft(o, par) when (par :? String || par :? Char) -> sprintf "(%s %s %s)" (fieldParam par) o column
                    | Greatest(SqlConstant x) -> sprintf "GREATEST(%s, %s)" column (fieldParam x)
                    | Greatest(SqlCol(al2, col2)) -> sprintf "GREATEST(%s, %s)" column (fieldNotation al2 col2)
                    | Least(SqlConstant x) -> sprintf "LEAST(%s, %s)" column (fieldParam x)
                    | Least(SqlCol(al2, col2)) -> sprintf "LEAST(%s, %s)" column (fieldNotation al2 col2)
                    | Pow(SqlCol(al2, col2)) -> sprintf "POWER(%s, %s)" column (fieldNotation al2 col2)
                    | Pow(SqlConstant x) -> sprintf "POWER(%s, %s)" column (fieldParam x)
                    | PowConst(SqlConstant x) -> sprintf "POWER(%s, %s)" (fieldParam x) column
                    //if-then-else
                    | CaseSql(f, SqlCol(al2, col2)) -> sprintf "CASE WHEN %s THEN %s ELSE %s END " (buildf f) column (fieldNotation al2 col2)
                    | CaseSql(f, SqlConstant itm) -> sprintf "CASE WHEN %s THEN %s ELSE %s END " (buildf f) column (fieldParam itm)
                    | CaseNotSql(f, SqlConstant itm) -> sprintf "CASE WHEN %s THEN %s ELSE %s END " (buildf f) (fieldParam itm) column
                    | CaseSqlPlain(Condition.ConstantTrue, itm, _) -> sprintf " %s " (fieldParam itm)
                    | CaseSqlPlain(Condition.ConstantFalse, _, itm2) -> sprintf " %s " (fieldParam itm2)
                    | CaseSqlPlain(f, itm, itm2) -> sprintf "CASE WHEN %s THEN %s ELSE %s END " (buildf f) (fieldParam itm) (fieldParam itm2)
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
                                                Array.init (elements.Length) (elements.GetValue >> createParam columnDataType)
                                            | Some(x) -> [|createParam columnDataType (box x)|]
                                            | None ->    [|createParam columnDataType null|]

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
                                        | FSharp.Data.Sql.NestedIn ->
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
                                        | FSharp.Data.Sql.NestedNotIn ->
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
            // now we can build the sql query that has been simplified by the above expression converter
            // working on the basis that we will alias everything to make my life eaiser
            // build the select statment, this is easy ...
            let selectcolumns =
                if projectionColumns |> Seq.isEmpty then "1" else
                (String.concat ","
                    [|for KeyValue(k,v) in projectionColumns do
                        let cols = (getTable k).FullName
                        let k = if k <> "" then k elif baseAlias <> "" then baseAlias else baseTable.Name
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in schemaCache.Columns.[cols] |> Seq.map (fun c -> c.Key) do
                                if singleEntity then yield sprintf "%s.%s as \"%s\"" k col col
                                else yield sprintf "%s.%s as \"%s.%s\"" k col k col
                        else
                            for colp in v |> Seq.distinct do
                                match colp with
                                | EntityColumn col ->
                                    if singleEntity then yield sprintf "%s.%s as \"%s\"" k (quoteWhiteSpace col) col
                                    else yield sprintf "%s.%s as \"%s.%s\"" k (quoteWhiteSpace col) k col // F# makes this so easy :)
                                | OperationColumn(n,op) ->
                                    yield sprintf "%s as \"%s\"" (fieldNotation k op) n|])

            // Cache select-params to match group-by params
            let tmpGrpParams = Dictionary<(alias*SqlColumnType), string>()

            // Create sumBy, minBy, maxBy, ... field columns
            let columns =
                let extracolumns =
                    match sqlQuery.Grouping with
                    | [] -> FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation Oracle.fieldNotationAlias sqlQuery.AggregateOp
                    | g  -> 
                        let keys = g |> List.collect fst |> List.map(fun (a,c) ->
                            let fn = fieldNotation a c
                            if not (tmpGrpParams.ContainsKey (a,c)) then
                                tmpGrpParams.Add((a,c), fn)
                            if sqlQuery.Aliases.Count < 2 then fn
                            else sprintf "%s as \"%s\"" fn fn)
                        let aggs = g |> List.collect snd
                        let res2 = FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation Oracle.fieldNotationAlias aggs |> List.toSeq
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
                    ~~  (sprintf "%s %s %s on "
                            joinType destTable.FullName destAlias)
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
                    ~~ (sprintf "%s %s" (fieldNotation alias column) (if not desc then " DESC NULLS LAST" else " ASC NULLS FIRST")))

            if isDeleteScript then
                ~~(sprintf "DELETE FROM %s " baseTable.FullName)
            else 
                // SELECT
                if sqlQuery.Distinct && sqlQuery.Count then
                    let colsAggrs = columns.Split([|" as "|], StringSplitOptions.None)
                    let distColumns = colsAggrs.[0] + (if colsAggrs.Length = 2 then "" else " || ',' || " + String.Join(" || ',' || ", colsAggrs |> Seq.filter(fun c -> c.Contains ",") |> Seq.map(fun c -> c.Substring(c.IndexOf(',')+1))))
                    ~~(sprintf "SELECT COUNT(DISTINCT %s) " distColumns)
                elif sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s " columns)
                elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
                else  ~~(sprintf "SELECT %s " columns)
                // FROM
                let bal = if baseAlias = "" then baseTable.Name else baseAlias
                ~~(sprintf "FROM %s %s " baseTable.FullName bal)
                sqlQuery.CrossJoins |> Seq.iter(fun (a,t) -> ~~(sprintf ", %s %s " t.FullName a))
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
                ~~(sprintf " MINUS %s " suquery)
            | None -> ()

            //I think on oracle this will potentially impact the ordering as the row num is generated before any
            //filters or ordering is applied hance why this produces a nested query. something like
            //select * from (select ....) where ROWNUM <= 5.
            match sqlQuery.Take with
            | ValueSome v ->
                let sql = sprintf "select * from (%s) where ROWNUM <= %i" (sb.ToString()) v
                (sql, parameters)
            | ValueNone ->
                let sql = sb.ToString()
                (sql,parameters)

        member this.ProcessUpdates(con, entities, transactionOptions, timeout) =
            let sb = Text.StringBuilder()
            let provider = this :> ISqlProvider

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
                        use cmd = createInsertCommand provider con sb e
                        Common.QueryEvents.PublishSqlQueryICol con.ConnectionString cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        let id = cmd.ExecuteScalar()
                        match schemaCache.PrimaryKeys.TryGetValue (e :> IColumnHolder).Table.Name with
                        | true, pk ->
                            match (e :> IColumnHolder).GetPkColumnOption pk with
                            | [] ->  (e :> IColumnHolder).SetPkColumnSilent(pk, id)
                            | _ -> () // if the primary key exists, do nothing
                                            // this is because non-identity columns will have been set
                                            // manually and in that case scope_identity would bring back 0 "" or whatever
                        | false, _ -> ()
                        e._State <- Unchanged
                    | Modified fields ->
                        use cmd = createUpdateCommand provider con sb e fields
                        Common.QueryEvents.PublishSqlQueryICol con.ConnectionString cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        cmd.ExecuteNonQuery() |> ignore
                        e._State <- Unchanged
                    | Delete ->
                        use cmd = createDeleteCommand provider con sb e
                        Common.QueryEvents.PublishSqlQueryICol con.ConnectionString cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        cmd.ExecuteNonQuery() |> ignore
                        // remove the pk to prevent this attempting to be used again
                        (e :> IColumnHolder).SetPkColumnOptionSilent(schemaCache.PrimaryKeys.[(e :> IColumnHolder).Table.Name], None)
                        e._State <- Deleted
                    | Deleted | Unchanged -> failwithf "Unchanged entity encountered in update list - this should not be possible! (%O)" e)
                if not(isNull scope) then scope.Complete()

            finally
                con.Close()

        member this.ProcessUpdatesAsync(con, entities, transactionOptions, timeout) =
            let sb = Text.StringBuilder()
            let provider = this :> ISqlProvider

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
                                use cmd = createInsertCommand provider con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol con.ConnectionString cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                let! id = cmd.ExecuteScalarAsync()
                                match schemaCache.PrimaryKeys.TryGetValue (e :> IColumnHolder).Table.Name with
                                | true, pk ->
                                    match (e :> IColumnHolder).GetPkColumnOption pk with
                                    | [] ->  (e :> IColumnHolder).SetPkColumnSilent(pk, id)
                                    | _ -> () // if the primary key exists, do nothing
                                                    // this is because non-identity columns will have been set
                                                    // manually and in that case scope_identity would bring back 0 "" or whatever
                                | false, _ -> ()
                                e._State <- Unchanged
                            }
                        | Modified fields ->
                            task {
                                use cmd = createUpdateCommand provider con sb e fields :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol con.ConnectionString cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                let! c = cmd.ExecuteNonQueryAsync()
                                e._State <- Unchanged
                            }
                        | Delete ->
                            task {
                                use cmd = createDeleteCommand provider con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol con.ConnectionString cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                let! c = cmd.ExecuteNonQueryAsync()
                                // remove the pk to prevent this attempting to be used again
                                (e :> IColumnHolder).SetPkColumnOptionSilent(schemaCache.PrimaryKeys.[(e :> IColumnHolder).Table.Name], None)
                                e._State <- Deleted
                            }
                        | Deleted | Unchanged -> failwithf "Unchanged entity encountered in update list - this should not be possible! (%O)" e

                    let! _ = Sql.evaluateOneByOne handleEntity (CommonTasks.sortEntities entities |> Seq.toList)
                    if not(isNull scope) then scope.Complete()

                finally
                    con.Close()
            }
