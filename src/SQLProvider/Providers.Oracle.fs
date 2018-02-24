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
            let types = 
                try assembly.GetTypes() 
                with | :? System.Reflection.ReflectionTypeLoadException as e ->
                    let msgs = e.LoaderExceptions |> Seq.map(fun e -> e.GetBaseException().Message) |> Seq.distinct
                    let details = "Details: " + Environment.NewLine + String.Join(Environment.NewLine, msgs)
                    failwith (e.Message + Environment.NewLine + details)
            types |> Array.find(fun t -> t.Name = name)
        | Choice2Of2(paths, errors) ->
           let details = 
                match errors with 
                | [] -> "" 
                | x -> Environment.NewLine + "Details: " + Environment.NewLine + String.Join(Environment.NewLine, x)
           failwithf "Unable to resolve assemblies. One of %s must exist in the paths: %s %s %s"
                (String.Join(", ", assemblyNames |> List.toArray))
                Environment.NewLine
                (String.Join(Environment.NewLine, paths |> Seq.filter(fun p -> not(String.IsNullOrEmpty p))))
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

    let getSchema name (args:string[]) conn =
        getSchemaMethod.Value.Invoke(conn,[|name; args|]) :?> DataTable

    let mutable typeMappings = []
    let mutable findClrType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"

    let createCommandParameter (param:QueryParameter) value =
        let value = if value = null then (box System.DBNull.Value) else value
        let parameterType = parameterType.Value
        let oracleDbTypeSetter =
            parameterType.GetProperty("OracleDbType").GetSetMethod()

        let p = Activator.CreateInstance(parameterType,[|box param.Name; box value|]) :?> IDbDataParameter
        p.Direction <- param.Direction

        match param.TypeMapping.ProviderTypeName with
        | Some _ ->
            p.DbType <- param.TypeMapping.DbType
            param.TypeMapping.ProviderType |> Option.iter (fun pt -> oracleDbTypeSetter.Invoke(p, [|pt|]) |> ignore)
        | None -> ()

        match param.Length with
        | Some(length) when length >= 0 -> p.Size <- length
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
        let getDbType(providerType) =
            let p = Activator.CreateInstance(parameterType.Value,[||]) :?> IDbDataParameter
            let oracleDbTypeSetter = parameterType.Value.GetProperty("OracleDbType").GetSetMethod()
            let dbTypeGetter = parameterType.Value.GetProperty("DbType").GetGetMethod()
            oracleDbTypeSetter.Invoke(p, [|providerType|]) |> ignore
            dbTypeGetter.Invoke(p, [||]) :?> DbType

        let getClrType (input:string) =
            (match input.ToLower() with
            | "system.long"  -> typeof<System.Int64>
            | _ -> Type.GetType(input)).ToString()

        let mappings =
            [
                let dt = getSchema "DataTypes" [||] con
                for r in dt.Rows do
                    let clrType = getClrType (string r.["DataType"])
                    let oracleType = string r.["TypeName"]
                    let providerType = unbox<int> r.["ProviderDbType"]
                    let dbType = getDbType providerType
                    yield { ProviderTypeName = Some oracleType; ClrType = clrType; DbType = dbType; ProviderType = Some providerType; }
                yield { ProviderTypeName = Some "REF CURSOR"; ClrType = (typeof<SqlEntity[]>).ToString(); DbType = DbType.Object; ProviderType = Some 121; }
            ]

        let clrMappings =
            mappings
            |> List.map (fun m -> m.ClrType, m)
            |> Map.ofList

        let oracleMappings =
            mappings
            |> List.map (fun m -> m.ProviderTypeName.Value, m)
            |> Map.ofList

        typeMappings <- mappings
        findClrType <- clrMappings.TryFind
        findDbType <- oracleMappings.TryFind

    let tryReadValueProperty (instance:obj) =
        if instance <> null
        then
            let typ = instance.GetType()
            let isNull = 
                let isNullProp = typ.GetProperty("IsNull")
                if isNullProp <> null
                then unbox<bool>(isNullProp.GetGetMethod().Invoke(instance, [||]))
                else false
            let prop = typ.GetProperty("Value")
            if not(isNull) && prop <> null
            then prop.GetGetMethod().Invoke(instance, [||]) |> Some
            else None
        else None

    let createConnection connectionString =
        try
            Activator.CreateInstance(connectionType.Value,[|box connectionString|]) :?> IDbConnection
        with
        | :? System.Reflection.ReflectionTypeLoadException as ex ->
            let errorfiles = ex.LoaderExceptions |> Array.map(fun e -> e.GetBaseException().Message) |> Seq.distinct |> Seq.toArray
            let msg = ex.Message + "\r\n" + String.Join("\r\n", errorfiles)
            raise(new System.Reflection.TargetInvocationException(msg, ex))
        | :? System.Reflection.TargetInvocationException as ex when (ex.InnerException <> null && ex.InnerException :? DllNotFoundException) ->
            let msg = ex.GetBaseException().Message + ", Path: " + (System.IO.Path.GetFullPath resolutionPath)
            raise(new System.Reflection.TargetInvocationException(msg, ex))
        | :? System.TypeInitializationException as te when (te.InnerException :? System.Reflection.TargetInvocationException) ->
            let ex = te.InnerException :?> System.Reflection.TargetInvocationException
            let msg = ex.GetBaseException().Message + ", Path: " + (System.IO.Path.GetFullPath resolutionPath)
            raise(new System.Reflection.TargetInvocationException(msg, ex.InnerException)) 

    let createCommand commandText connection =
        Activator.CreateInstance(commandType.Value,[|box commandText;box connection|]) :?> IDbCommand

    let readParameter (parameter:IDbDataParameter) =
        let parameterType = parameterType.Value
        let oracleDbTypeGetter =
            parameterType.GetProperty("OracleDbType").GetGetMethod()

        match parameter.DbType, (oracleDbTypeGetter.Invoke(parameter, [||]) :?> int) with
        | DbType.Object, 121 ->
             if parameter.Value = null
             then null
             else
                let data =
                    Sql.dataReaderToArray (getDataReaderForRefCursor.Value.Invoke(parameter.Value, [||]) :?> IDataReader)
                    |> Seq.ofArray
                data |> box
        | _, _ ->
            match tryReadValueProperty parameter.Value with
            | Some(obj) -> obj |> box
            | _ -> parameter.Value |> box

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
            let pkName     = Sql.dbUnbox row.[0]
            let tableName  = Sql.dbUnbox row.[1]
            let columnName = [Sql.dbUnbox row.[2]]
            let indexName  = Sql.dbUnbox row.[3]
            tableName, { PrimaryKey.Name = pkName
                         Table = tableName
                         Column = columnName
                         IndexName = indexName })
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
        |> Seq.toList

    let getColumns (primaryKeys:IDictionary<_,_>) (table : Table) conn = 
        sprintf """select data_type, nullable, column_name, data_length from all_tab_columns where table_name = '%s' and owner = '%s'""" table.Name table.Schema
        |> read conn (fun row ->
                let columnType = Sql.dbUnbox row.[0]
                let nullable   = (Sql.dbUnbox row.[1]) = "Y"
                let columnName = Sql.dbUnbox row.[2]
                let typeinfo = 
                    let datalength = (Sql.dbUnbox row.[3]).ToString()
                    if datalength <> "0" then columnType
                    else columnType + "(" + datalength + ")"
                findDbType columnType
                |> Option.map (fun m ->
                    { Name = columnName
                      TypeMapping = m
                      IsPrimaryKey = primaryKeys.Values |> Seq.exists (fun x -> x.Table = table.Name && x.Column = [columnName])
                      IsNullable = nullable
                      TypeInfo = Some typeinfo }
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
                    match pks.Column, foreignKeyCols.TryFind name with
                    | [pk], Some(fk) ->
                         { Name = name
                           PrimaryTable = Table.CreateFullName(Sql.dbUnbox row.[1],Sql.dbUnbox row.[2])
                           PrimaryKey = pk
                           ForeignTable = Table.CreateFullName(Sql.dbUnbox row.[3],Sql.dbUnbox row.[5])
                           ForeignKey = fk } |> Some
                    | _, Some(fk) -> None
                    | _, None -> None
                | false, _ -> None
            )
        let children =
            rels |> List.map (fun x ->
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
           let maxLength = Some(int(Sql.dbUnboxWithDefault<decimal> -1M row.["DATA_LENGTH"]))

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
                | Some "REF CURSOR" -> SingleResultSet(col.Name, Sql.dataReaderToArray reader)
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
                            | Some "REF CURSOR" -> ResultSet(col.Name, readParameter p :?> ResultSet)
                            | _ -> ScalarResultSet(col.Name, readParameter p)
                        | None -> failwithf "Excepted return column %s but could not find it in the parameter set" col.Name
                    )
                Set(returnValues)
        entities

    let executeSprocCommandAsync (com:System.Data.Common.DbCommand) (inputParameters:QueryParameter[]) (retCols:QueryParameter[]) (values:obj[]) =
        async {

            let allParams, outps = executeSprocCommandCommon inputParameters retCols values
            allParams |> Array.iter (fun (_,p) -> com.Parameters.Add(p) |> ignore)

            match retCols with
            | [||] -> do! com.ExecuteNonQueryAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
                      return Unit
            | [|col|] ->
                use! reader = com.ExecuteReaderAsync() |> Async.AwaitTask
                match col.TypeMapping.ProviderTypeName with
                | Some "REF CURSOR" -> return SingleResultSet(col.Name, Sql.dataReaderToArray reader)
                | _ ->
                    match outps |> Array.tryFind (fun (_,p) -> p.ParameterName = col.Name) with
                    | Some(_,p) -> return Scalar(p.ParameterName, readParameter p)
                    | None -> return failwithf "Excepted return column %s but could not find it in the parameter set" col.Name
            | cols ->
                do! com.ExecuteNonQueryAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
                let returnValues =
                    cols
                    |> Array.map (fun col ->
                        match outps |> Array.tryFind (fun (_,p) -> p.ParameterName = col.Name) with
                        | Some(_,p) ->
                            match col.TypeMapping.ProviderTypeName with
                            | Some "REF CURSOR" -> ResultSet(col.Name, readParameter p :?> ResultSet)
                            | _ -> ScalarResultSet(col.Name, readParameter p)
                        | None -> failwithf "Excepted return column %s but could not find it in the parameter set" col.Name
                    )
                return Set(returnValues)
        }

type internal OracleProvider(resolutionPath, owner, referencedAssemblies, tableNames) =
    let mutable primaryKeyColumn : IDictionary<string,PrimaryKey> = null
    let relationshipCache = new ConcurrentDictionary<string, Relationship list * Relationship list>()
    let columnCache = new ConcurrentDictionary<string,ColumnLookup>()
    let mutable tableCache : Table list = []

    let isPrimaryKey tableName columnName = 
        match primaryKeyColumn.TryGetValue tableName with
        | true, pk when pk.Column = [columnName] -> true
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
            (entity.Table.FullName)
            (String.Join(",",columnNames))
            (String.Join(",",values |> Array.map(fun p -> p.ParameterName))))
        let cmd = provider.CreateCommand(con, sb.ToString())
        values |> Array.iter (cmd.Parameters.Add >> ignore)
        cmd

    let createUpdateCommand (provider:ISqlProvider) (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) (changedColumns: string list) =
        let (~~) (t:string) = sb.Append t |> ignore
        sb.Clear() |> ignore

        if changedColumns |> List.exists (isPrimaryKey entity.Table.Name) 
        then failwith "Error - you cannot change the primary key of an entity."

        let pk = primaryKeyColumn.[entity.Table.Name]
        let pkValues =
            match entity.GetPkColumnOption<obj> pk.Column with
            | [] -> failwith ("Error - you cannot update an entity that does not have a primary key. (" + entity.Table.FullName + ")")
            | v -> v

        let columns, parameters =
            (([],0),changedColumns)
            ||> List.fold(fun (out,i) col ->
                let name = sprintf ":param%i" i
                let p =
                    match entity.GetColumnOption<obj> col with
                    | Some v -> provider.CreateCommandParameter(QueryParameter.Create(name,i), v)
                    | None -> provider.CreateCommandParameter(QueryParameter.Create(name,i), DBNull.Value)
                (col,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray
            |> Array.unzip

        match pk.Column with
        | [] -> ()
        | ks -> 
            ~~(sprintf "UPDATE %s SET (%s) = (SELECT %s FROM DUAL) WHERE "
                (entity.Table.FullName)
                (String.Join(",", columns))
                (String.Join(",", parameters |> Array.map (fun p -> p.ParameterName))))
            ~~(String.Join(" AND ", ks |> List.mapi(fun i k -> (sprintf "\"%s\" = :pk%i" k i))))

        let cmd = provider.CreateCommand(con, sb.ToString())
        parameters |> Array.iter (cmd.Parameters.Add >> ignore)
        pkValues |> List.iteri(fun i pkValue ->
            let pkParam = provider.CreateCommandParameter(QueryParameter.Create((":pk"+i.ToString()),i), pkValue)
            cmd.Parameters.Add pkParam |> ignore
        )
        cmd

    let createDeleteCommand (provider:ISqlProvider) (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore
        let pk = primaryKeyColumn.[entity.Table.Name]
        sb.Clear() |> ignore
        let pkValues =
            match entity.GetPkColumnOption<obj> pk.Column with
            | [] -> failwith ("Error - you cannot delete an entity that does not have a primary key. (" + entity.Table.FullName + ")")
            | v -> v

        match pk.Column with
        | [] -> ()
        | ks -> 
            ~~(sprintf "DELETE FROM %s WHERE " entity.Table.FullName)
            ~~(String.Join(" AND ", ks |> List.mapi(fun i k -> (sprintf "\"%s\" = :pk%i" k i))))

        let cmd = provider.CreateCommand(con, sb.ToString())
        pkValues |> List.iteri(fun i pkValue ->
            let pkParam = provider.CreateCommandParameter(QueryParameter.Create((":pk"+i.ToString()),i), pkValue)
            cmd.Parameters.Add pkParam |> ignore
        )
        cmd

    do
        Oracle.owner <- owner
        Oracle.referencedAssemblies <- referencedAssemblies
        Oracle.resolutionPath <- resolutionPath

    interface ISqlProvider with
        member __.GetTableDescription(con,tableName) = 
            let sn = tableName.Substring(0,tableName.LastIndexOf(".")) 
            let tn = tableName.Substring(tableName.LastIndexOf(".")+1) 
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
            let sn = tableName.Substring(0,tableName.LastIndexOf(".")) 
            let tn = tableName.Substring(tableName.LastIndexOf(".")+1) 
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

        member __.CreateTypeMappings(con) =
            Sql.connect con (fun con ->
                Oracle.createTypeMappings con
                primaryKeyColumn <- (Oracle.getPrimaryKeys tableNames con))

        member __.GetTables(con,_) =
               match tableCache with
               | [] ->
                    let tables = Sql.connect con (Oracle.getTables tableNames)
                    tableCache <- tables
                    tables
                | a -> a

        member __.GetPrimaryKey(table) =
            match primaryKeyColumn.TryGetValue table.Name with
            | true, v -> match v.Column with [x] -> Some(x) | _ -> None
            | _ -> None

        member __.GetColumns(con,table) =
            match columnCache.TryGetValue table.FullName  with
            | true, cols when cols.Count > 0 -> cols
            | _ ->
                let cols = Sql.connect con (Oracle.getColumns primaryKeyColumn table)
                columnCache.GetOrAdd(table.FullName, cols)

        member __.GetRelationships(con,table) =
            relationshipCache.GetOrAdd(table.FullName, fun name ->
                    let rels = Sql.connect con (Oracle.getRelationships primaryKeyColumn table.Name)
                    rels)

        member __.GetSprocs(con) = Sql.connect con Oracle.getSprocs
        member __.GetIndividualsQueryText(table,amount) = Oracle.getIndivdualsQueryText amount table
        member __.GetIndividualQueryText(table,column) = Oracle.getIndivdualQueryText table column

        member this.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns,isDeleteScript) =
            let parameters = ResizeArray<_>()

            // NOTE: really need to assign the parameters their correct db types
            let param = ref 0
            let nextParam() =
                incr param
                sprintf ":param%i" !param

            let createParam (value:obj) =
                let paramName = nextParam()
                Oracle.createCommandParameter (QueryParameter.Create(paramName, !param)) value

            let fieldParam (value:obj) =
                let p = createParam value
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
                    | true -> fun col -> quoteWhiteSpace col
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
                    //if-then-else
                    | CaseSql(f, SqlCol(al2, col2)) -> sprintf "CASE WHEN %s THEN %s ELSE %s END" (buildf f) column (fieldNotation al2 col2)
                    | CaseSql(f, SqlConstant itm) -> sprintf "CASE WHEN %s THEN %s ELSE %s END" (buildf f) column (fieldParam itm)
                    | CaseNotSql(f, SqlConstant itm) -> sprintf "CASE WHEN %s THEN %s ELSE %s END" (buildf f) (fieldParam itm) column
                    | CaseSqlPlain(f, itm, itm2) -> sprintf "CASE WHEN %s THEN %s ELSE %s END" (buildf f) (fieldParam itm) (fieldParam itm2)
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
                                    let column = fieldNotation alias col
                                    let extractData data =
                                            match data with
                                            | Some(x) when (box x :? System.Linq.IQueryable) -> [||]
                                            | Some(x) when (box x :? obj array) ->
                                                // in and not in operators pass an array
                                                let elements = box x :?> obj array
                                                Array.init (elements.Length) (fun i -> createParam (elements.GetValue(i)))
                                            | Some(x) -> [|createParam (box x)|]
                                            | None ->    [|createParam null|]

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
                                        | FSharp.Data.Sql.NestedIn ->
                                            let innersql, innerpars = data.Value |> box :?> string * IDbDataParameter[]
                                            Array.iter parameters.Add innerpars
                                            sprintf "%s IN (%s)" column innersql
                                        | FSharp.Data.Sql.NotIn ->
                                            let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                            Array.iter parameters.Add paras
                                            sprintf "%s NOT IN (%s)" column text
                                        | FSharp.Data.Sql.NestedNotIn ->
                                            let innersql, innerpars = data.Value |> box :?> string * IDbDataParameter[]
                                            Array.iter parameters.Add innerpars
                                            sprintf "%s NOT IN (%s)" column innersql
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
                String.Join(",",
                    [|for KeyValue(k,v) in projectionColumns do
                        let cols = (getTable k).FullName
                        let k = if k <> "" then k elif baseAlias <> "" then baseAlias else baseTable.Name
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in columnCache.[cols] |> Seq.map (fun c -> c.Key) do
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

            // Create sumBy, minBy, maxBy, ... field columns
            let columns =
                let extracolumns =
                    match sqlQuery.Grouping with
                    | [] -> FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation Oracle.fieldNotationAlias sqlQuery.AggregateOp
                    | g  -> 
                        let keys = g |> List.map(fst) |> List.concat |> List.map(fun (a,c) -> fieldNotation a c)
                        let aggs = g |> List.map(snd) |> List.concat
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
                    ~~  (String.Join(" AND ", (List.zip data.ForeignKey data.PrimaryKey) |> List.map(fun (foreignKey,primaryKey) ->
                        sprintf "%s = %s "
                            (fieldNotation (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias) foreignKey)
                            (fieldNotation (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias) primaryKey)
                            ))))

            let groupByBuilder groupkeys =
                groupkeys
                |> List.iteri(fun i (alias,column) ->
                    if i > 0 then ~~ ", "
                    ~~ (fieldNotation alias column))

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) ->
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "%s %s" (fieldNotation alias column) (if not desc then " DESC NULLS LAST" else " ASC NULLS FIRST")))

            if isDeleteScript then
                ~~(sprintf "DELETE FROM %s " baseTable.FullName)
            else 
                // SELECT
                if sqlQuery.Distinct && sqlQuery.Count then ~~(sprintf "SELECT COUNT(DISTINCT %s) " (columns.Substring(0, columns.IndexOf(" as "))))
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
            if sqlQuery.Take.IsSome
            then
                let sql = sprintf "select * from (%s) where ROWNUM <= %i" (sb.ToString()) sqlQuery.Take.Value
                (sql, parameters)
            else
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
                entities.Keys
                |> Seq.iter(fun e ->
                    match e._State with
                    | Created ->
                        let cmd = createInsertCommand provider con sb e
                        Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        let id = cmd.ExecuteScalar()
                        if primaryKeyColumn.ContainsKey e.Table.Name then
                            match e.GetPkColumnOption primaryKeyColumn.[e.Table.Name].Column with
                            | [] ->  e.SetPkColumnSilent(primaryKeyColumn.[e.Table.Name].Column, id)
                            | _ -> () // if the primary key exists, do nothing
                                            // this is because non-identity columns will have been set
                                            // manually and in that case scope_identity would bring back 0 "" or whatever
                        e._State <- Unchanged
                    | Modified fields ->
                        let cmd = createUpdateCommand provider con sb e fields
                        Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        cmd.ExecuteNonQuery() |> ignore
                        e._State <- Unchanged
                    | Delete ->
                        let cmd = createDeleteCommand provider con sb e
                        Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        cmd.ExecuteNonQuery() |> ignore
                        // remove the pk to prevent this attempting to be used again
                        e.SetPkColumnOptionSilent(primaryKeyColumn.[e.Table.Name].Column, None)
                        e._State <- Deleted
                    | Deleted | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!")
                if scope<>null then scope.Complete()

            finally
                con.Close()

        member this.ProcessUpdatesAsync(con, entities, transactionOptions, timeout) =
            let sb = Text.StringBuilder()
            let provider = this :> ISqlProvider

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
                                let cmd = createInsertCommand provider con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                let! id = cmd.ExecuteScalarAsync() |> Async.AwaitTask
                                if primaryKeyColumn.ContainsKey e.Table.Name then
                                    match e.GetPkColumnOption primaryKeyColumn.[e.Table.Name].Column with
                                    | [] ->  e.SetPkColumnSilent(primaryKeyColumn.[e.Table.Name].Column, id)
                                    | _ -> () // if the primary key exists, do nothing
                                                    // this is because non-identity columns will have been set
                                                    // manually and in that case scope_identity would bring back 0 "" or whatever
                                e._State <- Unchanged
                            }
                        | Modified fields ->
                            async {
                                let cmd = createUpdateCommand provider con sb e fields :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                e._State <- Unchanged
                            }
                        | Delete ->
                            async {
                                let cmd = createDeleteCommand provider con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                // remove the pk to prevent this attempting to be used again
                                e.SetPkColumnOptionSilent(primaryKeyColumn.[e.Table.Name].Column, None)
                                e._State <- Deleted
                            }
                        | Deleted | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!"

                    do! Utilities.executeOneByOne handleEntity (entities.Keys|>Seq.toList)
                    if scope<>null then scope.Complete()

                finally
                    con.Close()
            }
