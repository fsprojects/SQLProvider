namespace FSharp.Data.Sql.Providers

open System
open System.IO
open System.Collections.Concurrent
open System.Collections.Generic
open System.Data
open FSharp.Data.Sql
open FSharp.Data.Sql.Transactions
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common
open System.Data.SqlClient

type internal SQLiteProvider(resolutionPath, contextSchemaPath, referencedAssemblies, runtimeAssembly, sqliteLibrary) as this =
    // note we intentionally do not hang onto a connection object at any time,
    // as the type provider will dicate the connection lifecycles
    let schemaCache = SchemaCache.LoadOrEmpty(contextSchemaPath)
    let myLock = new Object()

    /// This is custom getSchema operation for data libraries that doesn't support System.Data.Common GetSchema interface.
    let customGetSchema name conn =
        let dt = new DataTable(name)
        match name with
        | "DataTypes" -> 
            dt.Columns.AddRange([|"DataType",typeof<string>;"TypeName",typeof<string>;"ProviderDbType",typeof<int>|]|>Array.map(fun (x,t) -> new DataColumn(x,t)))
            let addrow(a:string,b:string,c:int) = dt.Rows.Add([|box(a);box(b);box(c);|]) |> ignore
            [   "System.Int16","smallint",10
                "System.Int32","int",11
                "System.Double","real",8
                "System.Single","single",15 
                "System.Double","float",8
                "System.Double","double",8
                "System.Decimal","money",7
                "System.Decimal","currency",7
                "System.Decimal","decimal",7
                "System.Decimal","numeric",7
                "System.Boolean","bit",3
                "System.Boolean","yesno",3
                "System.Boolean","logical",3
                "System.Boolean","bool",3
                "System.Boolean","boolean",3
                "System.Byte","tinyint",2
                "System.Int64","integer",12
                "System.Int64","counter",12
                "System.Int64","autoincrement",12
                "System.Int64","identity",12
                "System.Int64","long",12
                "System.Int64","bigint",12
                "System.Byte[]","binary",1
                "System.Byte[]","varbinary",1
                "System.Byte[]","blob",1
                "System.Byte[]","image",1
                "System.Byte[]","general",1
                "System.Byte[]","oleobject",1
                "System.String","varchar",16
                "System.String","nvarchar",16
                "System.String","memo",16
                "System.String","longtext",16
                "System.String","note",16
                "System.String","text",16
                "System.String","ntext",16
                "System.String","string",16
                "System.String","char",16
                "System.String","nchar",16
                "System.DateTime","datetime",6
                "System.DateTime","smalldate",6
                "System.DateTime","timestamp",6
                "System.DateTime","date",6
                "System.DateTime","time",6
                "System.Guid","uniqueidentifier",4
                "System.Guid","guid",4 ] |> List.iter(addrow)
            dt
        | "Tables" -> 
            dt.Columns.AddRange([|"TABLE_TYPE";"TABLE_CATALOG";"TABLE_NAME"|]|>Array.map(fun x -> new DataColumn(x)))

            let query = "SELECT type as TABLE_TYPE, 'main' as TABLE_CATALOG, name as TABLE_NAME FROM sqlite_master WHERE type='table';"

            use com = (this:>ISqlProvider).CreateCommand(conn,query)
            use reader = com.ExecuteReader()
            while reader.Read() do
                dt.Rows.Add([|box(reader.GetString(0));box(reader.GetString(1));box(reader.GetString(2));|]) |> ignore
            dt
        | "ForeignKeys" ->
            let tablequery = "SELECT name as TABLE_NAME FROM sqlite_master WHERE type='table';"
            let tables = 
                use com = (this:>ISqlProvider).CreateCommand(conn,tablequery)
                use reader = com.ExecuteReader()
                [while reader.Read() do yield reader.GetString(0)]

            dt.Columns.AddRange([|"TABLE_NAME";"FKEY_TO_CATALOG";"TABLE_CATALOG";"FKEY_TO_TABLE";"FKEY_FROM_COLUMN";"FKEY_TO_COLUMN";"CONSTRAINT_NAME"|]|>Array.map(fun x -> new DataColumn(x)))

            tables |> List.iter(fun tablename ->
                let query = sprintf "pragma foreign_key_list(%s)" tablename
                use com = (this:>ISqlProvider).CreateCommand(conn,query)
                use reader = com.ExecuteReader()
                while reader.Read() do 
                    dt.Rows.Add([|box(tablename); box("main"); box("main"); box(reader.GetString(2));box(reader.GetString(3));box(reader.GetString(4));box("fk_"+tablename+reader.GetString(0));|]) |> ignore
            )
            dt
        | _ -> failwith "Not supported. This custom getSchema will be removed when the corresponding System.Data.Common interface is supported by the connection driver. "

    // Dynamically load the SQLite assembly so we don't have a dependency on it in the project
    let assemblyNames =
        let fileStart = 
            match sqliteLibrary with
            | SQLiteLibrary.SystemDataSQLite -> "System"
            | SQLiteLibrary.MonoDataSQLite -> "Mono"
            | SQLiteLibrary.MicrosoftDataSqlite -> "Microsoft"
#if NETSTANDARD
            | SQLiteLibrary.AutoSelect -> "Microsoft"
#else
            | SQLiteLibrary.AutoSelect -> if Type.GetType ("Mono.Runtime") <> null then "Mono" else "System"
#endif
            | _ -> failwith ("Unsupported SQLiteLibrary option: " + sqliteLibrary.ToString())
        [
           fileStart + ".Data.SQLite.dll"
           fileStart + ".Data.Sqlite.dll"
        ]
    
    let lowercasedll = 
            match sqliteLibrary with
            | SQLiteLibrary.SystemDataSQLite -> false
            | SQLiteLibrary.MonoDataSQLite
            | SQLiteLibrary.MicrosoftDataSqlite -> true
#if NETSTANDARD
            | SQLiteLibrary.AutoSelect -> true
#else
            | SQLiteLibrary.AutoSelect -> if Type.GetType ("Mono.Runtime") <> null then true else false
#endif
            | _ -> failwith ("Unsupported SQLiteLibrary option: " + sqliteLibrary.ToString())

    let assembly =
        lazy Reflection.tryLoadAssemblyFrom resolutionPath (Array.append [|runtimeAssembly|] referencedAssemblies) assemblyNames

    let findType f =
#if NETSTANDARD
        let example = "Microsoft.Data.Sqlite.Core"
#else
        let example = 
            match sqliteLibrary with
            | SQLiteLibrary.MicrosoftDataSqlite -> "Microsoft.Data.Sqlite.Core"
            | _ -> "System.Data.SQLite.Core"
#endif
        match assembly.Value with
        | Choice1Of2(assembly) -> 
            let types = 
                try assembly.GetTypes() 
                with | :? System.Reflection.ReflectionTypeLoadException as e ->
                    let msgs = e.LoaderExceptions |> Seq.map(fun e -> e.GetBaseException().Message) |> Seq.distinct
                    let details = "Details: " + Environment.NewLine + String.Join(Environment.NewLine, msgs)
                    failwith (e.Message + Environment.NewLine + details)
            types |> Array.find f
        | Choice2Of2(paths, errors) ->
           let details = 
                match errors with 
                | [] -> "" 
                | x -> Environment.NewLine + "Details: " + Environment.NewLine + String.Join(Environment.NewLine, x)
           failwithf "Unable to resolve assemblies. One of %s (e.g. from Nuget package %s) must exist in the paths: %s %s %s %s"
                (String.Join(", ", assemblyNames |> List.toArray))
                example
                Environment.NewLine
                (String.Join(Environment.NewLine, paths |> Seq.filter(fun p -> not(String.IsNullOrEmpty p))))
                details
                (if Environment.Is64BitProcess then "(You are running on x64.)" else "(You are NOT running on x64.)")

    let connectionType =  lazy(findType (fun t -> t.Name = if lowercasedll then "SqliteConnection" else "SQLiteConnection"))
    let commandType =     lazy(findType (fun t -> t.Name = if lowercasedll then "SqliteCommand" else "SQLiteCommand"))
    let paramterType =    lazy(findType (fun t -> t.Name = if lowercasedll then "SqliteParameter" else "SQLiteParameter"))
    let getSchemaMethod = lazy(connectionType.Value.GetMethod("GetSchema",[|typeof<string>|]))

    let mutable typeMappings = []
    let mutable findClrType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"

    let createParam name ordinal value =
        let paramType = 
            match value with
            | null -> None
            | value -> findClrType (value.GetType().FullName)
        let queryParameter = 
            match paramType with
            | None -> QueryParameter.Create( name, ordinal )
            | Some typeMapping -> QueryParameter.Create( name, ordinal, typeMapping)
        (this:>ISqlProvider).CreateCommandParameter(queryParameter, value)

    let fieldNotationAlias(al:alias,col:SqlColumnType) =
        let aliasSprint =
            match String.IsNullOrEmpty(al) with
            | true -> sprintf "'%s'"
            | false -> sprintf "'[%s].[%s]'" al
        Utilities.genericAliasNotation aliasSprint col

    let getSchema name conn =
        if sqliteLibrary <> SQLiteLibrary.MicrosoftDataSqlite then
            getSchemaMethod.Value.Invoke(conn,[|name|]) :?> DataTable
        else
            customGetSchema name conn // GetSchema Not supported by Microsoft.Data.SQLite

    let createTypeMappings con =
        let dt = getSchema "DataTypes" con

        let mappings =
            [
                for r in dt.Rows do
                    let clrType = string r.["DataType"]
                    let sqlliteType = string r.["TypeName"]
                    let providerType = unbox<int> r.["ProviderDbType"]
                    let dbType = Enum.ToObject(typeof<DbType>, providerType) :?> DbType
                    yield { ProviderTypeName = Some sqlliteType; ClrType = clrType; DbType = dbType; ProviderType = Some providerType; }
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

    let createInsertCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore

        let cmd = (this :> ISqlProvider).CreateCommand(con,"")
        cmd.Connection <- con

        let columnNames, values =
            (([],0),entity.ColumnValues)
            ||> Seq.fold(fun (out,i) (k,v) ->
                let name = sprintf "@param%i" i
                let p = createParam name i v
                (k,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray
            |> Array.unzip

        let conflictClause =
          match entity.OnConflict with
          | Throw -> ""
          | Update -> " OR REPLACE "
          | DoNothing -> " OR IGNORE "

        sb.Clear() |> ignore
        ~~(sprintf "INSERT %s INTO %s (%s) VALUES (%s); SELECT last_insert_rowid();"
            conflictClause
            entity.Table.FullName
            (String.Join(",",columnNames))
            (String.Join(",",values |> Array.map(fun p -> p.ParameterName))))

        values |> Array.iter (cmd.Parameters.Add >> ignore)
        cmd.CommandText <- sb.ToString()
        cmd

    let createUpdateCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) (changedColumns: string list) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = (this :> ISqlProvider).CreateCommand(con,"")
        cmd.Connection <- con
        let haspk = schemaCache.PrimaryKeys.ContainsKey(entity.Table.FullName)
        let pk = if haspk then schemaCache.PrimaryKeys.[entity.Table.FullName] else []
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
                    | Some v -> createParam name i v
                    | None -> (this :> ISqlProvider).CreateCommandParameter(QueryParameter.Create(name,i),DBNull.Value)
                (col,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray

        match pk with
        | [] -> ()
        | ks -> 
            ~~(sprintf "UPDATE %s SET %s WHERE "
                entity.Table.FullName
                (String.Join(",", data |> Array.map(fun (c,p) -> sprintf "[%s] = %s" c p.ParameterName ) )))
            ~~(String.Join(" AND ", ks |> List.mapi(fun i k -> (sprintf "[%s] = @pk%i" k i))) + ";")

        data |> Array.map snd |> Array.iter (cmd.Parameters.Add >> ignore)
        pkValues |> List.iteri(fun i pkValue ->
            let p = createParam ("@pk"+i.ToString()) i pkValue
            cmd.Parameters.Add(p) |> ignore)
        cmd.CommandText <- sb.ToString()
        cmd

    let createDeleteCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = (this :> ISqlProvider).CreateCommand(con,"")
        cmd.Connection <- con
        sb.Clear() |> ignore
        let haspk = schemaCache.PrimaryKeys.ContainsKey(entity.Table.FullName)
        let pk = if haspk then schemaCache.PrimaryKeys.[entity.Table.FullName] else []
        sb.Clear() |> ignore
        let pkValues =
            match entity.GetPkColumnOption<obj> pk with
            | [] -> failwith ("Error - you cannot delete an entity that does not have a primary key. (" + entity.Table.FullName + ")")
            | v -> v
        pkValues |> List.iteri(fun i pkValue ->
            let p = createParam ("@id"+i.ToString()) i pkValue
            cmd.Parameters.Add(p) |> ignore)

        match pk with
        | [] -> ()
        | ks -> 
            ~~(sprintf "DELETE FROM %s WHERE " entity.Table.FullName)
            ~~(String.Join(" AND ", ks |> List.mapi(fun i k -> (sprintf "[%s] = @id%i" k i))) + ";")
        cmd.CommandText <- sb.ToString()
        cmd
    let pragmacheck (values:obj array) =
        let checkp p = 
            let p = p.ToString()
            if p.Contains("'") || p.Contains("\"") || p.Contains(";") then failwithf "Unsupported pragma: %s" p
            p
        match values.Length with
        | 1 -> checkp values.[0]
        | 2 -> (checkp values.[0]) + "(" + (checkp values.[1]) + ")"
        | _ -> failwith "Unsupported pragma"

    interface ISqlProvider with
        member __.GetLockObject() = myLock
        member __.GetTableDescription(con,tableName) = "" // SQLite doesn't support table descriptions/comments
        member __.GetColumnDescription(con,tableName,columnName) = "" // SQLite doesn't support column descriptions/comments
        member __.CreateConnection(connectionString) =
            //Forces relative paths to be relative to the Runtime assembly
            let basePath =
                if String.IsNullOrEmpty(resolutionPath) || resolutionPath = Path.DirectorySeparatorChar.ToString()
                then runtimeAssembly
                else resolutionPath
                |> Path.GetFullPath
             
            let connectionString = 
                connectionString // We don't want to replace /../ and we want to support general unix paths as well as current env paths.
                    .Replace(@"=." + Path.DirectorySeparatorChar.ToString(), "=" + basePath + Path.DirectorySeparatorChar.ToString())
                    .Replace(@"=./", "=" + basePath + Path.DirectorySeparatorChar.ToString())
            try
                Activator.CreateInstance(connectionType.Value,[|box connectionString|]) :?> IDbConnection
            with
            | :? System.Reflection.ReflectionTypeLoadException as ex ->
                let errorfiles = ex.LoaderExceptions |> Array.map(fun e -> e.GetBaseException().Message) |> Seq.distinct |> Seq.toArray
                let msg = ex.Message + "\r\n" + String.Join("\r\n", errorfiles) + (if Environment.Is64BitProcess then " (You are running on x64.)" else " (You are NOT running on x64.)")
                raise(new System.Reflection.TargetInvocationException(msg, ex))
            | :? System.Reflection.TargetInvocationException as ex when (ex.InnerException <> null && ex.InnerException :? DllNotFoundException) ->
                let msg = ex.GetBaseException().Message + ", Path: " + (Path.GetFullPath resolutionPath) + (if Environment.Is64BitProcess then " (You are running on x64.)" else " (You are NOT running on x64.)")
                raise(new System.Reflection.TargetInvocationException(msg, ex))
            | :? System.TypeInitializationException as te when (te.InnerException :? System.Reflection.TargetInvocationException) ->
                let ex = te.InnerException :?> System.Reflection.TargetInvocationException
                let msg = ex.GetBaseException().Message + ", Path: " + (Path.GetFullPath resolutionPath) + (if Environment.Is64BitProcess then " (You are running on x64.)" else " (You are NOT running on x64.)")
                raise(new System.Reflection.TargetInvocationException(msg, ex.InnerException)) 

        member __.CreateCommand(connection,commandText) = Activator.CreateInstance(commandType.Value,[|box commandText;box connection|]) :?> IDbCommand

        member __.CreateCommandParameter(param,value) =
            let p = Activator.CreateInstance(paramterType.Value,[|box param.Name;box value|]) :?> IDbDataParameter
            p.DbType <- param.TypeMapping.DbType
            p.Direction <- param.Direction
            Option.iter (fun l -> p.Size <- l) param.Length
            p

        member __.ExecuteSprocCommand(com, _, returnCols, values:obj array) = 
            let pars = pragmacheck values
            use pcom = (this:>ISqlProvider).CreateCommand(com.Connection, ("PRAGMA " + pars))
            match returnCols with
            | [||] -> 
                pcom.ExecuteNonQuery() |> ignore
                Unit
            | cols ->
                use reader = pcom.ExecuteReader()
                let processReturnColumn (col:QueryParameter) =
                    let result = ResultSet(col.Name, Sql.dataReaderToArray reader)
                    reader.NextResult() |> ignore
                    result
                Set(cols |> Array.map (processReturnColumn))

        member __.ExecuteSprocCommandAsync(com, inputParameters, returnCols, values:obj array) =  
            async {
                let pars = pragmacheck values
                use pcom = (this:>ISqlProvider).CreateCommand(com.Connection, ("PRAGMA " + pars)) :?> Common.DbCommand
                match returnCols with
                | [||] -> 
                    do! pcom.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                    return Unit
                | cols ->
                    use! reader = pcom.ExecuteReaderAsync() |> Async.AwaitTask
                    let processReturnColumnAsync (col:QueryParameter) =
                        async {
                            let! r = Sql.dataReaderToArrayAsync reader
                            let result = ResultSet(col.Name, r)
                            let! _ = reader.NextResultAsync() |> Async.AwaitTask
                            return result
                        }
                    let! r = cols |> Seq.toList |> Sql.evaluateOneByOne (processReturnColumnAsync)
                    return Set(r |> List.toArray)
            }

        member __.CreateTypeMappings(con) =
            if con.State <> ConnectionState.Open then con.Open()
            createTypeMappings con
            con.Close()

        member __.GetTables(con,_) =
            if con.State <> ConnectionState.Open then con.Open()
            let ret =
                [ for row in (getSchema "Tables" con).Rows do
                    let ty = string row.["TABLE_TYPE"]
                    if ty <> "SYSTEM_TABLE" then
                        let table = { Schema = string row.["TABLE_CATALOG"] ; Name = string row.["TABLE_NAME"]; Type=ty }
                        yield schemaCache.Tables.GetOrAdd(table.FullName,table)
                        ]
            con.Close()
            ret

        member __.GetPrimaryKey(table) =
            match schemaCache.PrimaryKeys.TryGetValue table.FullName with
            | true, [v] -> Some v
            | _ -> None

        member __.GetColumns(con,table) =
            match schemaCache.Columns.TryGetValue table.FullName with
            | (true,data) when data.Count > 0 -> data
            | _ ->
                if con.State <> ConnectionState.Open then con.Open()
                let query = sprintf "pragma table_info(%s)" table.Name
                use com = (this:>ISqlProvider).CreateCommand(con,query)
                use reader = com.ExecuteReader()
                let columns =
                    [ while reader.Read() do
                        let dtv = reader.GetString(2).ToLower()
                        let dt = if dtv.Contains("(") then dtv.Substring(0,dtv.IndexOf("(")) else dtv
                        let dt = dt.Trim()
                        match findDbType dt with
                        | Some(m) ->
                            let pkColumn = if reader.GetBoolean(5) then true else false
                            let col =
                                { Column.Name = reader.GetString(1);
                                  TypeMapping = m
                                  IsNullable = not <| reader.GetBoolean(3);
                                  IsPrimaryKey = pkColumn
                                  IsAutonumber = pkColumn
                                  HasDefault = not (reader.IsDBNull 4)
                                  TypeInfo = Some dtv }
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
                con.Close()
                schemaCache.Columns.AddOrUpdate(table.FullName, columns, fun x old -> match columns.Count with 0 -> old | x -> columns)

        member __.GetRelationships(con,table) =
          System.Threading.Monitor.Enter schemaCache.Relationships
          try
            match schemaCache.Relationships.TryGetValue(table.FullName) with
            | true,v -> v
            | _ ->
                // SQLite doesn't have great metadata capabilities.
                // while we can use PRGAMA FOREIGN_KEY_LIST, it will only show us
                // relationships in one direction, the only way to get all the relationships
                // is to retrieve all the relationships in the entire database.  This is not ideal for
                // huge schemas, but SQLite is not generally used for that purpose so we should be ok.
                // At least we can perform all the work for all the tables once here
                // and cache the results for successive calls.....
                if con.State <> ConnectionState.Open then con.Open()
                let relData = (getSchema "ForeignKeys" con)
                for row in relData.Rows do
                    let pTable =
                        { Schema = string row.["FKEY_TO_CATALOG"]     //I've not seen a schema column populated in SQLite so I'm using catalog instead
                          Name = string row.["FKEY_TO_TABLE"]
                          Type = ""}
                    let fTable =
                        { Schema = string row.["TABLE_CATALOG"]
                          Name = string row.["TABLE_NAME"]
                          Type = ""}

                    if not <| schemaCache.Relationships.ContainsKey pTable.FullName then schemaCache.Relationships.[pTable.FullName] <- ([],[])
                    if not <| schemaCache.Relationships.ContainsKey fTable.FullName then schemaCache.Relationships.[fTable.FullName] <- ([],[])

                    let rel = { Name = string row.["CONSTRAINT_NAME"]; PrimaryTable= pTable.FullName; PrimaryKey=string row.["FKEY_TO_COLUMN"]
                                ForeignTable=fTable.FullName; ForeignKey=string row.["FKEY_FROM_COLUMN"] }

                    let (c,p) = schemaCache.Relationships.[pTable.FullName]
                    schemaCache.Relationships.[pTable.FullName] <- (rel::c,p)
                    let (c,p) = schemaCache.Relationships.[fTable.FullName]
                    schemaCache.Relationships.[fTable.FullName] <- (c,rel::p)
                con.Close()
                match schemaCache.Relationships.TryGetValue table.FullName with
                | true,v -> v
                | _ -> [],[]
          finally
            System.Threading.Monitor.Exit schemaCache.Relationships

        member __.GetSprocs(_) = // SQLite does not support stored procedures. Let's just add a possibilirt to query a pragma value.
             let inParamType = (findDbType "text").Value
             let outParamType = (findDbType "cursor").Value
             [
                Root("Pragma", Sproc({ 
                                        Name = { ProcName = "Get"; Owner = "Main"; PackageName = String.Empty; }; 
                                        Params = fun _ -> [QueryParameter.Create("Name", 0, inParamType, ParameterDirection.Input)]; 
                                        ReturnColumns = (fun _ name -> [QueryParameter.Create("ResultSet",0,outParamType,ParameterDirection.Output)])
                }))
                Root("Pragma", Sproc({ 
                                        Name = { ProcName = "GetOf"; Owner = "Main"; PackageName = String.Empty; }; 
                                        Params = fun _ -> [QueryParameter.Create("Name", 0, inParamType, ParameterDirection.Input); QueryParameter.Create("Param", 0, inParamType, ParameterDirection.Input)]; 
                                        ReturnColumns = (fun _ name -> [QueryParameter.Create("ResultSet",0,outParamType,ParameterDirection.Output)])
                }))
             ]
        member __.GetIndividualsQueryText(table,amount) = sprintf "SELECT * FROM %s LIMIT %i;" table.FullName amount
        member __.GetIndividualQueryText(table,column) = sprintf "SELECT * FROM [%s].[%s] WHERE [%s].[%s].[%s] = @id" table.Schema table.Name table.Schema table.Name column
        member __.GetSchemaCache() = schemaCache

        member this.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns,isDeleteScript, _) =
            let parameters = ResizeArray<_>()
            // NOTE: really need to assign the parameters their correct db types
            let param = ref 0
            let nextParam() =
                incr param
                sprintf "@param%i" !param
            let fieldParam (x:obj)=
                let p = createParam (nextParam()) !param (box x)
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
                    | CastVarchar -> sprintf "CAST(%s AS TEXT)" column
                    // Date functions
                    | Date -> sprintf "DATE(%s)" column
                    | Year -> sprintf "CAST(STRFTIME('%%Y', %s) as INTEGER)" column
                    | Month -> sprintf "CAST(STRFTIME('%%m', %s) as INTEGER)" column
                    | Day -> sprintf "CAST(STRFTIME('%%d', %s) as INTEGER)" column
                    | Hour -> sprintf "CAST(STRFTIME('%%H', %s) as INTEGER)" column
                    | Minute -> sprintf "CAST(STRFTIME('%%M', %s) as INTEGER)" column
                    | Second -> sprintf "CAST(STRFTIME('%%S', %s) as INTEGER)" column
                    | AddYears(SqlConstant x) -> sprintf "DATETIME(%s, '+%s year')" column (Utilities.fieldConstant x)
                    | AddMonths x -> sprintf "DATETIME(%s, '+%d month')" column x
                    | AddDays(SqlConstant x) -> sprintf "DATETIME(%s, '+%s day')" column (Utilities.fieldConstant x) // SQL ignores decimal part :-(
                    | AddHours x -> sprintf "DATETIME(%s, '+%f hour')" column x
                    | AddMinutes(SqlConstant x) -> sprintf "DATETIME(%s, '+%s minute')" column (Utilities.fieldConstant x)
                    | AddSeconds x -> sprintf "DATETIME(%s, '+%f second')" column x
                    | DateDiffDays(SqlCol(al2, col2)) -> sprintf "CAST(JULIANDAY(%s) - JULIANDAY(%s) as INTEGER)" column (fieldNotation al2 col2)
                    | DateDiffSecs(SqlCol(al2, col2)) -> sprintf "(JULIANDAY(%s) - JULIANDAY(%s))*24*60*60" (fieldNotation al2 col2) column
                    | DateDiffDays(SqlConstant x) -> sprintf "CAST(JULIANDAY(%s) - JULIANDAY(%s) as INTEGER)" column (fieldParam x)
                    | DateDiffSecs(SqlConstant x) -> sprintf "(JULIANDAY(%s) - JULIANDAY(%s))*24*60*60" (fieldParam x) column
                    // Math functions
                    | Truncate -> sprintf "SUBSTR(%s, 1, INSTR(%s, '.') + 1)" column column
                    | Ceil -> sprintf "CAST(%s + 0.5 AS INT)" column // Ceil not supported, this will do
                    | Floor -> sprintf "CAST(%s AS INT)" column // Floor not supported, this will do
                    | BasicMathOfColumns(o, a, c) -> sprintf "(%s %s %s)" column o (fieldNotation a c)
                    | BasicMath(o, par) when (par :? String || par :? Char) -> sprintf "(%s %s %s)" column o (fieldParam par)
                    | BasicMathLeft(o, par) when (par :? String || par :? Char) -> sprintf "(%s %s %s)" (fieldParam par) o column
                    | Greatest(SqlConstant x) -> sprintf "MAX(%s, %s)" column (fieldParam x)
                    | Greatest(SqlCol(al2, col2)) -> sprintf "MAX(%s, %s)" column (fieldNotation al2 col2)
                    | Least(SqlConstant x) -> sprintf "MIN(%s, %s)" column (fieldParam x)
                    | Least(SqlCol(al2, col2)) -> sprintf "MIN(%s, %s)" column (fieldNotation al2 col2)
                    //if-then-else
                    | CaseSql(f, SqlCol(al2, col2)) -> sprintf "CASE WHEN %s THEN %s ELSE %s END" (buildf f) column (fieldNotation al2 col2)
                    | CaseSql(f, SqlConstant itm) -> sprintf "CASE WHEN %s THEN %s ELSE %s END" (buildf f) column (fieldParam itm)
                    | CaseNotSql(f, SqlConstant itm) -> sprintf "CASE WHEN %s THEN %s ELSE %s END" (buildf f) (fieldParam itm) column
                    | CaseSqlPlain(f, itm, itm2) -> sprintf "CASE WHEN %s THEN %s ELSE %s END" (buildf f) (fieldParam itm) (fieldParam itm2)
                    | _ -> Utilities.genericFieldNotation (fieldNotation al) colSprint c
                | GroupColumn (StdDevOp key, KeyColumn _) -> sprintf "STDEV(%s)" (colSprint key)
                | GroupColumn (StdDevOp _,x) -> sprintf "STDEV(%s)" (fieldNotation al x)
                | _ -> Utilities.genericFieldNotation (fieldNotation al) colSprint c

            and filterBuilder (~~) (f:Condition list) =

                // next up is the filter expressions

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
                                                let strings = box x :?> obj array
                                                strings 
                                                |> Array.map (fun x -> createParam (nextParam()) !param x)
                                            | Some(x) -> [|createParam (nextParam()) !param (box x)|]
                                            | None ->    [|createParam (nextParam()) !param DBNull.Value|]

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

            // all tables should be aliased.
            // the LINQ infrastructure will cause this will happen by default if the query includes more than one table
            // if it does not, then we first need to create an alias for the single table
            let getTable x =
                match sqlQuery.Aliases.TryFind x with
                | Some(a) -> a
                | None -> baseTable

            let singleEntity = sqlQuery.Aliases.Count = 0

            // build the select statement, this is easy ...
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
                                    else yield sprintf "[%s].[%s] as '[%s].[%s]'" k col k col // F# makes this so easy :)
                                | OperationColumn(n,op) ->
                                    yield sprintf "%s as [%s]" (fieldNotation k op) n|])

            // Create sumBy, minBy, maxBy, ... field columns
            let columns =
                let extracolumns =
                    match sqlQuery.Grouping with
                    | [] -> FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation fieldNotationAlias sqlQuery.AggregateOp
                    | g  -> 
                        let keys = g |> List.map(fst) |> List.concat |> List.map(fun (a,c) -> (fieldNotation a c))
                        let aggs = g |> List.map(snd) |> List.concat
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
                    ~~  (sprintf "%s [%s].[%s] as [%s] on "
                            joinType destTable.Schema destTable.Name destAlias)
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
                    ~~ (sprintf "%s %s" (fieldNotation alias column) (if not desc then "DESC " else "")))

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
                ~~(sprintf "FROM %s as [%s] " baseTable.FullName bal)
                sqlQuery.CrossJoins |> Seq.iter(fun (a,t) -> ~~(sprintf ", %s as [%s] " t.FullName a))
            fromBuilder()
            // WHERE
            if sqlQuery.Filters.Length > 0 then
                // each filter is effectively the entire contents of each where clause in the LINQ  query,
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
                ~~" ORDER BY "
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
            | Some take, Some skip ->  ~~(sprintf " LIMIT %i OFFSET %i;" take skip)
            | Some take, None ->  ~~(sprintf " LIMIT %i;" take)
            | None, Some skip -> ~~(sprintf " LIMIT %i OFFSET %i;" System.UInt32.MaxValue skip)
            | None, None -> ()

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
                entities.Keys
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
                        e.SetPkColumnOptionSilent(schemaCache.PrimaryKeys.[e.Table.FullName], None)
                        e._State <- Deleted
                    | Deleted | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!")
                if scope<>null then scope.Complete()

            finally
                con.Close()

        member this.ProcessUpdatesAsync(con, entities, transactionOptions, timeout) =
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
                                use cmd = createInsertCommand con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol con.ConnectionString cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                let! id = cmd.ExecuteScalarAsync() |> Async.AwaitTask
                                CommonTasks.checkKey schemaCache.PrimaryKeys id e
                                e._State <- Unchanged
                            }
                        | Modified fields ->
                            async {
                                use cmd = createUpdateCommand con sb e fields :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol con.ConnectionString cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                e._State <- Unchanged
                            }
                        | Delete ->
                            async {
                                use cmd = createDeleteCommand con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol con.ConnectionString cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
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
