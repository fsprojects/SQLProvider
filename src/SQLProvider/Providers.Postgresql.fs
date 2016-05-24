namespace FSharp.Data.Sql.Providers

open System
open System.Collections
open System.Collections.Concurrent
open System.Collections.Generic
open System.Data
open System.Net
open System.Net.NetworkInformation
open System.Threading
open FSharp.Data.Sql
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common

module PostgreSQL =
    let mutable resolutionPath = String.Empty
    let mutable owner = "public"
    let mutable referencedAssemblies = [| |]

    let assemblyNames = [
        "Npgsql.dll"
    ]

    let assembly =
        lazy
            match Reflection.tryLoadAssemblyFrom resolutionPath referencedAssemblies assemblyNames with
            | Choice1Of2(assembly) -> assembly
            | Choice2Of2(paths) ->
                let assemblyNames = String.Join(", ", assemblyNames |> List.toArray)
                let resolutionPaths = String.Join(Environment.NewLine, paths)
                failwithf "Unable to resolve assemblies. One of %s must exist in the paths: %s %s" assemblyNames Environment.NewLine resolutionPaths

    let isLegacyVersion = lazy (assembly.Value.GetName().Version.Major < 3)
    let findType name = assembly.Value.GetTypes() |> Array.tryFind (fun t -> t.Name = name)
    let getType = findType >> Option.get

    let connectionType = lazy (getType "NpgsqlConnection")
    let commandType = lazy (getType "NpgsqlCommand")
    let parameterType = lazy (getType "NpgsqlParameter")
    let dbType = lazy (getType "NpgsqlDbType")
    let dbTypeGetter = lazy (parameterType.Value.GetProperty("NpgsqlDbType").GetGetMethod())
    let dbTypeSetter = lazy (parameterType.Value.GetProperty("NpgsqlDbType").GetSetMethod())

    let getDbType(providerType) =
        let parameterType = parameterType.Value
        let p = Activator.CreateInstance(parameterType, [| |]) :?> IDbDataParameter
        dbTypeSetter.Value.Invoke(p, [|providerType|]) |> ignore
        p.DbType

    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"

    let parseDbType (ty: Type) dbt =
        try Some(ty, Enum.Parse(dbType.Value, dbt) |> unbox<int>)
        with _ -> None

    let typemap<'t> = List.tryPick (parseDbType typeof<'t>)
    let namemap name dbTypes = findType name |> Option.bind (fun ty -> dbTypes |> List.tryPick (parseDbType ty))

    let createTypeMappings () =
        // http://www.npgsql.org/doc/2.2/
        // http://www.npgsql.org/doc/3.0/types.html
        let mappings =
            [ "abstime"                     , typemap<DateTime>                   ["Abstime"]
              "bigint"                      , typemap<int64>                      ["Bigint"]
              "bit",                    (if isLegacyVersion.Value
                                         then typemap<bool>                       ["Bit"]
                                         else typemap<BitArray>                   ["Bit"])
              "bit varying"                 , typemap<BitArray>                   ["Varbit"]
              "boolean"                     , typemap<bool>                       ["Boolean"]
              "box"                         , namemap "NpgsqlBox"                 ["Box"]
              "bytea"                       , typemap<byte[]>                     ["Bytea"]
              "\"char\""                    , typemap<char>                       ["InternalChar"; "SingleChar"]
              "character"                   , typemap<string>                     ["Char"]
              "character varying"           , typemap<string>                     ["Varchar"]
              "cid"                         , typemap<uint32>                     ["Cid"]
              "cidr"                        , namemap "NpgsqlInet"                ["Cidr"]
              "circle"                      , namemap "NpgsqlCircle"              ["Circle"]
              "citext"                      , typemap<string>                     ["Citext"]
              "date"                        , typemap<DateTime>                   ["Date"]
              "double precision"            , typemap<double>                     ["Double"]
              "geometry"                    , namemap "IGeometry"                 ["Geometry"]
              "hstore",                 (if isLegacyVersion.Value
                                         then typemap<string>                     ["Hstore"]
                                         else typemap<IDictionary<string,string>> ["Hstore"])
              "inet"                        , typemap<IPAddress>                  ["Inet"]
            //"int2vector"                  , typemap<short[]>                    ["Int2Vector"]
              "integer"                     , typemap<int32>                      ["Integer"]
              "interval"                    , typemap<TimeSpan>                   ["Interval"]
              "json"                        , typemap<string>                     ["Json"]
              "jsonb"                       , typemap<string>                     ["Jsonb"]
              "line"                        , namemap "NpgsqlLine"                ["Line"]
              "lseg"                        , namemap "NpgsqlLSeg"                ["LSeg"]
              "macaddr"                     , typemap<PhysicalAddress>            ["MacAddr"]
              "money"                       , typemap<decimal>                    ["Money"]
              "name"                        , typemap<string>                     ["Name"]
              "numeric"                     , typemap<decimal>                    ["Numeric"]
              "oid"                         , typemap<uint32>                     ["Oid"]
            //"oidvector",              (if isLegacyVersion.Value
            //                           then typemap<string>                     ["Oidvector"]
            //                           else typemap<uint32[]>                   ["Oidvector"])
              "path"                        , namemap "NpgsqlPath"                ["Path"]
            //"pg_lsn"                      , typemap<???>                        ["???"]
              "point"                       , namemap "NpgsqlPoint"               ["Point"]
              "polygon"                     , namemap "NpgsqlPolygon"             ["Polygon"]
              "real"                        , typemap<single>                     ["Real"]
              "record"                      , typemap<SqlEntity[]>                ["Refcursor"]
              "refcursor"                   , typemap<SqlEntity[]>                ["Refcursor"]
              "regtype"                     , typemap<uint32>                     ["Regtype"]
              "SETOF refcursor"             , typemap<SqlEntity[]>                ["Refcursor"]
              "smallint"                    , typemap<int16>                      ["Smallint"]
              "text"                        , typemap<string>                     ["Text"]
              "tid"                         , namemap "NpgsqlTid"                 ["Tid"]
              "time without time zone"      , typemap<TimeSpan>                   ["Time"]
              "time with time zone",    (if isLegacyVersion.Value
                                         then namemap "NpgsqlTimeTZ"              ["TimeTZ"]
                                         else typemap<DateTimeOffset>             ["TimeTZ"])
              "timestamp without time zone" , typemap<DateTime>                   ["Timestamp"]
              "timestamp with time zone"    , typemap<DateTime>                   ["TimestampTZ"]
              "tsquery"                     , namemap "NpgsqlTsQuery"             ["TsQuery"]
              "tsvector"                    , namemap "NpgsqlTsVector"            ["TsVector"]
            //"txid_snapshot"               , typemap<???>                        ["???"]
              "unknown"                     , typemap<obj>                        ["Unknown"]
              "uuid"                        , typemap<Guid>                       ["Uuid"]
              "xid"                         , typemap<uint32>                     ["Xid"]
              "xml"                         , typemap<string>                     ["Xml"]
            //"array"                       , typemap<Array>                      ["Array"]
            //"composite"                   , typemap<obj>                        ["Composite"]
            //"enum"                        , typemap<obj>                        ["Enum"]
            //"range"                       , typemap<Array>                      ["Range"]
              ]
            |> List.choose (fun (name,dbt) ->
                dbt |> Option.map (fun (clrType,providerType) ->
                    { ProviderTypeName = Some(name)
                      ClrType = clrType.AssemblyQualifiedName
                      DbType = getDbType providerType
                      ProviderType = Some(providerType) }))

        let dbMappings =
            mappings
            |> List.map (fun m -> m.ProviderTypeName.Value, m)
            |> Map.ofList

        findDbType <- dbMappings.TryFind

    let createConnection connectionString =
        try
            Activator.CreateInstance(connectionType.Value,[|box connectionString|]) :?> IDbConnection
        with
        | :? System.Reflection.TargetInvocationException as ex when (ex.InnerException <> null && ex.InnerException :? DllNotFoundException) ->
            let msg = ex.InnerException.Message + " , Path: " + (System.IO.Path.GetFullPath resolutionPath)
            raise(new System.Reflection.TargetInvocationException(msg, ex))
        | :? System.Reflection.TargetInvocationException as e when (e.InnerException <> null) ->
            failwithf "Could not create the connection, most likely this means that the connectionString is wrong. See error from Npgsql to troubleshoot: %s" e.InnerException.Message

    let createCommand commandText connection =
        try
            Activator.CreateInstance(commandType.Value,[|box commandText;box connection|]) :?> IDbCommand
        with
          | :? System.Reflection.TargetInvocationException as e ->
            failwithf "Could not create the command, error from Npgsql %s" e.InnerException.Message

    let tryReadValueProperty instance =
        let typ = instance.GetType()
        let prop = typ.GetProperty("Value")
        if prop <> null
        then prop.GetGetMethod().Invoke(instance, [||]) |> Some
        else None

    let isOptionValue value =
        if value = null then false else
        let typ = value.GetType()
        typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<Option<_>>

    let createCommandParameter (param:QueryParameter) value =
        let value =
            if not (isOptionValue value) then value else
            match tryReadValueProperty value with Some(v) -> v | None -> null
        let p = Activator.CreateInstance(parameterType.Value, [||]) :?> IDbDataParameter
        p.ParameterName <- param.Name
        Option.iter (fun dbt -> dbTypeSetter.Value.Invoke(p, [| dbt |]) |> ignore) param.TypeMapping.ProviderType
        p.Value <- value
        p.Direction <- param.Direction
        Option.iter (fun l -> p.Size <- l) param.Length
        p

    let readParameter (parameter:IDbDataParameter) =
        match parameter.DbType, (dbTypeGetter.Value.Invoke(parameter, [||]) :?> int) with
        | DbType.Object, 23 ->
            match parameter.Value with
            | null -> null
            | value -> Sql.dataReaderToArray (value :?> IDataReader) |> Seq.toArray |> box
        | _ ->
            match tryReadValueProperty parameter.Value with
            | Some(obj) -> obj |> box
            | _ -> parameter.Value |> box

    let executeSprocCommand (com:IDbCommand) (inputParams:QueryParameter[]) (retCols:QueryParameter[]) (values:obj[]) =
        let inputParameters = inputParams |> Array.filter (fun p -> p.Direction = ParameterDirection.Input)

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

        Array.append outps inps
        |> Array.sortBy fst
        |> Array.iter (fun (_,p) -> com.Parameters.Add(p) |> ignore)

        let tran = com.Connection.BeginTransaction()
        let entities =
            try
                match retCols with
                | [||] -> com.ExecuteNonQuery() |> ignore; Unit
                | [|col|] ->
                    match col.TypeMapping.ProviderTypeName with
                    | Some "record" ->
                        use reader = com.ExecuteReader()
                        SingleResultSet(col.Name, Sql.dataReaderToArray reader)
                    | Some "refcursor" ->
                        if not isLegacyVersion.Value then
                            let cursorName = com.ExecuteScalar() |> unbox
                            com.CommandText <- sprintf @"FETCH ALL IN ""%s""" cursorName
                            com.CommandType <- CommandType.Text
                        use reader = com.ExecuteReader()
                        SingleResultSet(col.Name, Sql.dataReaderToArray reader)
                    | Some "SETOF refcursor" ->
                        use reader = com.ExecuteReader()
                        let results = ref [ResultSet("ReturnValue", Sql.dataReaderToArray reader)]
                        let i = ref 1
                        while reader.NextResult() do
                             results := ResultSet("ReturnValue" + (string !i), Sql.dataReaderToArray reader) :: !results
                             incr(i)
                        Set(!results)
                    | _ ->
                        match outps |> Array.tryFind (fun (_,p) -> p.ParameterName = col.Name) with
                        | Some(_,p) -> Scalar(p.ParameterName, com.ExecuteScalar())
                        | None -> failwithf "Excepted return column %s but could not find it in the parameter set" col.Name
                | cols ->
                    com.ExecuteNonQuery() |> ignore
                    let returnValues =
                        cols
                        |> Array.map (fun col ->
                            match outps |> Array.tryFind (fun (_,p) -> p.ParameterName = col.Name) with
                            | Some(_,p) ->
                                match col.TypeMapping.ProviderTypeName with
                                | Some "refcursor" -> ResultSet(col.Name, readParameter p :?> ResultSet)
                                | _ -> ScalarResultSet(col.Name, readParameter p)
                            | None -> failwithf "Excepted return column %s but could not find it in the parameter set" col.Name
                        )
                    Set(returnValues)
            finally
                tran.Commit()
        entities

    let getSprocs con =
        let query = @"SELECT  r.specific_name AS id,
                              r.routine_schema AS schema_name,
                              r.routine_name AS name,
                              r.data_type AS returntype,
                              (SELECT  STRING_AGG(x.param, E'\n')
                                 FROM  (SELECT  p.parameter_mode || ';' || p.parameter_name || ';' || p.data_type AS param
                                          FROM  information_schema.parameters p
                                         WHERE  p.specific_name = r.specific_name
                                      ORDER BY  p.ordinal_position) x) AS args
                        FROM  information_schema.routines r
                       WHERE      r.routine_schema NOT IN ('pg_catalog', 'information_schema')
                              AND r.routine_name NOT IN (SELECT  routine_name
                                                           FROM  information_schema.routines
                                                       GROUP BY  routine_name
                                                         HAVING  COUNT(routine_name) > 1)"
        Sql.executeSqlAsDataTable createCommand query con
        |> DataTable.map (fun r ->
            let name = { ProcName = Sql.dbUnbox<string> r.["name"]
                         Owner = Sql.dbUnbox<string> r.["schema_name"]
                         PackageName = String.Empty }
            let sparams =
                match Sql.dbUnbox<string> r.["args"] with
                | null -> []
                | args ->
                    args.Split('\n')
                    |> Array.mapi (fun i arg ->
                        let direction, name, typeName =
                            match arg.Split(';') with
                            | [| direction; name; typeName |] -> direction, name, typeName
                            | _ -> failwith "Invalid procedure argument description."
                        findDbType typeName
                        |> Option.map (fun m ->
                           let direction =
                               match direction.ToLower() with
                               | "in" -> ParameterDirection.Input
                               | "inout" -> ParameterDirection.InputOutput
                               | "out" -> ParameterDirection.Output
                               | _ -> failwithf "Unknown parameter direction value %s." direction
                           QueryParameter.Create(name,i,m,direction)))
                    |> Array.choose id
                    |> Array.toList
            let sparams, rcolumns =
                let rcolumns = sparams |> List.filter (fun p -> p.Direction <> ParameterDirection.Input)
                match Sql.dbUnbox<string> r.["returntype"] with
                | null -> sparams, rcolumns
                | "record" ->
                    match findDbType "record" with
                    | Some(m) ->
                        // TODO: query parameters can contain output parameters which could be used to populate provided properties to return value type.
                        let sparams = sparams |> List.filter (fun p -> p.Direction = ParameterDirection.Input)
                        sparams, [ QueryParameter.Create("ReturnValue", -1, { m with ProviderTypeName = Some("record") }, ParameterDirection.ReturnValue) ]
                    | None -> sparams, rcolumns
                | rtype ->
                    findDbType rtype
                    |> Option.map (fun m -> QueryParameter.Create("ReturnValue",0,m,ParameterDirection.ReturnValue))
                    |> Option.fold (fun acc col -> fst acc, col :: (snd acc)) (sparams, rcolumns)
            Root("Functions", Sproc({ Name = name; Params = (fun _ -> sparams); ReturnColumns = (fun _ _ -> rcolumns) })))

type internal PostgresqlProvider(resolutionPath, owner, referencedAssemblies) =
    let pkLookup = Dictionary<string,string>()
    let tableLookup = Dictionary<string,Table>()
    let columnLookup = ConcurrentDictionary<string,ColumnLookup>()
    let relationshipLookup = Dictionary<string,Relationship list * Relationship list>()

    let createInsertCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = PostgreSQL.createCommand "" con
        cmd.Connection <- con
        let pk = pkLookup.[entity.Table.FullName]
        let columnNames, values =
            (([],0),entity.ColumnValuesWithDefinition)
            ||> Seq.fold(fun (out,i) (k,v,c) ->
                let name = sprintf "@param%i" i
                let qp = match c with
                         | Some(c) -> QueryParameter.Create(name,i,c.TypeMapping)
                         | None -> QueryParameter.Create(name,i)
                let p = PostgreSQL.createCommandParameter qp v
                (k,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray
            |> Array.unzip

        sb.Clear() |> ignore
        ~~(sprintf "INSERT INTO \"%s\".\"%s\" " entity.Table.Schema entity.Table.Name)

        match columnNames with
        | [||] -> ~~(sprintf "DEFAULT VALUES")
        | _ -> ~~(sprintf "(%s) VALUES (%s)"
                    (String.Join(",",columnNames |> Array.map (fun c -> sprintf "\"%s\"" c)))
                    (String.Join(",",values |> Array.map(fun p -> p.ParameterName))))

        ~~(sprintf " RETURNING \"%s\";" pk)

        values |> Array.iter (cmd.Parameters.Add >> ignore)
        cmd.CommandText <- sb.ToString()
        cmd

    let createUpdateCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) changedColumns =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = PostgreSQL.createCommand "" con
        cmd.Connection <- con
        let pk = pkLookup.[entity.Table.FullName]
        sb.Clear() |> ignore

        if changedColumns |> List.exists ((=)pk) then failwith "Error - you cannot change the primary key of an entity."

        let pkValue =
            match entity.GetColumnOption<obj> pk with
            | Some v -> v
            | None -> failwith "Error - you cannot update an entity that does not have a primary key."

        let data =
            (([],0),changedColumns)
            ||> List.fold(fun (out,i) col ->
                let name = sprintf "@param%i" i
                let qp, v =
                    match entity.GetColumnOptionWithDefinition col with
                    | Some(v, Some(c)) -> QueryParameter.Create(name,i,c.TypeMapping), v
                    | Some(v, None) -> QueryParameter.Create(name,i), v
                    | None -> QueryParameter.Create(name,i), box DBNull.Value
                let p = PostgreSQL.createCommandParameter qp v
                (col,p)::out,i+1)
            |> fun (x,_)-> x
            |> List.rev
            |> List.toArray

        let pkParam = PostgreSQL.createCommandParameter (QueryParameter.Create("@pk",0)) pkValue

        ~~(sprintf "UPDATE \"%s\".\"%s\" SET %s WHERE %s = @pk;"
            entity.Table.Schema entity.Table.Name
            (String.Join(",", data |> Array.map(fun (c,p) -> sprintf "%s = %s" c p.ParameterName ) ))
            pk)

        data |> Array.map snd |> Array.iter (cmd.Parameters.Add >> ignore)
        cmd.Parameters.Add pkParam |> ignore
        cmd.CommandText <- sb.ToString()
        cmd

    let createDeleteCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = PostgreSQL.createCommand "" con
        cmd.Connection <- con
        sb.Clear() |> ignore
        let pk = pkLookup.[entity.Table.FullName]
        sb.Clear() |> ignore
        let pkValue =
            match entity.GetColumnOption<obj> pk with
            | Some v -> v
            | None -> failwith "Error - you cannot delete an entity that does not have a primary key."
        let p = PostgreSQL.createCommandParameter (QueryParameter.Create("@id",0)) pkValue

        cmd.Parameters.Add(p) |> ignore
        ~~(sprintf "DELETE FROM \"%s\".\"%s\" WHERE %s = @id" entity.Table.Schema entity.Table.Name pk )
        cmd.CommandText <- sb.ToString()
        cmd

    do
        PostgreSQL.resolutionPath <- resolutionPath
        PostgreSQL.referencedAssemblies <- referencedAssemblies

        if not(String.IsNullOrEmpty owner) then
            PostgreSQL.owner <- owner

    let checkKey id (e:SqlEntity) =
        if pkLookup.ContainsKey e.Table.FullName then
            match e.GetColumnOption pkLookup.[e.Table.FullName] with
            | Some(_) -> () // if the primary key exists, do nothing
                            // this is because non-identity columns will have been set
                            // manually and in that case scope_identity would bring back 0 "" or whatever
            | None ->  e.SetColumnSilent(pkLookup.[e.Table.FullName], id)

    interface ISqlProvider with
        member __.CreateConnection(connectionString) = PostgreSQL.createConnection connectionString
        member __.CreateCommand(connection,commandText) =  PostgreSQL.createCommand commandText connection
        member __.CreateCommandParameter(param, value) = PostgreSQL.createCommandParameter param value
        member __.ExecuteSprocCommand(con, param, retCols, values:obj array) = PostgreSQL.executeSprocCommand con param retCols values
        member __.CreateTypeMappings(_) = PostgreSQL.createTypeMappings()

        member __.GetTables(con,_) =
            use reader = Sql.executeSql PostgreSQL.createCommand (sprintf "SELECT  table_schema,
                                                          table_name,
                                                          table_type
                                                    FROM  information_schema.tables
                                                   WHERE  table_schema = '%s'" PostgreSQL.owner) con
            [ while reader.Read() do
                let table = { Schema = Sql.dbUnbox<string> reader.["table_schema"]
                              Name = Sql.dbUnbox<string> reader.["table_name"]
                              Type = (Sql.dbUnbox<string> reader.["table_type"]).ToLower() }
                if tableLookup.ContainsKey table.FullName = false then
                    tableLookup.Add(table.FullName, table)
                yield table ]

        member __.GetPrimaryKey(table) =
            match pkLookup.TryGetValue table.FullName with
            | true, v -> Some v
            | _ -> None

        member __.GetColumns(con,table) =
            Monitor.Enter columnLookup
            try
                match columnLookup.TryGetValue table.FullName with
                | (true,data) -> data
                | _ ->
                    let baseQuery = @"SELECT  c.column_name,
                                              c.data_type,
                                              c.character_maximum_length,
                                              c.numeric_precision,
                                              c.is_nullable,
                                              (CASE WHEN pk.column_name IS NOT NULL THEN 'PRIMARY KEY' ELSE '' END) AS keytype
                                        FROM  information_schema.columns c
                                              LEFT JOIN  (SELECT  ku.table_catalog,
                                                                  ku.table_schema,
                                                                  ku.table_name,
                                                                  ku.column_name
                                                            FROM  information_schema.table_constraints AS tc
                                                                  INNER JOIN  information_schema.key_column_usage AS ku
                                                                              ON      tc.constraint_type = 'PRIMARY KEY'
                                                                                  AND tc.constraint_name = ku.constraint_name
                                                         ) pk
                                                         ON      c.table_catalog = pk.table_catalog
                                                             AND c.table_schema = pk.table_schema
                                                             AND c.table_name = pk.table_name
                                                             AND c.column_name = pk.column_name
                                       WHERE      c.table_schema = @schema
                                              AND c.table_name = @table
                                    ORDER BY  c.table_schema,
                                              c.table_name,
                                              c.ordinal_position"
                    use command = PostgreSQL.createCommand baseQuery con
                    PostgreSQL.createCommandParameter (QueryParameter.Create("@schema", 0)) table.Schema |> command.Parameters.Add |> ignore
                    PostgreSQL.createCommandParameter (QueryParameter.Create("@table", 1)) table.Name |> command.Parameters.Add |> ignore
                    Sql.connect con (fun _ ->
                        use reader = command.ExecuteReader()
                        let columns =
                            [ while reader.Read() do
                                let dataType = Sql.dbUnbox<string> reader.["data_type"]
                                match PostgreSQL.findDbType (dataType.ToLower()) with
                                | Some m ->
                                    let col =
                                        { Column.Name = Sql.dbUnbox<string> reader.["column_name"]
                                          TypeMapping = m
                                          IsNullable = (Sql.dbUnbox<string> reader.["is_nullable"]) = "YES"
                                          IsPrimaryKey = (Sql.dbUnbox<string> reader.["keytype"]) = "PRIMARY KEY" }
                                    if col.IsPrimaryKey && pkLookup.ContainsKey table.FullName = false then
                                        pkLookup.Add(table.FullName, col.Name)
                                    yield (col.Name,col)
                                | _ -> failwithf "Could not get columns for `%s`, the type `%s` is unknown to Npgsql" table.FullName dataType ]
                            |> Map.ofList
                        columnLookup.GetOrAdd(table.FullName, columns))
            finally
                Monitor.Exit columnLookup

        member __.GetRelationships(con,table) =
            Monitor.Enter relationshipLookup
            try
                match relationshipLookup.TryGetValue(table.FullName) with
                | true,v -> v
                | _ ->
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
                    if con.State <> ConnectionState.Open then con.Open()
                    use reader = Sql.executeSql PostgreSQL.createCommand (sprintf "%s WHERE KCU2.TABLE_NAME = '%s'" baseQuery table.Name ) con
                    let children : Relationship list =
                        [ while reader.Read() do
                            yield {
                                    Name = reader.GetString(0);
                                    PrimaryTable=Table.CreateFullName(reader.GetString(9), reader.GetString(5));
                                    PrimaryKey=reader.GetString(6)
                                    ForeignTable=Table.CreateFullName(reader.GetString(8), reader.GetString(1));
                                    ForeignKey=reader.GetString(2)
                                  } ]
                    reader.Dispose()
                    use reader = Sql.executeSql PostgreSQL.createCommand (sprintf "%s WHERE KCU1.TABLE_NAME = '%s'" baseQuery table.Name ) con
                    let parents : Relationship list =
                        [ while reader.Read() do
                            yield {
                                    Name = reader.GetString(0);
                                    PrimaryTable = Table.CreateFullName(reader.GetString(9), reader.GetString(5));
                                    PrimaryKey = reader.GetString(6)
                                    ForeignTable = Table.CreateFullName(reader.GetString(8), reader.GetString(1));
                                    ForeignKey = reader.GetString(2)
                                  } ]
                    relationshipLookup.Add(table.FullName,(children,parents))
                    con.Close()
                    (children,parents)
                finally
                    Monitor.Exit relationshipLookup

        member __.GetSprocs(con) = Sql.connect con PostgreSQL.getSprocs
        member __.GetIndividualsQueryText(table,amount) = sprintf "SELECT * FROM \"%s\".\"%s\" LIMIT %i;" table.Schema table.Name amount
        member __.GetIndividualQueryText(table,column) = sprintf "SELECT * FROM \"%s\".\"%s\" WHERE \"%s\".\"%s\".\"%s\" = @id" table.Schema table.Name table.Schema table.Name  column

        member __.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns) =
            // NOTE: presently this is identical to the SQLite code (except the whitespace qualifiers),
            // however it is duplicated intentionally so that any Postgre specific
            // optimisations can be applied here.
            let sb = System.Text.StringBuilder()
            let parameters = ResizeArray<_>()
            let (~~) (t:string) = sb.Append t |> ignore

            // all tables should be aliased.
            // the LINQ infrastructure will cause this will happen by default if the query includes more than one table
            // if it does not, then we first need to create an alias for the single table
            let getTable x =
                match sqlQuery.Aliases.TryFind x with
                | Some(a) -> a
                | None -> baseTable

            let singleEntity = sqlQuery.Aliases.Count = 0
            // now we can build the sql query that has been simplified by the above expression converter
            // working on the basis that we will alias everything to make my life eaiser
            // first build  the select statment, this is easy ...
            let selectcolumns =
                if projectionColumns |> Seq.isEmpty then "1" else
                String.Join(",",
                    [|for KeyValue(k,v) in projectionColumns do
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in columnLookup.[(getTable k).FullName] |> Seq.map (fun c -> c.Key) do
                                if singleEntity then yield sprintf "\"%s\".\"%s\" as \"%s\"" k col col
                                else yield sprintf "\"%s\".\"%s\" as \"%s.%s\"" k col k col
                        else
                            for col in v do
                                if singleEntity then yield sprintf "\"%s\".\"%s\" as \"%s\"" k col col
                                else yield sprintf "\"%s\".\"%s\" as \"%s.%s\"" k col k col|]) // F# makes this so easy :)

            // Create sumBy, minBy, maxBy, ... field columns
            let columns =
                let extracolumns =
                    let fieldNotation(al:alias,col:string) =
                        match String.IsNullOrEmpty(al) with
                        | true -> sprintf "\"%s\"" col
                        | false -> sprintf "\"%s\".\"%s\"" al col
                    let fieldNotationAlias(al:alias,col:string) =
                        match String.IsNullOrEmpty(al) with
                        | true -> sprintf "\"%s\"" col
                        | false -> sprintf "\"%s.%s\"" al col
                    FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation fieldNotationAlias sqlQuery.AggregateOp
                // Currently we support only aggregate or select. selectcolumns + String.Join(",", extracolumns) when groupBy is ready
                match extracolumns with
                | [] -> selectcolumns
                | h::t -> h

            // next up is the filter expressions
            // NOTE: really need to assign the parameters their correct db types
            let param = ref 0
            let nextParam() =
                incr param
                sprintf "@param%i" !param

            let createParam (value:obj) =
                let paramName = nextParam()
                PostgreSQL.createCommandParameter (QueryParameter.Create(paramName, !param)) value

            let rec filterBuilder = function
                | [] -> ()
                | (cond::conds) ->
                    let build op preds (rest:Condition list option) =
                        ~~ "("
                        preds |> List.iteri( fun i (alias,col,operator,data) ->
                                let extractData data =
                                     match data with
                                     | Some(x) when box x :? obj array || operator = FSharp.Data.Sql.In || operator = FSharp.Data.Sql.NotIn ->
                                         // in and not in operators pass an array
                                            (box x :?> obj []) |> Array.map createParam
                                     | Some(x) ->
                                         [|createParam (box x)|]
                                     | None ->    [|createParam DBNull.Value|]

                                let prefix = if i>0 then (sprintf " %s " op) else ""
                                let paras = extractData data
                                ~~(sprintf "%s%s" prefix <|
                                    match operator with
                                    | FSharp.Data.Sql.IsNull -> (sprintf "\"%s\".\"%s\" IS NULL") alias col
                                    | FSharp.Data.Sql.NotNull -> (sprintf "\"%s\".\"%s\" IS NOT NULL") alias col
                                    | FSharp.Data.Sql.In ->
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "\"%s\".\"%s\" IN (%s)") alias col text
                                    | FSharp.Data.Sql.NotIn ->
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "\"%s\".\"%s\" NOT IN (%s)") alias col text
                                    | _ ->
                                        parameters.Add paras.[0]
                                        (sprintf "\"%s\".\"%s\" %s %s") alias col
                                         (operator.ToString()) paras.[0].ParameterName)
                        )
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

                    filterBuilder conds

            // next up is the FROM statement which includes joins ..
            let fromBuilder() =
                sqlQuery.Links
                |> List.iter(fun (fromAlias, data, destAlias)  ->
                    let joinType = if data.OuterJoin then "LEFT OUTER JOIN " else "INNER JOIN "
                    let destTable = getTable destAlias
                    ~~  (sprintf "%s \"%s\".\"%s\" as \"%s\" on \"%s\".\"%s\" = \"%s\".\"%s\" "
                            joinType destTable.Schema destTable.Name destAlias
                            (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias)
                            data.ForeignKey
                            (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias)
                            data.PrimaryKey))

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) ->
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "\"%s\".\"%s\" %s" alias column (if not desc then "DESC" else "")))

            // SELECT
            if sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s " columns)
            elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
            else  ~~(sprintf "SELECT %s " columns)

            // FROM
            ~~(sprintf "FROM \"%s\".\"%s\" as \"%s\" " baseTable.Schema baseTable.Name  baseAlias)
            fromBuilder()
            // WHERE
            if sqlQuery.Filters.Length > 0 then
                // each filter is effectively the entire contents of each where clause in the linq query,
                // of which there can be many. Simply turn them all into one big AND expression as that is the
                // only logical way to deal with them.
                let f = [And([],Some sqlQuery.Filters)]
                ~~"WHERE "
                filterBuilder f

            if sqlQuery.Ordering.Length > 0 then
                ~~"ORDER BY "
                orderByBuilder()

            match sqlQuery.Take, sqlQuery.Skip with
            | Some take, Some skip ->  ~~(sprintf " LIMIT %i OFFSET %i;" take skip)
            | Some take, None ->  ~~(sprintf " LIMIT %i;" take)
            | None, Some skip -> ~~(sprintf " LIMIT ALL OFFSET %i;" skip)
            | None, None -> ()

            let sql = sb.ToString()
            (sql,parameters)

        member this.ProcessUpdates(con, entities) =
            let sb = Text.StringBuilder()

            // ensure columns have been loaded
            entities |> Seq.map(fun e -> e.Key.Table)
                     |> Seq.distinct
                     |> Seq.iter(fun t -> (this :> ISqlProvider).GetColumns(con,t) |> ignore )

            if entities.Count = 0 then 
                ()
            else

            con.Open()

            use scope = Utilities.ensureTransaction()
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
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        let id = cmd.ExecuteScalar()
                        checkKey id e
                        e._State <- Unchanged
                    | Modified fields ->
                        let cmd = createUpdateCommand con sb e fields
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        cmd.ExecuteNonQuery() |> ignore
                        e._State <- Unchanged
                    | Deleted ->
                        let cmd = createDeleteCommand con sb e
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        cmd.ExecuteNonQuery() |> ignore
                        // remove the pk to prevent this attempting to be used again
                        e.SetColumnOptionSilent(pkLookup.[e.Table.FullName], None)
                    | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!")
                scope.Complete()

            finally
                con.Close()

        member this.ProcessUpdatesAsync(con, entities) =
            let sb = Text.StringBuilder()

            // ensure columns have been loaded
            entities |> Seq.map(fun e -> e.Key.Table)
                     |> Seq.distinct
                     |> Seq.iter(fun t -> (this :> ISqlProvider).GetColumns(con,t) |> ignore )

            if entities.Count = 0 then 
                async { () }
            else

            async {
                use scope = Utilities.ensureTransaction()
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
                                Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                let! id = cmd.ExecuteScalarAsync() |> Async.AwaitTask
                                checkKey id e
                                e._State <- Unchanged
                            }
                        | Modified fields ->
                            async {
                                let cmd = createUpdateCommand con sb e fields :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                e._State <- Unchanged
                            }
                        | Deleted ->
                            async {
                                let cmd = createDeleteCommand con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                // remove the pk to prevent this attempting to be used again
                                e.SetColumnOptionSilent(pkLookup.[e.Table.FullName], None)
                            }
                        | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!"

                    do! Utilities.executeOneByOne handleEntity (entities.Keys|>Seq.toList)
                    scope.Complete()

                finally
                    con.Close()
            }
