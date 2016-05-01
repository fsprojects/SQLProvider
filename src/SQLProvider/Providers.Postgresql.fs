namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Generic
open System.Data
open System.Reflection
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
        lazy Reflection.tryLoadAssemblyFrom resolutionPath referencedAssemblies assemblyNames

    let findType name = 
        match assembly.Value with
        | Choice1Of2(assembly) -> assembly.GetTypes() |> Array.find(fun t -> t.Name = name)
        | Choice2Of2(paths) -> 
           failwithf "Unable to resolve assemblies. One of %s must exist in the paths: %s %s"
                (String.Join(", ", assemblyNames |> List.toArray)) 
                Environment.NewLine
                (String.Join(Environment.NewLine, paths))
    
    
    let checkType name = 
        match assembly.Value with
        | Choice1Of2(assembly) -> assembly.GetTypes() |> Array.exists(fun t -> t.Name = name) 
        | Choice2Of2 _ -> false

    let connectionType = lazy (findType "NpgsqlConnection")
    let commandType = lazy (findType "NpgsqlCommand")
    let parameterType = lazy (findType "NpgsqlParameter")

    let dbType = lazy (findType "NpgsqlDbType")
    let dbTypeGetter = lazy (parameterType.Value.GetProperty("NpgsqlDbType").GetGetMethod())
    let dbTypeSetter = lazy (parameterType.Value.GetProperty("NpgsqlDbType").GetSetMethod())
    let getSchemaMethod = lazy (connectionType.Value.GetMethod("GetSchema",[|typeof<string>; typeof<string[]>|]))
        
    let isSupportedType = function
        | "abstime"     -> false                            // The types abstime and reltime are lower precision types which are used internally.
                                                            // You are discouraged from using these types in applications; these internal types
                                                            // might disappear in a future release.
        | "box"         -> checkType "NpgsqlBox"
        | "cidr"        -> checkType "NpgsqlInet"
        | "circle"      -> checkType "NpgsqlCircle"
        | "date"        -> checkType "NpgsqlDate"
        | "inet"        -> checkType "NpgsqlInet"
        | "interval"    -> checkType "NpgsqlTimeSpan"
        | "line"        -> checkType "NpgsqlLine"
        | "lseg"        -> checkType "NpgsqlLSeg"
        | "path"        -> checkType "NpgsqlPath"
        | "point"       -> checkType "NpgsqlPoint"
        | "polygon"     -> checkType "NpgsqlPolygon"
        | "timestamp"   -> checkType "NpgsqlDateTime"
        | "timestamptz" -> checkType "NpgsqlDateTime"
        | "tsquery"     -> checkType "NpgsqlTsQuery"
        | "tsvector"    -> checkType "NpgsqlTsVector"
        | _             -> true

    let mapNpgsqlDbTypeToClrType = function
        | "abstime"     -> Some typeof<DateTime>
        | "bigint"      -> Some typeof<int64>
        | "bit"         -> Some typeof<bool>                // Fixed-width bit array; by default bit(1) is mapped to boolean and larger type values to BitString.
        | "boolean"     -> Some typeof<bool>
        | "box"         -> Some typeof<obj>
        | "bytea"       -> Some typeof<byte[]>
        | "char"        -> Some typeof<char[]>              // Fixed-width character array.
        | "cid"         -> Some typeof<uint32>
        | "cidr"        -> Some typeof<obj>
        | "circle"      -> Some typeof<obj>
        | "date"        -> Some typeof<DateTime>
        | "double"      -> Some typeof<double>
        | "hstore"      -> Some typeof<obj>                 // Npgsql maps to IDictionary<string,string>, but provider is currently unable to support
                                                            // the type because data_type returns `USER-DEFINED` and real type name is in udt_name.
        | "inet"        -> Some typeof<obj>                 // System.Net.IPAddress
        | "integer"     -> Some typeof<int32>
        | "interval"    -> Some typeof<TimeSpan>
        | "json"        -> Some typeof<string>
        | "jsonb"       -> Some typeof<string>
        | "line"        -> Some typeof<obj>
        | "lseg"        -> Some typeof<obj>
        | "macaddr"     -> Some typeof<obj>                 // System.Net.NetworkInformation.PhysicalAddress
        | "money"       -> Some typeof<decimal>
        | "name"        -> Some typeof<string>
        | "numeric"     -> Some typeof<decimal>
        | "oid"         -> Some typeof<uint32>
        | "oidvector"   -> Some typeof<uint32[]>
        | "path"        -> Some typeof<obj>
        | "point"       -> Some typeof<obj>
        | "polygon"     -> Some typeof<obj>
        | "real"        -> Some typeof<single>
        | "refcursor"   -> Some typeof<SqlEntity[]>
        | "singlechar"  -> Some typeof<char>
        | "smallint"    -> Some typeof<int16>
        | "text"        -> Some typeof<string>
        | "time"        -> Some typeof<DateTime>
        | "timestamp"   -> Some typeof<DateTime>
        | "timestamptz" -> Some typeof<DateTime>
        | "timetz"      -> Some typeof<DateTime>
        | "tsquery"     -> Some typeof<obj>
        | "tsvector"    -> Some typeof<obj>
        | "unknown"     -> Some typeof<obj>
        | "uuid"        -> Some typeof<Guid>
        | "varbit"      -> Some typeof<obj>                 // Variable-length bit array, maps to System.Collections.BitArray
        | "varchar"     -> Some typeof<string>
        | "xid"         -> Some typeof<uint32>
        | "xml"         -> Some typeof<string>
        | "array"
        | "composite"
        | "enum"
        | "range"
        | _             -> None                             // Special enum values need separate handling

    let mapNpgsqlDbTypeToDataType = function
        | "char" -> "character"
        | "double" -> "double precision"
        | "singlechar" -> "\"char\""
        | "time" -> "time without time zone"
        | "timestamp" -> "timestamp without time zone"
        | "timestamptz" -> "timestamp with time zone"
        | "timetz" -> "time with time zone"
        | "varbit" -> "bit varying"
        | "varchar" -> "character varying"
        | name -> name

    let getDbType(providerType) =
        let parameterType = parameterType.Value
        let p = Activator.CreateInstance(parameterType, [| |]) :?> IDbDataParameter
        dbTypeSetter.Value.Invoke(p, [|providerType|]) |> ignore
        p.DbType

    let mutable typeMappings = []
    let mutable findClrType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"

    let createTypeMappings() =
        let typ = dbType.Value

        let mappings =
            let enumMappings =
                [ for v in Enum.GetValues(typ) -> Enum.GetName(typ, v).ToLower(), unbox<int> v ]
                |> List.filter (fst >> isSupportedType)
                |> List.choose (fun (name, value) ->
                    match mapNpgsqlDbTypeToClrType name with
                    | Some(t) -> Some({ ProviderTypeName = Some(mapNpgsqlDbTypeToDataType name)
                                        ClrType = t.ToString()
                                        DbType = (getDbType value)
                                        ProviderType = Some value })
                    | None -> None)
            let refCursor = enumMappings |> List.find (fun x -> x.ProviderTypeName = Some "refcursor")
            enumMappings @ [{ refCursor with ProviderTypeName = Some "SETOF refcursor" }]

        let adjustments =
            [ (typeof<DateTime>.ToString(), DbType.DateTime)
              (typeof<string>.ToString(), DbType.String) ]
            |> List.map (fun (``type``,dbType) -> ``type``,mappings |> List.find (fun mp -> mp.ClrType = ``type`` && mp.DbType = dbType))

        let clrMappings =
            mappings
            |> List.map (fun m -> m.ClrType, m)
            |> (fun tys -> List.append tys adjustments)
            |> Map.ofList

        let dbMappings =
            mappings
            |> List.map (fun m -> m.ProviderTypeName.Value, m)
            |> Map.ofList

        typeMappings <- mappings
        findClrType <- clrMappings.TryFind
        findDbType <- dbMappings.TryFind

    let getSchema name (args:string[]) (conn:IDbConnection) = 
        getSchemaMethod.Value.Invoke(conn,[|name; args|]) :?> DataTable


    let createConnection connectionString = 
        try
            Activator.CreateInstance(connectionType.Value,[|box connectionString|]) :?> IDbConnection
        with 
          | :? System.Reflection.TargetInvocationException as e ->
            failwithf "Could not create the connection, most likely this means that the connectionString is wrong. See error from Npgsql to troubleshoot: %s" e.InnerException.Message

    let createCommand commandText connection = 
        try
            Activator.CreateInstance(commandType.Value,[|box commandText;box connection|]) :?> IDbCommand
        with 
          | :? System.Reflection.TargetInvocationException as e ->
            failwithf "Could not create the command, error from Npgsql %s" e.InnerException.Message

    let createCommandParameter sprocCommand (param:QueryParameter) value =
        let mapping = if value <> null && (not sprocCommand) then (findClrType (value.GetType().ToString())) else None
        let p = Activator.CreateInstance(parameterType.Value, [||]) :?> IDbDataParameter
        p.ParameterName <- param.Name
        p.Value <- box value
        p.DbType <- (defaultArg mapping param.TypeMapping).DbType
        p.Direction <- param.Direction
        Option.iter (fun l -> p.Size <- l) param.Length
        p

    let tryReadValueProperty instance = 
        let typ = instance.GetType()
        let prop = typ.GetProperty("Value")
        if prop <> null
        then prop.GetGetMethod().Invoke(instance, [||]) |> Some
        else None   

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
                 let p = createCommandParameter true ip null
                 (ip.Ordinal, p))
        
        let inps =
             inputParameters
             |> Array.mapi(fun i ip ->
                 let p = createCommandParameter true ip values.[i]
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
                    | Some "refcursor" ->
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
                             { Name = name
                               TypeMapping = m
                               Direction =
                                 match direction.ToLower() with
                                 | "in" -> ParameterDirection.Input
                                 | "inout" -> ParameterDirection.InputOutput
                                 | "out" -> ParameterDirection.Output
                                 | _ -> failwithf "Unknown parameter direction value %s." direction
                               Ordinal = i
                               Length = None }))
                     |> Array.choose id
                     |> Array.toList
             let rcolumns =
                 let rcolumns = sparams |> List.filter (fun p -> p.Direction <> ParameterDirection.Input)
                 match Sql.dbUnbox<string> r.["returntype"] with
                 | null -> rcolumns
                 | rtype ->
                     findDbType rtype
                     |> Option.map (fun m ->
                         { Name = "ReturnValue"
                           TypeMapping = m
                           Direction = ParameterDirection.ReturnValue
                           Ordinal = 0
                           Length = None })
                     |> Option.fold (fun acc col -> col :: acc) rcolumns
             Root("Functions", Sproc({ Name = name; Params = (fun con -> sparams); ReturnColumns = (fun con _ -> rcolumns) })))

type internal PostgresqlProvider(resolutionPath, owner, referencedAssemblies) as this =
    let pkLookup = Dictionary<string,string>()
    let tableLookup = Dictionary<string,Table>()
    let columnLookup = Dictionary<string,Column list>()
    let relationshipLookup = Dictionary<string,Relationship list * Relationship list>()

    let createInsertCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =                 
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = (this :> ISqlProvider).CreateCommand(con,"")
        cmd.Connection <- con 
        let pk = pkLookup.[entity.Table.FullName] 
        let columnNames, values = 
            (([],0),entity.ColumnValues)
            ||> Seq.fold(fun (out,i) (k,v) -> 
                let name = sprintf "@param%i" i
                let p = (this :> ISqlProvider).CreateCommandParameter(QueryParameter.Create(name,i),v)
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
        let cmd = (this :> ISqlProvider).CreateCommand(con,"")
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
                let p = 
                    match entity.GetColumnOption<obj> col with
                    | Some v -> (this :> ISqlProvider).CreateCommandParameter(QueryParameter.Create(name,i),v)
                    | None -> (this :> ISqlProvider).CreateCommandParameter(QueryParameter.Create(name,i),DBNull.Value)
                (col,p)::out,i+1)
            |> fun (x,_)-> x 
            |> List.rev
            |> List.toArray 
                    
                
        let pkParam = (this :> ISqlProvider).CreateCommandParameter(QueryParameter.Create("@pk",0),pkValue)

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
        let cmd = (this :> ISqlProvider).CreateCommand(con,"")
        cmd.Connection <- con 
        sb.Clear() |> ignore
        let pk = pkLookup.[entity.Table.FullName] 
        sb.Clear() |> ignore
        let pkValue = 
            match entity.GetColumnOption<obj> pk with
            | Some v -> v
            | None -> failwith "Error - you cannot delete an entity that does not have a primary key."
        let p = (this :> ISqlProvider).CreateCommandParameter(QueryParameter.Create("@id",0),pkValue)

        cmd.Parameters.Add(p) |> ignore
        ~~(sprintf "DELETE FROM \"%s\".\"%s\" WHERE %s = @id" entity.Table.Schema entity.Table.Name pk )
        cmd.CommandText <- sb.ToString()
        cmd

    do
        PostgreSQL.resolutionPath <- resolutionPath
        PostgreSQL.referencedAssemblies <- referencedAssemblies

        if not(String.IsNullOrEmpty owner) then
            PostgreSQL.owner <- owner

    interface ISqlProvider with
        member __.CreateConnection(connectionString) = PostgreSQL.createConnection connectionString
        member __.CreateCommand(connection,commandText) =  PostgreSQL.createCommand commandText connection
        member __.CreateCommandParameter(param, value) = PostgreSQL.createCommandParameter false param value
        member __.ExecuteSprocCommand(con, param, retCols, values:obj array) = PostgreSQL.executeSprocCommand con param retCols values
        member __.CreateTypeMappings(_) = PostgreSQL.createTypeMappings()

        member __.GetTables(con,cs) =
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
                    PostgreSQL.createCommandParameter false (QueryParameter.Create("@schema", 0)) table.Schema |> command.Parameters.Add |> ignore
                    PostgreSQL.createCommandParameter false (QueryParameter.Create("@table", 1)) table.Name |> command.Parameters.Add |> ignore
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
                                          IsPrimarKey = (Sql.dbUnbox<string> reader.["keytype"]) = "PRIMARY KEY" }
                                    if col.IsPrimarKey && pkLookup.ContainsKey table.FullName = false then
                                        pkLookup.Add(table.FullName, col.Name)
                                    yield col
                                | _ -> failwithf "Could not get columns for `%s`, the type `%s` is unknown to Npgsql" table.FullName dataType ]
                        columnLookup.Add(table.FullName, columns)
                        columns)
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
        
        /// Have not attempted stored procs yet
        member __.GetSprocs(con) = Sql.connect con PostgreSQL.getSprocs 

        member this.GetIndividualsQueryText(table,amount) = sprintf "SELECT * FROM \"%s\".\"%s\" LIMIT %i;" table.Schema table.Name amount 

        member this.GetIndividualQueryText(table,column) = sprintf "SELECT * FROM \"%s\".\"%s\" WHERE \"%s\".\"%s\".\"%s\" = @id" table.Schema table.Name table.Schema table.Name  column

        member this.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns) = 
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
            let columns = 
                String.Join(",",
                    [|for KeyValue(k,v) in projectionColumns do
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in columnLookup.[(getTable k).FullName] |> List.map(fun c -> c.Name) do 
                                if singleEntity then yield sprintf "\"%s\".\"%s\" as \"%s\"" k col col
                                else yield sprintf "\"%s\".\"%s\" as \"%s.%s\"" k col k col
                        else
                            for col in v do 
                                if singleEntity then yield sprintf "\"%s\".\"%s\" as \"%s\"" k col col
                                else yield sprintf "\"%s\".\"%s\" as \"%s.%s\"" k col k col|]) // F# makes this so easy :)
        
            // next up is the filter expressions
            // NOTE: really need to assign the parameters their correct db types
            let param = ref 0
            let nextParam() =
                incr param
                sprintf "@param%i" !param

            let createParam (value:obj) =
                let paramName = nextParam()
                (this:>ISqlProvider).CreateCommandParameter(QueryParameter.Create(paramName, !param),value)

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
            let (~~) (t:string) = sb.Append t |> ignore

            // ensure columns have been loaded
            entities |> List.map(fun e -> e.Table) 
                     |> Seq.distinct 
                     |> Seq.iter(fun t -> (this :> ISqlProvider).GetColumns(con,t) |> ignore )

            con.Open()

            use scope = new Transactions.TransactionScope(Transactions.TransactionScopeAsyncFlowOption.Enabled)
            try
                
                // close the connection first otherwise it won't get enlisted into the transaction 
                if con.State = ConnectionState.Open then con.Close()
                con.Open()          
                // initially supporting update/create/delete of single entities, no hierarchies yet
                entities
                |> List.iter(fun e -> 
                    match e._State with
                    | Created -> 
                        let cmd = createInsertCommand con sb e
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        let id = cmd.ExecuteScalar()
                        match e.GetColumnOption pkLookup.[e.Table.FullName] with
                        | Some v -> () // if the primary key exists, do nothing
                                       // this is because non-identity columns will have been set 
                                       // manually and in that case scope_identity would bring back 0 "" or whatever
                        | None ->  e.SetColumnSilent(pkLookup.[e.Table.FullName], id)
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
            let (~~) (t:string) = sb.Append t |> ignore

            // ensure columns have been loaded
            entities |> List.map(fun e -> e.Table) 
                     |> Seq.distinct 
                     |> Seq.iter(fun t -> (this :> ISqlProvider).GetColumns(con,t) |> ignore )

            con.Open()

            use scope = new Transactions.TransactionScope(Transactions.TransactionScopeAsyncFlowOption.Enabled)
            try
                
                // close the connection first otherwise it won't get enlisted into the transaction 
                if con.State = ConnectionState.Open then con.Close()
                con.Open()          
                // initially supporting update/create/delete of single entities, no hierarchies yet
                let handleEntity (e: SqlEntity) =
                    match e._State with
                    | Created -> 
                        async {
                            let cmd = createInsertCommand con sb e :?> System.Data.Common.DbCommand
                            Common.QueryEvents.PublishSqlQuery cmd.CommandText
                            let! id = cmd.ExecuteScalarAsync() |> Async.AwaitTask
                            match e.GetColumnOption pkLookup.[e.Table.FullName] with
                            | Some v -> () // if the primary key exists, do nothing
                                           // this is because non-identity columns will have been set 
                                           // manually and in that case scope_identity would bring back 0 "" or whatever
                            | None ->  e.SetColumnSilent(pkLookup.[e.Table.FullName], id)
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
                async { 
                    do! Utilities.execiuteOneByOne handleEntity entities
                    scope.Complete()
                }

            finally
                con.Close()
