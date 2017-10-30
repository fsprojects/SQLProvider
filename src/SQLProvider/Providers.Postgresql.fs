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
open FSharp.Data.Sql.Transactions
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
            | Choice2Of2(paths, errors) ->
                let details = 
                    match errors with 
                    | [] -> "" 
                    | x -> Environment.NewLine + "Details: " + Environment.NewLine + String.Join(Environment.NewLine, x)
                let assemblyNames = String.Join(", ", assemblyNames |> List.toArray)
                let resolutionPaths = String.Join(Environment.NewLine, paths |> Seq.filter(fun p -> not(String.IsNullOrEmpty p)))
                failwithf "Unable to resolve assemblies. One of %s must exist in the paths: %s %s %s" 
                    assemblyNames Environment.NewLine resolutionPaths details

    let isLegacyVersion = lazy (assembly.Value.GetName().Version.Major < 3)
    let findType name = 
        let types = 
            try assembly.Value.GetTypes() 
            with | :? System.Reflection.ReflectionTypeLoadException as e ->
                let msgs = e.LoaderExceptions |> Seq.map(fun e -> e.GetBaseException().Message) |> Seq.distinct
                let details = "Details: " + Environment.NewLine + String.Join(Environment.NewLine, msgs)
                failwith (e.Message + Environment.NewLine + details)
        types |> Array.tryFind (fun t -> t.Name = name)
    let getType = findType >> Option.get

    let connectionType = lazy (getType "NpgsqlConnection")
    let commandType = lazy (getType "NpgsqlCommand")
    let parameterType = lazy (getType "NpgsqlParameter")
    let dbType = lazy (getType "NpgsqlDbType")
    let dbTypeGetter = lazy (parameterType.Value.GetProperty("NpgsqlDbType").GetGetMethod())
    let dbTypeSetter = lazy (parameterType.Value.GetProperty("NpgsqlDbType").GetSetMethod())

    let getDbType(providerType : int) =
        let parameterType = parameterType.Value
        let p = Activator.CreateInstance(parameterType, [| |]) :?> IDbDataParameter
        dbTypeSetter.Value.Invoke(p, [|providerType|]) |> ignore
        p.DbType

    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"

    let parseDbType dbTypeName =
        try Some(Enum.Parse(dbType.Value, dbTypeName) |> unbox<int>)
        with _ -> None
        
    let rec fieldNotation (al:alias) (c:SqlColumnType) =
        let colSprint =
            match String.IsNullOrEmpty(al) with
            | true -> sprintf "\"%s\""
            | false -> sprintf "\"%s\".\"%s\"" al
        match c with
        // Custom database spesific overrides for canonical functions:
        | SqlColumnType.CanonicalOperation(cf,col) ->
            let column = fieldNotation al col
            match cf with
            // String functions
            | Replace(SqlStr(searchItm),SqlStrCol(al2, col2)) -> sprintf "REPLACE(%s,'%s',%s)" column searchItm (fieldNotation al2 col2)
            | Replace(SqlStrCol(al2, col2),SqlStr(toItm)) -> sprintf "REPLACE(%s,%s,'%s')" column (fieldNotation al2 col2) toItm
            | Replace(SqlStrCol(al2, col2),SqlStrCol(al3, col3)) -> sprintf "REPLACE(%s,%s,%s)" column (fieldNotation al2 col2) (fieldNotation al3 col3)
            | Substring(SqlInt startPos) -> sprintf "SUBSTRING(%s from %i)" column startPos
            | Substring(SqlIntCol(al2, col2)) -> sprintf "SUBSTRING(%s from %s)" column (fieldNotation al2 col2)
            | SubstringWithLength(SqlInt startPos,SqlInt strLen) -> sprintf "SUBSTRING(%s from %i for %i)" column startPos strLen
            | SubstringWithLength(SqlInt startPos,SqlIntCol(al2, col2)) -> sprintf "SUBSTRING(%s from %i for %s)" column startPos (fieldNotation al2 col2)
            | SubstringWithLength(SqlIntCol(al2, col2),SqlInt strLen) -> sprintf "SUBSTRING(%s from %s for %i)" column (fieldNotation al2 col2) strLen
            | SubstringWithLength(SqlIntCol(al2, col2),SqlIntCol(al3, col3)) -> sprintf "SUBSTRING(%s from %s for %s)" column (fieldNotation al2 col2) (fieldNotation al3 col3)
            | Trim -> sprintf "TRIM(BOTH ' ' FROM %s)" column
            | Length -> sprintf "CHAR_LENGTH(%s)" column
            | IndexOf(SqlStr search) -> sprintf "STRPOS('%s',%s)" search column
            | IndexOf(SqlStrCol(al2, col2)) -> sprintf "STRPOS(%s,%s)" (fieldNotation al2 col2) column
            // Date functions
            | Date -> sprintf "DATE_TRUNC('day', %s)" column
            | Year -> sprintf "DATE_PART('year', %s)" column
            | Month -> sprintf "DATE_PART('month', %s)" column
            | Day -> sprintf "DATE_PART('day', %s)" column
            | Hour -> sprintf "DATE_PART('hour', %s)" column
            | Minute -> sprintf "DATE_PART('minute', %s)" column
            | Second -> sprintf "DATE_PART('second', %s)" column
            | AddYears(SqlInt x) -> sprintf "(%s + INTERVAL '1 year' * %d)" column x
            | AddYears(SqlIntCol(al2, col2)) -> sprintf "(%s + INTERVAL '1 year' * %s)" column (fieldNotation al2 col2)
            | AddMonths x -> sprintf "(%s + INTERVAL '1 month' * %d)" column x
            | AddDays(SqlFloat x) -> sprintf "(%s + INTERVAL '1 day' * %f)" column x // SQL ignores decimal part :-(
            | AddDays(SqlNumCol(al2, col2)) -> sprintf "(%s + INTERVAL '1 day' * %s)" column (fieldNotation al2 col2)
            | AddHours x -> sprintf "(%s + INTERVAL '1 hour' * %f)" column x
            | AddMinutes(SqlFloat x) -> sprintf "(%s + INTERVAL '1 minute' * %f)" column x
            | AddMinutes(SqlNumCol(al2, col2)) -> sprintf "(%s + INTERVAL '1 minute' * %s)" column (fieldNotation al2 col2)
            | AddSeconds x -> sprintf "(%s + INTERVAL '1 second' * %f)" column x
            // Math functions
            | Truncate -> sprintf "TRUNC(%s)" column
            | BasicMathOfColumns(o, a, c) -> sprintf "(%s %s %s)" column o (fieldNotation a c)
            | BasicMath(o, par) when (par :? String || par :? Char) -> sprintf "(%s %s '%O')" column o par
            | _ -> Utilities.genericFieldNotation (fieldNotation al) colSprint c
        | _ -> Utilities.genericFieldNotation (fieldNotation al) colSprint c
        
    let fieldNotationAlias(al:alias,col:SqlColumnType) =
        let aliasSprint =
            match String.IsNullOrEmpty(al) with
            | true -> sprintf "\"%s\""
            | false -> sprintf "\"%s.%s\"" al
        Utilities.genericAliasNotation aliasSprint col

    // store the enum value for Array; it will be combined later with the generic argument
    let arrayProviderDbType = lazy (Option.get <| parseDbType "Array")        
    
    /// Pairs a CLR type by type object with a value of Npgsql's type enumeration
    let typemap' t = List.tryPick parseDbType >> Option.map (fun dbType -> t, dbType)

    /// Pairs a CLR type by type parameter with a value of Npgsql's type enumeration
    let typemap<'t> = typemap' typeof<'t>
    
    /// Pairs a CLR type by name with a value of Npgsql's type enumeration
    let namemap name dbTypes = findType name |> Option.bind (fun ty -> typemap' ty dbTypes)

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
            //"composite"                   , typemap<obj>                        ["Composite"]
            //"enum"                        , typemap<obj>                        ["Enum"]
            //"range"                       , typemap<Array>                      ["Range"]
              ]
            |> List.choose (
                function
                | name, Some(clrType, providerType) -> 
                    Some (name, { ProviderTypeName = Some(name)
                                  ClrType = clrType.AssemblyQualifiedName
                                  DbType = getDbType providerType
                                  ProviderType = Some(providerType) })    
                | _ -> None
            )
            |> Map.ofList
        
        let resolveAlias = function
            | "int8"            -> "bigint"
            | "bool"            -> "boolean"
            | "varbit"          -> "bit varying"
            | "char"            -> "character"
            | "varchar"         -> "character varying"
            | "float8"          -> "double precision"
            | "int" | "int4"    -> "integer"
            | "float4"          -> "real"
            | "decimal"         -> "numeric"
            | "int2"            -> "smallint"
            | "timetz"          -> "time with time zone"
            | "timestamptz"     -> "timestamp with time zone"
            | x -> x                

        findDbType <- resolveAlias >> mappings.TryFind

    let createConnection connectionString =
        try
            Activator.CreateInstance(connectionType.Value,[|box connectionString|]) :?> IDbConnection
        with
        | :? System.Reflection.ReflectionTypeLoadException as ex ->
            let errorfiles = ex.LoaderExceptions |> Array.map(fun e -> e.GetBaseException().Message) |> Seq.distinct |> Seq.toArray
            let msg = ex.Message + "\r\n" + String.Join("\r\n", errorfiles)
            raise(new System.Reflection.TargetInvocationException(msg, ex))
        | :? System.Reflection.TargetInvocationException as ex when (ex.InnerException <> null && ex.InnerException :? DllNotFoundException) ->
            let msg = ex.GetBaseException().Message + " , Path: " + (System.IO.Path.GetFullPath resolutionPath)
            raise(new System.Reflection.TargetInvocationException(msg, ex))
        | :? System.Reflection.TargetInvocationException as e when (e.InnerException <> null) ->
            failwithf "Could not create the connection, most likely this means that the connectionString is wrong. See error from Npgsql to troubleshoot: %s" e.InnerException.Message
        | :? System.TypeInitializationException as te when (te.InnerException :? System.Reflection.TargetInvocationException) ->
            let ex = te.InnerException :?> System.Reflection.TargetInvocationException
            let msg = ex.GetBaseException().Message + ", Path: " + (System.IO.Path.GetFullPath resolutionPath)
            raise(new System.Reflection.TargetInvocationException(msg, ex.InnerException)) 

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
        let normalizedValue =
            if not (isOptionValue value) then (if value = null || value.GetType() = typeof<DBNull> then box DBNull.Value else value) else
            match tryReadValueProperty value with Some(v) -> v | None -> box DBNull.Value
        let p = Activator.CreateInstance(parameterType.Value, [||]) :?> IDbDataParameter
        p.ParameterName <- param.Name
        Option.iter (fun dbt -> dbTypeSetter.Value.Invoke(p, [| dbt |]) |> ignore) param.TypeMapping.ProviderType
        p.Value <- normalizedValue
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

    let executeSprocCommandCommon (inputParams:QueryParameter []) (retCols:QueryParameter[]) (values:obj[]) =
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

        let allParams =
            Array.append outps inps
            |> Array.sortBy fst

        allParams, outps

    let executeSprocCommand (com:IDbCommand) (inputParams:QueryParameter[]) (retCols:QueryParameter[]) (values:obj[]) =

        let allParams, outps = executeSprocCommandCommon inputParams retCols values
        allParams |> Array.iter (fun (_,p) -> com.Parameters.Add(p) |> ignore)

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

    let executeSprocCommandAsync (com:System.Data.Common.DbCommand) (inputParams:QueryParameter[]) (retCols:QueryParameter[]) (values:obj[]) =
        async {
            let allParams, outps = executeSprocCommandCommon inputParams retCols values
            allParams |> Array.iter (fun (_,p) -> com.Parameters.Add(p) |> ignore)

            let tran = com.Connection.BeginTransaction()
                //try
            let entities = 
                async {
                    match retCols with
                    | [||] -> do! com.ExecuteNonQueryAsync()|> Async.AwaitIAsyncResult |> Async.Ignore
                              return Unit
                    | [|col|] ->
                        match col.TypeMapping.ProviderTypeName with
                        | Some "record" ->
                            use! reader = com.ExecuteReaderAsync() |> Async.AwaitTask
                            return SingleResultSet(col.Name, Sql.dataReaderToArray reader)
                        | Some "refcursor" ->
                            if not isLegacyVersion.Value then
                                let! cur = com.ExecuteScalarAsync() |> Async.AwaitTask
                                let cursorName = cur |> unbox
                                com.CommandText <- sprintf @"FETCH ALL IN ""%s""" cursorName
                                com.CommandType <- CommandType.Text
                            use! reader = com.ExecuteReaderAsync() |> Async.AwaitTask
                            return SingleResultSet(col.Name, Sql.dataReaderToArray reader)
                        | Some "SETOF refcursor" ->
                            use! reader = com.ExecuteReaderAsync() |> Async.AwaitTask
                            let results = ref [ResultSet("ReturnValue", Sql.dataReaderToArray reader)]
                            let i = ref 1
                            while reader.NextResult() do
                                    results := ResultSet("ReturnValue" + (string !i), Sql.dataReaderToArray reader) :: !results
                                    incr(i)
                            return Set(!results)
                        | _ ->
                            match outps |> Array.tryFind (fun (_,p) -> p.ParameterName = col.Name) with
                            | Some(_,p) -> 
                                let! co = com.ExecuteScalarAsync() |> Async.AwaitTask
                                return Scalar(p.ParameterName, co)
                            | None -> return failwithf "Excepted return column %s but could not find it in the parameter set" col.Name
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
                        return Set(returnValues)
                }
            // For some reasont there was commit also on failure?
            // Should it be on Async.Catch ok result only?
            let! res = entities
            tran.Commit()
            return res
        }

    let getSprocs con =
        let query = @"
          SELECT 
             r.specific_name AS id
	          ,r.routine_schema AS schema_name
	          ,r.routine_name AS name
	          ,r.data_type AS returntype
	          ,COALESCE((
			          SELECT STRING_AGG(x.param, E'\n')
			          FROM (
				          SELECT p.parameter_mode || ';' || COALESCE(p.parameter_name, ('param' || p.ORDINAL_POSITION::TEXT)) || ';' || p.data_type AS param
				          FROM information_schema.parameters p
				          WHERE p.specific_name = r.specific_name
				          ORDER BY p.ordinal_position
				          ) x
			          ), '') AS args
          FROM information_schema.routines r
          NATURAL JOIN information_schema.routine_privileges p
          WHERE r.routine_schema NOT IN ('pg_catalog', 'information_schema')
            AND r.data_type <> 'trigger'
	          AND p.grantee = current_user
            AND p.privilege_type = 'EXECUTE'
        "
        Sql.executeSqlAsDataTable createCommand query con
        |> DataTable.map (fun r ->
            let name = { ProcName = Sql.dbUnbox<string> r.["name"]
                         Owner = Sql.dbUnbox<string> r.["schema_name"]
                         PackageName = String.Empty }
            let sparams =
                match Sql.dbUnbox<string> r.["args"] with
                | "" -> []
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
    let pkLookup = ConcurrentDictionary<string,string list>()
    let tableLookup = ConcurrentDictionary<string,Table>()
    let columnLookup = ConcurrentDictionary<string,ColumnLookup>()
    let relationshipLookup = Dictionary<string,Relationship list * Relationship list>()

    let createInsertCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = PostgreSQL.createCommand "" con
        cmd.Connection <- con
        let haspk = pkLookup.ContainsKey(entity.Table.FullName)
        let pk = if haspk then pkLookup.[entity.Table.FullName] else []
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

        match haspk, pk with
        | true, [itm] -> ~~(sprintf " RETURNING \"%s\";" itm)
        | _ -> ()

        values |> Array.iter (cmd.Parameters.Add >> ignore)
        cmd.CommandText <- sb.ToString()
        cmd

    let createUpdateCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) (changedColumns: string list) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = PostgreSQL.createCommand "" con
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

        match pk with
        | [] -> ()
        | ks -> 
            ~~(sprintf "UPDATE \"%s\".\"%s\" SET %s WHERE "
                entity.Table.Schema entity.Table.Name
                (String.Join(",", data |> Array.map(fun (c,p) -> sprintf "\"%s\" = %s" c p.ParameterName ) )))
            ~~(String.Join(" AND ", ks |> List.mapi(fun i k -> (sprintf "\"%s\" = @pk%i" k i))) + ";")

        data |> Array.map snd |> Array.iter (cmd.Parameters.Add >> ignore)
        pkValues |> List.iteri(fun i pkValue ->
            let p = PostgreSQL.createCommandParameter (QueryParameter.Create("@pk"+i.ToString(),i)) pkValue
            cmd.Parameters.Add(p) |> ignore)
        cmd.CommandText <- sb.ToString()
        cmd

    let createDeleteCommand (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore
        let cmd = PostgreSQL.createCommand "" con
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
            let p = PostgreSQL.createCommandParameter (QueryParameter.Create("@id"+i.ToString(),i)) pkValue
            cmd.Parameters.Add(p) |> ignore)

        match pk with
        | [] -> ()
        | ks -> 
            ~~(sprintf "DELETE FROM \"%s\".\"%s\" WHERE " entity.Table.Schema entity.Table.Name)
            ~~(String.Join(" AND ", ks |> List.mapi(fun i k -> (sprintf "%s = @id%i" k i))))

        cmd.CommandText <- sb.ToString()
        cmd

    do
        PostgreSQL.resolutionPath <- resolutionPath
        PostgreSQL.referencedAssemblies <- referencedAssemblies

        if not(String.IsNullOrEmpty owner) then
            PostgreSQL.owner <- owner

    interface ISqlProvider with
        member __.GetTableDescription(con,tableName) = 
            Sql.connect con (fun _ ->
                use reader = 
                    Sql.executeSql PostgreSQL.createCommand (
                        sprintf """SELECT description
                                    FROM   pg_description
                                    WHERE  objoid = '%s'::regclass AND objsubid=0;
                                """ tableName) con
                if reader.Read() then
                    let comment = Sql.dbUnbox<string> reader.["description"]
                    if comment <> null then comment else ""
                else
                "")
        member __.GetColumnDescription(con,tableName,columnName) = 
            Sql.connect con (fun _ ->
                let sn = tableName.Substring(0,tableName.LastIndexOf(".")) 
                let tn = tableName.Substring(tableName.LastIndexOf(".")+1) 
                use reader = 
                    Sql.executeSql PostgreSQL.createCommand (
                        sprintf """SELECT description
                                    FROM pg_description d 
                                    LEFT JOIN information_schema.columns c 
                                    ON c.ordinal_position = d.objsubid
                                    WHERE objoid = '%s'::regclass
                                    AND c.table_schema = '%s'
                                    AND c.table_name = '%s'
                                    AND c.column_name = '%s';
                                """ tableName sn tn columnName) con
                if reader.Read() then
                    let comment = Sql.dbUnbox<string> reader.["description"]
                    if comment <> null then comment else ""
                else
                "")
        member __.CreateConnection(connectionString) = PostgreSQL.createConnection connectionString
        member __.CreateCommand(connection,commandText) =  PostgreSQL.createCommand commandText connection
        member __.CreateCommandParameter(param, value) = PostgreSQL.createCommandParameter param value
        member __.ExecuteSprocCommand(con, param, retCols, values:obj array) = PostgreSQL.executeSprocCommand con param retCols values
        member __.ExecuteSprocCommandAsync(con, param, retCols, values:obj array) = PostgreSQL.executeSprocCommandAsync con param retCols values
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
                
                yield tableLookup.GetOrAdd(table.FullName, table)
                ]

        member __.GetPrimaryKey(table) =
            match pkLookup.TryGetValue table.FullName with
            | true, [v] -> Some v
            | _ -> None

        member __.GetColumns(con,table) =
            Monitor.Enter columnLookup

            try
                match columnLookup.TryGetValue table.FullName with
                | (true,data) when data.Count > 0 -> data
                | _ ->
                    let baseQuery = @"
                        SELECT DISTINCT
                             pg_attribute.attname                                          AS column_name
                            ,rtrim(format_type(pg_attribute.atttypid, NULL), '[]')         AS base_data_type
                            ,format_type(pg_attribute.atttypid, pg_attribute.atttypmod)    AS data_type_with_sizes
                            ,pg_attribute.attndims                                         AS array_dimensions
                            ,(not pg_attribute.attnotnull)                                 AS is_nullable
                            ,coalesce(pg_index.indisprimary, false)                        AS is_primary_key
                            ,coalesce(pg_class.relkind = 'S', false)                       AS is_sequence
                        FROM pg_attribute 
                        LEFT JOIN pg_index
                            ON pg_attribute.attrelid = pg_index.indrelid
                            AND pg_attribute.attnum = ANY(pg_index.indkey)
                        LEFT JOIN pg_depend
                            ON (pg_depend.refobjid, pg_depend.refobjsubid) = (pg_attribute.attrelid, pg_attribute.attnum)
                        LEFT JOIN pg_class 
                            ON pg_depend.objid = pg_class.oid
                        LEFT JOIN pg_type
                            ON pg_class.reltype = pg_type.oid 
                        WHERE   
                                pg_attribute.attnum > 0
                            AND pg_attribute.attrelid = format('%I.%I', @schema, @table) ::regclass
                            AND NOT pg_attribute.attisdropped
                        "
                    use command = PostgreSQL.createCommand baseQuery con
                    PostgreSQL.createCommandParameter (QueryParameter.Create("@schema", 0)) table.Schema |> command.Parameters.Add |> ignore
                    PostgreSQL.createCommandParameter (QueryParameter.Create("@table", 1)) table.Name |> command.Parameters.Add |> ignore
                    Sql.connect con (fun _ ->
                        use reader = command.ExecuteReader()
                        let columns =
                            [ while reader.Read() do
                            
                                let dimensions = Sql.dbUnbox<int> reader.["array_dimensions"]  
                                let baseTypeName = Sql.dbUnbox<string> reader.["base_data_type"]
                                let fullTypeName = Sql.dbUnbox<string> reader.["data_type_with_sizes"]
                                let baseDataType = PostgreSQL.findDbType baseTypeName

                                let typeMapping = 
                                    match dimensions with   
                                    | 0 -> 
                                        // plain column
                                        baseDataType
                                    | n ->
                                        // array column: we convert the base type to a type mapping for the array
                                        baseDataType
                                        |> Option.bind (fun m -> 
                                            let pt = m.ProviderType
                                            // binary-add the array type to the bitflag
                                            match pt with
                                            | None -> None
                                            | Some t ->                                            
                                                let providerType = (t ||| PostgreSQL.arrayProviderDbType.Value)                                                
                                                Some { ProviderTypeName = Some "array"
                                                       ClrType = Type.GetType(m.ClrType).MakeArrayType(dimensions).AssemblyQualifiedName
                                                       ProviderType = Some providerType
                                                       DbType = PostgreSQL.getDbType providerType
                                                     }
                                        )
                                                                
                                match typeMapping with
                                | None ->                                                     
                                    failwithf "Could not get columns for `%s`, the type `%s` is unknown to Npgsql type mapping" table.FullName fullTypeName
                                | Some m ->

                                    let col =
                                        { Column.Name = Sql.dbUnbox<string> reader.["column_name"]
                                          TypeMapping = m
                                          IsNullable = Sql.dbUnbox<bool> reader.["is_nullable"]
                                          IsPrimaryKey = Sql.dbUnbox<bool> reader.["is_primary_key"]
                                          TypeInfo = Some fullTypeName
                                        }

                                    if col.IsPrimaryKey then
                                        pkLookup.AddOrUpdate(table.FullName, [col.Name], fun _ old -> 
                                            match col.Name with 
                                            | "" -> old 
                                            | x -> match old with
                                                   | [] -> [x]
                                                   | os -> x::os |> Seq.distinct |> Seq.toList |> List.sort
                                        ) |> ignore

                                    yield col.Name, col
                            ]
                            |> Map.ofList
                        columnLookup.AddOrUpdate(table.FullName, columns, fun x old -> match columns.Count with 0 -> old | x -> columns))
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

                    use command = PostgreSQL.createCommand (sprintf "%s WHERE KCU2.TABLE_NAME = @table" baseQuery) con
                    PostgreSQL.createCommandParameter (QueryParameter.Create("@table", 0)) table.Name |> command.Parameters.Add |> ignore
                    use reader = command.ExecuteReader()

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

                    use command = PostgreSQL.createCommand (sprintf "%s WHERE KCU1.TABLE_NAME = @table" baseQuery) con
                    PostgreSQL.createCommandParameter (QueryParameter.Create("@table", 0)) table.Name |> command.Parameters.Add |> ignore
                    use reader = command.ExecuteReader()

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

        member __.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns,isDeleteScript) =
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
                    match sqlQuery.Grouping with
                    | [] -> FSharp.Data.Sql.Common.Utilities.parseAggregates PostgreSQL.fieldNotation PostgreSQL.fieldNotationAlias sqlQuery.AggregateOp
                    | g  -> 
                        let keys = g |> List.map(fst) |> List.concat |> List.map(fun (a,c) -> PostgreSQL.fieldNotation a c)
                        let aggs = g |> List.map(snd) |> List.concat
                        let res2 = FSharp.Data.Sql.Common.Utilities.parseAggregates PostgreSQL.fieldNotation PostgreSQL.fieldNotationAlias aggs |> List.toSeq
                        [String.Join(", ", keys) + (match aggs with [] -> "" | _ -> ", ") + String.Join(", ", res2)] 
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
                                let column = PostgreSQL.fieldNotation alias col
                                let extractData data =
                                     match data with
                                     | Some(x) when (box x :? System.Linq.IQueryable) -> [||]
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
                                            let alias2f = PostgreSQL.fieldNotation alias2 col2
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
                    ~~  (sprintf "%s \"%s\".\"%s\" as \"%s\" on "
                            joinType destTable.Schema destTable.Name destAlias)
                    ~~  (String.Join(" AND ", (List.zip data.ForeignKey data.PrimaryKey) |> List.map(fun (foreignKey,primaryKey) ->
                        sprintf "%s = %s "
                            (PostgreSQL.fieldNotation (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias) foreignKey)
                            (PostgreSQL.fieldNotation (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias) primaryKey)
                            ))))

            let groupByBuilder() =
                sqlQuery.Grouping |> List.map(fst) |> List.concat
                |> List.iteri(fun i (alias,column) ->
                    if i > 0 then ~~ ", "
                    ~~ (PostgreSQL.fieldNotation alias column))

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) ->
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "%s %s" (PostgreSQL.fieldNotation alias column) (if not desc then "DESC " else "")))

            if isDeleteScript then
                ~~(sprintf "DELETE FROM \"%s\".\"%s\" " baseTable.Schema baseTable.Name)
            else 
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

            // GROUP BY
            if sqlQuery.Grouping.Length > 0 then
                ~~" GROUP BY "
                groupByBuilder()

            if sqlQuery.HavingFilters.Length > 0 then
                let keys = sqlQuery.Grouping |> List.map(fst) |> List.concat

                let f = [And([],Some (sqlQuery.HavingFilters |> CommonTasks.parseHaving PostgreSQL.fieldNotation keys))]
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
            | None, Some skip -> ~~(sprintf " LIMIT ALL OFFSET %i;" skip)
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
                        let cmd = createInsertCommand con sb e
                        Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        let id = cmd.ExecuteScalar()
                        CommonTasks.checkKey pkLookup id e
                        e._State <- Unchanged
                    | Modified fields ->
                        let cmd = createUpdateCommand con sb e fields
                        Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        cmd.ExecuteNonQuery() |> ignore
                        e._State <- Unchanged
                    | Delete ->
                        let cmd = createDeleteCommand con sb e
                        Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                        if timeout.IsSome then
                            cmd.CommandTimeout <- timeout.Value
                        cmd.ExecuteNonQuery() |> ignore
                        // remove the pk to prevent this attempting to be used again
                        e.SetPkColumnOptionSilent(pkLookup.[e.Table.FullName], None)
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
                                let cmd = createInsertCommand con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                let! id = cmd.ExecuteScalarAsync() |> Async.AwaitTask
                                CommonTasks.checkKey pkLookup id e
                                e._State <- Unchanged
                            }
                        | Modified fields ->
                            async {
                                let cmd = createUpdateCommand con sb e fields :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                e._State <- Unchanged
                            }
                        | Delete ->
                            async {
                                let cmd = createDeleteCommand con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQueryICol cmd.CommandText cmd.Parameters
                                if timeout.IsSome then
                                    cmd.CommandTimeout <- timeout.Value
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
