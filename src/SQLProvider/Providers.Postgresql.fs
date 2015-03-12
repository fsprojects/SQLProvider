namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Generic
open System.Data
open System.Reflection
open FSharp.Data.Sql
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common

module PostgreSQL = 
    
    let mutable resolutionPath = String.Empty
    let mutable owner = "public"
    let mutable referencedAssemblies = [||]

    let assemblyNames = [
        "Npgsql.dll"
    ]

    let assembly =
        lazy Reflection.tryLoadAssemblyFrom resolutionPath referencedAssemblies assemblyNames
    
    let findType name = 
        match assembly.Value with
        | Some(assembly) -> assembly.GetTypes() |> Array.find(fun t -> t.Name = name)
        | None -> failwithf "Unable to resolve postgresql assemblies. One of %s must exist in the resolution path" (String.Join(", ", assemblyNames |> List.toArray))

   
    let connectionType = lazy (findType "NpgsqlConnection")
    let commandType = lazy    (findType  "NpgsqlCommand")
    let parameterType = lazy  (findType "NpgsqlParameter")
    let dbType =  lazy (findType "NpgsqlDbType")
    let transactionType = lazy (findType "NpgsqlTransaction")
    let getSchemaMethod = lazy (connectionType.Value.GetMethod("GetSchema",[|typeof<string>|]))

    let getBeginTransactionMethod = lazy (connectionType.Value.GetMethod("BeginTransaction",[||]))

    let getEndTransactionMethod = lazy (transactionType.Value.GetMethod("Commit", [||]))

    let beginTransaction conn = getBeginTransactionMethod.Value.Invoke(conn, [||])

    let endTransaction tran = getEndTransactionMethod.Value.Invoke(tran, [||])

    let createConnection connectionString = 
        Activator.CreateInstance(connectionType.Value,[|box connectionString|]) :?> IDbConnection

    let connect (con:IDbConnection) f =
        if con.State <> ConnectionState.Open then con.Open()
        let result = f con
        con.Close(); result

    let getSchema name (args:string[]) conn = 
        getSchemaMethod.Value.Invoke(conn,[|name|]) :?> DataTable

    let mapTypeToClrType (sqlType:string) =
            match sqlType.ToLower() with
            | "bigint"
            | "int8"       -> Some typeof<Int64>
            | "bit"           // Doesn't seem to correspond to correct type - fixed-length bit string (Npgsql.BitString)
            | "varbit"        // Doesn't seem to correspond to correct type - variable-length bit string (Npgsql.BitString)
            | "boolean"
            | "bool"       -> Some typeof<Boolean>
            | "box"
            | "circle"
            | "line"
            | "lseg"
            | "path"
            | "point"
            | "polygon"    -> Some typeof<Object>
            | "bytea"      -> Some typeof<Byte[]>
            | "double"
            | "float8"     -> Some typeof<Double>
            | "integer"
            | "int"
            | "int4"       -> Some typeof<Int32>
            | "money"
            | "numeric"    -> Some typeof<Decimal>
            | "real"
            | "float4"     -> Some typeof<Single>
            | "smallint"
            | "int2"       -> Some typeof<Int16>
            | "text"       -> Some typeof<String>
            | "date"
            | "time"
            | "timetz"
            | "timestamp"
            | "timestamptz"-> Some typeof<DateTime>
            | "interval"   -> Some typeof<TimeSpan>
            | "character"
            | "varchar"    -> Some typeof<String>
            | "inet"       -> Some typeof<System.Net.IPAddress>
            | "uuid"       -> Some typeof<Guid>
            | "xml"        -> Some typeof<String>
            | _ -> None

    let getDbType(providerType) =
        try
            let parameterType = parameterType.Value
            let p = Activator.CreateInstance(parameterType,[||]) :?> IDbDataParameter
            let npgDbTypeSetter = parameterType.GetProperty("NpgsqlDbType").GetSetMethod()
            let dbTypeGetter = parameterType.GetProperty("DbType").GetGetMethod()
            npgDbTypeSetter.Invoke(p, [|providerType|]) |> ignore
            dbTypeGetter.Invoke(p, [||]) :?> DbType
        with _ -> DbType.Object //Weird cant cast Line to any DbType exception

    let mutable typeMappings = []
    let mutable findClrType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"

    let createTypeMappings() = 
        let typ = dbType.Value
        let mappings =
            [
                for v in Enum.GetValues(typ) do
                    let name = Enum.GetName(typ, v).ToLower()
                    match (mapTypeToClrType name) with
                    | Some(t) -> yield { ProviderTypeName = Some name; ClrType = t.ToString(); DbType = (getDbType v); ProviderType = Some (v :?> int);}
                    | None -> ()
                yield { ProviderTypeName = Some "character varying"; ClrType = (typeof<string>).ToString(); DbType = DbType.String; ProviderType = Some 22; }
                yield { ProviderTypeName = Some "refcursor"; ClrType = (typeof<SqlEntity[]>).ToString(); DbType = DbType.Object; ProviderType = None; }
                yield { ProviderTypeName = Some "SETOF refcursor"; ClrType = (typeof<SqlEntity[]>).ToString(); DbType = DbType.Object; ProviderType = None; }
            ]

        let adjustments = 
            [(typeof<System.DateTime>.ToString(),System.Data.DbType.Date) ] 
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

    let createCommand commandText connection = 
        Activator.CreateInstance(commandType.Value,[|box commandText;box connection|]) :?> IDbCommand

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
        let parameterType = parameterType.Value
        let dbTypeGetter = 
            parameterType.GetProperty("NpgsqlDbType").GetGetMethod()
        
        match parameter.DbType, (dbTypeGetter.Invoke(parameter, [||]) :?> int) with
        | DbType.Object, 121 ->
             if parameter.Value = null
             then null
             else
                let data = 
                    Sql.dataReaderToArray (parameter.Value :?> IDataReader) 
                    |> Seq.ofArray
                data |> box
        | _, _ ->
            match tryReadValueProperty parameter.Value with
            | Some(obj) -> obj |> box
            | _ -> parameter.Value |> box

    let executeSprocCommand (com:IDbCommand) (definition:SprocDefinition) (retCols:QueryParameter[]) (values:obj[]) = 
        let inputParameters = definition.Params |> List.filter (fun p -> p.Direction = ParameterDirection.Input)
        
        let outps =
             retCols
             |> Array.map(fun ip ->
                 let p = createCommandParameter true ip null
                 (ip.Ordinal, p))
        
        let inps =
             inputParameters
             |> List.mapi(fun i ip ->
                 let p = createCommandParameter true ip values.[i]
                 (ip.Ordinal,p))
             |> List.toArray
        
        Array.append outps inps
        |> Array.sortBy fst
        |> Array.iter (fun (_,p) -> com.Parameters.Add(p) |> ignore)

        let tran = com.Connection.BeginTransaction()
        let entities = 
            try
                match retCols with
                | [||] -> com.ExecuteNonQuery() |> ignore; Unit
                | [|col|] ->
                    use reader = com.ExecuteReader()
                    match col.TypeMapping.ProviderTypeName with
                    | Some "refcursor" -> SingleResultSet(col.Name, Sql.dataReaderToArray reader)
                    | Some "SETOF refcursor" ->
                        let results = ref [ResultSet("ReturnValue", Sql.dataReaderToArray reader)]
                        let i = ref 1
                        while reader.NextResult() do
                             results := ResultSet("ReturnValue" + (string !i), Sql.dataReaderToArray reader) :: !results
                             incr(i)
                        Set(!results)
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
                                | Some "refcursor" -> ResultSet(col.Name, readParameter p :?> ResultSet)
                                | _ -> ScalarResultSet(col.Name, readParameter p)
                            | None -> failwithf "Excepted return column %s but could not find it in the parameter set" col.Name
                        )
                    Set(returnValues)
            finally 
                tran.Commit()

        entities 

    let getSprocs con = 
        let query = 
             "select
  cast(p.oid as varchar) as id,
  current_database() as catalog_name,
  n.nspname AS schema_name,
  p.proname as name,
  pg_get_function_result(p.oid) as returntype,
  pg_get_function_arguments(p.oid) as args,
  case 
	when (p.proretset = false and t.typname != 'void') then 'FUNCTION'
	else 'PROCEDURE'
  end as routine_type
  from pg_proc p
  left join pg_namespace n
  on n.oid = p.pronamespace
  left join pg_type t
  on p.prorettype = t.oid
  where n.nspname not in ('pg_catalog','information_schema') and p.proname not in (select pg_proc.proname from pg_proc group by pg_proc.proname having count(pg_proc.proname) > 1)"
        Sql.executeSqlAsDataTable createCommand query con
        |> DataTable.map (fun r -> 
            let name = { ProcName = (Sql.dbUnbox<string> r.["name"]); Owner = (Sql.dbUnbox<string> r.["schema_name"]); PackageName = String.Empty }
            let sparams = 
                (Sql.dbUnbox<string> r.["args"]).Replace("character varying", "varchar").Split([|','|], StringSplitOptions.RemoveEmptyEntries)
                |> Array.mapi (fun i arg -> 
                    match arg.Split([|' '|], StringSplitOptions.RemoveEmptyEntries) with
                    | [|"OUT"; name; typ|] -> 
                        findDbType typ
                        |> Option.map (fun m ->
                            { Name = name; 
                              TypeMapping = m; 
                              Direction = ParameterDirection.Output; 
                              Ordinal = i;
                              Length = None 
                            }
                        )
                    | [|"INOUT"; name; typ|] -> 
                        findDbType typ
                        |> Option.map (fun m ->
                            { Name = name; 
                              TypeMapping = m; 
                              Direction = ParameterDirection.InputOutput; 
                              Ordinal = i;
                              Length = None 
                            }
                        )
                    | [|_;name;typ|] | [|name; typ|] -> 
                        findDbType typ
                        |> Option.map (fun m ->
                            { Name = name; 
                              TypeMapping = m; 
                              Direction = ParameterDirection.Input; 
                              Ordinal = i;
                              Length = None 
                            }
                        )
                    | _ -> None
                )
                |> Array.choose id |> Array.toList



            match Sql.dbUnbox<string> r.["routine_type"] with
            | "FUNCTION" -> Root("Functions", Sproc({ Name = name; Params = sparams; }))
            | "PROCEDURE" -> Root("Procedures", Sproc({ Name = name; Params = sparams; }))
            | _ -> Empty
        )

    let getSprocReturnCols con (def:SprocDefinition) = 
        let query = 
             sprintf "select
              cast(p.oid as varchar) as id,
              current_database() as catalog_name,
              n.nspname AS schema_name,
              p.proname as name,
              pg_get_function_result(p.oid) as returntype,
              pg_get_function_arguments(p.oid) as args,
              case 
            	when (p.proretset = false and t.typname != 'void') then 'FUNCTION'
            	else 'PROCEDURE'
              end as routine_type
              from pg_proc p
              left join pg_namespace n
              on n.oid = p.pronamespace
              left join pg_type t
              on p.prorettype = t.oid
              where n.nspname not in ('pg_catalog','information_schema') and p.proname not in (select pg_proc.proname from pg_proc group by pg_proc.proname having count(pg_proc.proname) > 1) and p.proname = '%s'" def.Name.ProcName
        Sql.executeSqlAsDataTable createCommand query con
        |> DataTable.map 
            (fun r -> 
                    let retCols = 
                        (Sql.dbUnbox<string> r.["returntype"]).Split([|','|], StringSplitOptions.RemoveEmptyEntries)
                        |> Array.mapi (fun i returnType ->  
                                findDbType returnType
                                |> Option.map (fun m ->
                                    { Name = if i = 0 then "ReturnValue" else "ReturnValue" + (string i) 
                                      TypeMapping = m; 
                                      Direction = ParameterDirection.ReturnValue; 
                                      Ordinal = i;
                                      Length = None 
                                    }
                                )
                        )
                        |> Array.choose id 
                        |> Array.toList
            
                    retCols @ (def.Params |> List.filter (fun p -> p.Direction <> ParameterDirection.Input)) )
       |> Seq.head          
            

type internal PostgresqlProvider(resolutionPath, owner, referencedAssemblies) as this =
    let pkLookup =     Dictionary<string,string>()
    let tableLookup =  Dictionary<string,Table>()
    let columnLookup = Dictionary<string,Column list>()    
    let relationshipLookup = Dictionary<string,Relationship list * Relationship list>()
    
    do 
        PostgreSQL.resolutionPath <- resolutionPath
        PostgreSQL.referencedAssemblies <- referencedAssemblies

        if not(String.IsNullOrEmpty owner) 
        then PostgreSQL.owner <- owner
    
    let executeSql (con:IDbConnection) sql =        
        use com = (this:>ISqlProvider).CreateCommand(con,sql)    
        com.ExecuteReader()

    interface ISqlProvider with
        member __.CreateConnection(connectionString) = PostgreSQL.createConnection connectionString
        member __.CreateCommand(connection,commandText) =  PostgreSQL.createCommand commandText connection
        member __.CreateCommandParameter(param, value) = PostgreSQL.createCommandParameter false param value
        member __.ExecuteSprocCommand(con, definition:SprocDefinition,retCols, values:obj array) = PostgreSQL.executeSprocCommand con definition retCols values
        member __.GetSprocReturnColumns(con, def) = PostgreSQL.connect con (fun con -> PostgreSQL.getSprocReturnCols con def)
        member __.CreateTypeMappings(_) = PostgreSQL.createTypeMappings()

        member __.GetTables(con) =            
            use reader = executeSql con (sprintf "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE from INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s'" PostgreSQL.owner)
            [ while reader.Read() do 
                let table ={ Schema = reader.GetString(0); Name = reader.GetString(1); Type=reader.GetString(2).ToLower() } 
                if tableLookup.ContainsKey table.FullName = false then tableLookup.Add(table.FullName,table)
                yield table ]
        member __.GetPrimaryKey(table) = 
            match pkLookup.TryGetValue table.FullName with 
            | true, v -> Some v
            | _ -> None
        member __.GetColumns(con,table) = 
            match columnLookup.TryGetValue table.FullName with
            | (true,data) -> data
            | _ -> 
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
                use com = (this:>ISqlProvider).CreateCommand(con,baseQuery)
                let p =  (this:>ISqlProvider).CreateCommandParameter(QueryParameter.Create("@schema", 0),table.Schema)
                com.Parameters.Add p |> ignore
                let p =  (this:>ISqlProvider).CreateCommandParameter(QueryParameter.Create("@table", 1),table.Name)
                com.Parameters.Add p |> ignore
                if con.State <> ConnectionState.Open then con.Open()
                use reader = com.ExecuteReader()
                let columns =
                   [ while reader.Read() do 
                       let dt = reader.GetString(1)//.ToLower().Replace("\"","")
                       // postgre gives some really weird type names here like  "double precision" and  "timestamp with time zone"
                       // this is a simple first implementation, there's also some complex types that i don't think are supported
                       // with this .net connector, but this needs examining in detail (probably by someone else!)
                     //  let dt = if dt.Contains(" ") then dt.Substring(0,dt.IndexOf(" ")).Trim() else dt
                       match PostgreSQL.findDbType (dt.ToLower()) with
                       | Some m ->
                          let col =
                             { Column.Name = reader.GetString(0)
                               TypeMapping = m
                               IsNullable = let b = reader.GetString(4) in if b = "YES" then true else false
                               IsPrimarKey = if reader.GetString(5) = "PRIMARY KEY" then true else false } 
                          if col.IsPrimarKey && pkLookup.ContainsKey table.FullName = false then pkLookup.Add(table.FullName,col.Name)
                          yield col 
                       | _ -> ()] //failwithf "Cant map type %s" dt]  
                columnLookup.Add(table.FullName,columns)
                con.Close()
                columns
        member __.GetRelationships(con,table) =
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
                use reader = executeSql con (sprintf "%s WHERE KCU2.TABLE_NAME = '%s'" baseQuery table.Name )
                let children =
                    [ while reader.Read() do 
                        yield { Name = reader.GetString(0); PrimaryTable=Table.CreateFullName(reader.GetString(9), reader.GetString(5)); PrimaryKey=reader.GetString(6)
                                ForeignTable=Table.CreateFullName(reader.GetString(8), reader.GetString(1)); ForeignKey=reader.GetString(2) } ] 
                reader.Dispose()
                use reader = executeSql con (sprintf "%s WHERE KCU1.TABLE_NAME = '%s'" baseQuery table.Name )
                let parents =
                    [ while reader.Read() do 
                        yield { Name = reader.GetString(0); PrimaryTable=Table.CreateFullName(reader.GetString(9), reader.GetString(5)); PrimaryKey=reader.GetString(6)
                                ForeignTable=Table.CreateFullName(reader.GetString(8), reader.GetString(1)); ForeignKey=reader.GetString(2) } ] 
                relationshipLookup.Add(table.FullName,(children,parents))
                con.Close()
                (children,parents)    
        
        /// Have not attempted stored procs yet
        member __.GetSprocs(con) = PostgreSQL.connect con PostgreSQL.getSprocs 

        member this.GetIndividualsQueryText(table,amount) = sprintf "SELECT * FROM %s LIMIT %i;" (table.FullName.Replace("[","\"").Replace("]","\"")) amount 

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
                                     | Some(x) when box x :? string array || operator = FSharp.Data.Sql.In || operator = FSharp.Data.Sql.NotIn -> 
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
            ~~(sprintf "FROM %s as \"%s\" " (baseTable.FullName.Replace("[","\"").Replace("]","\""))  baseAlias)         
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

            if sqlQuery.Take.IsSome then 
                ~~(sprintf " LIMIT %i;" sqlQuery.Take.Value)

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
            let createInsertCommand (entity:SqlEntity) =                 
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
                ~~(sprintf "INSERT INTO %s " (entity.Table.FullName.Replace("[","\"").Replace("]","\"")))

                match columnNames with
                | [||] -> ~~(sprintf "DEFAULT VALUES")
                | _ -> ~~(sprintf "(%s) VALUES (%s)"
                           (String.Join(",",columnNames))
                           (String.Join(",",values |> Array.map(fun p -> p.ParameterName))))

                ~~(sprintf " RETURNING %s;" pk)

                values |> Array.iter (cmd.Parameters.Add >> ignore)
                cmd.CommandText <- sb.ToString()
                cmd

            let createUpdateCommand (entity:SqlEntity) changedColumns =
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

                ~~(sprintf "UPDATE %s SET %s WHERE %s = @pk;" 
                    (entity.Table.FullName.Replace("[","\"").Replace("]","\""))
                    (String.Join(",", data |> Array.map(fun (c,p) -> sprintf "%s = %s" c p.ParameterName ) ))
                    pk)

                data |> Array.map snd |> Array.iter (cmd.Parameters.Add >> ignore)
                cmd.Parameters.Add pkParam |> ignore
                cmd.CommandText <- sb.ToString()
                cmd
            
            let createDeleteCommand (entity:SqlEntity) =
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
                ~~(sprintf "DELETE FROM %s WHERE %s = @id" (entity.Table.FullName.Replace("[","\"").Replace("]","\"")) pk )
                cmd.CommandText <- sb.ToString()
                cmd

            use scope = new Transactions.TransactionScope()
            try
                
                // close the connection first otherwise it won't get enlisted into the transaction 
                if con.State = ConnectionState.Open then con.Close()
                con.Open()          
                // initially supporting update/create/delete of single entities, no hierarchies yet
                entities
                |> List.iter(fun e -> 
                    match e._State with
                    | Created -> 
                        let cmd = createInsertCommand e
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        let id = cmd.ExecuteScalar()
                        match e.GetColumnOption pkLookup.[e.Table.FullName] with
                        | Some v -> () // if the primary key exists, do nothing
                                       // this is because non-identity columns will have been set 
                                       // manually and in that case scope_identity would bring back 0 "" or whatever
                        | None ->  e.SetColumnSilent(pkLookup.[e.Table.FullName], id)
                        e._State <- Unchanged
                    | Modified fields -> 
                        let cmd = createUpdateCommand e fields
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        cmd.ExecuteNonQuery() |> ignore
                        e._State <- Unchanged
                    | Deleted -> 
                        let cmd = createDeleteCommand e
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        cmd.ExecuteNonQuery() |> ignore
                        // remove the pk to prevent this attempting to be used again
                        e.SetColumnOptionSilent(pkLookup.[e.Table.FullName], None)
                    | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!")
                scope.Complete()
            finally
                con.Close()
