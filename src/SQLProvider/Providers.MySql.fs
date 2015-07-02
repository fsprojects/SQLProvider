namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Generic
open System.Data

open FSharp.Data.Sql
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
        | Some(assembly) -> assembly.GetTypes() |> Array.find(fun t -> t.Name = name)
        | None -> failwithf "Unable to resolve mysql assemblies. One of %s must exist in the resolution path: %s" (String.Join(", ", assemblyNames |> List.toArray)) resolutionPath

   
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
                    let oleDbType = string r.["TypeName"]
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
        Activator.CreateInstance(connectionType.Value,[|box connectionString|]) :?> IDbConnection

    let createCommand commandText connection = 
        Activator.CreateInstance(commandType.Value,[|box commandText;box connection|]) :?> IDbCommand

    let createCommandParameter (param:QueryParameter) value = 
        let value = if value = null then (box System.DBNull.Value) else value
        let parameterType = parameterType.Value
        let mySqlDbTypeSetter = 
            parameterType.GetProperty("MySqlDbType").GetSetMethod()
        
        let p = Activator.CreateInstance(parameterType,[|box param.Name;value|]) :?> IDbDataParameter
        
        p.Direction <-  param.Direction 
        
        p.DbType <- param.TypeMapping.DbType
        param.TypeMapping.ProviderType |> Option.iter (fun pt -> mySqlDbTypeSetter.Invoke(p, [|pt|]) |> ignore)
        
        Option.iter (fun l -> p.Size <- l) param.Length             
        p

    let connect (con:IDbConnection) f =
        if con.State <> ConnectionState.Open then con.Open()
        let result = f con
        con.Close(); result

    let executeSql sql (con:IDbConnection) =        
        Sql.executeSql createCommand sql con  

    let executeSqlAsDataTable sql con = 
        executeSql sql con
        |> (fun r -> 
             let dt = new DataTable(); 
             dt.Load(r); 
             dt)

    let getSprocReturnCols con (sparams: QueryParameter list) = 
        match sparams |> List.filter (fun p -> p.Direction <> ParameterDirection.Input) with
        | [] ->
            match findDbType "cursor" with
            | None -> []
            | Some m -> 
                [{
                    Name = "ResultSet"
                    TypeMapping = m
                    Direction = ParameterDirection.Output
                    Length = None
                    Ordinal = 0
                 }]
        | a -> a

    let getSprocs (con:IDbConnection) =
       
        let procedures,functions = 
            getSchema "Procedures" [||] con 
            |> DataTable.map (fun row -> Sql.dbUnbox<string> row.["routine_name"], Sql.dbUnbox<string> row.["routine_type"])
            |> List.partition (fun (_,t) -> t = "PROCEDURE")

        let procedures = procedures |> List.map fst |> Set.ofList
        let functions = functions |> List.map fst |> Set.ofList

        let getName (row:DataRow) = 
            let owner = Sql.dbUnboxWithDefault<string> owner row.["specific_schema"]
            let procName = (Sql.dbUnboxWithDefault<string> (Guid.NewGuid().ToString()) row.["specific_name"])
            { ProcName = procName; Owner = owner; PackageName = String.Empty; }

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

        let parameters = 
            let withParameters = 
                if String.IsNullOrEmpty owner then owner <- con.Database
                connect con (executeSqlAsDataTable (sprintf "SELECT * FROM information_schema.PARAMETERS where SPECIFIC_SCHEMA = '%s'" owner))
                |> DataTable.groupBy (fun row -> getName row, createSprocParameters row)

            (Set.union procedures functions)
            |> Set.toSeq 
            |> Seq.choose (fun proc -> 
                match withParameters |> Seq.tryFind (fun (name,_) -> name.ProcName = proc) with
                | None -> Some({ ProcName = proc; Owner = owner; PackageName = String.Empty }, Seq.empty)
                | Some (name,parameters) -> Some(name, parameters))

        parameters
        |> Seq.map (fun (name, parameters) -> 
                        let sparams = 
                            parameters
                            |> Seq.choose id
                            |> Seq.sortBy (fun p -> p.Ordinal)
                            |> Seq.toList
                        let rcolumns = getSprocReturnCols con sparams
                        let isFunction, isProcedure =  Set.contains name.ProcName functions, Set.contains name.ProcName procedures

                        match isFunction, isProcedure with
                        | true, false -> Root("Functions", Sproc({ Name = name; Params = sparams; ReturnColumns = rcolumns }))
                        | false, true ->  Root("Procedures", Sproc({ Name = name; Params = sparams; ReturnColumns = rcolumns }))
                        | _, _ -> Empty
                      ) 
        |> Seq.toList

    let readParameter (parameter:IDbDataParameter) =
        if parameter <> null 
        then 
            let par = parameter
            par.Value
        else null

    let executeSprocCommand (com:IDbCommand) (definition:SprocDefinition) (retCols:QueryParameter[]) (values:obj[]) = 
        let inputParameters = definition.Params |> List.filter (fun p -> p.Direction = ParameterDirection.Input)
        
        let outps =
             retCols
             |> Array.map(fun ip ->
                 let p = createCommandParameter ip null
                 (ip.Ordinal, p))
        
        let inps =
             inputParameters
             |> List.mapi(fun i ip ->
                 let p = createCommandParameter ip values.[i]
                 (ip.Ordinal,p))
             |> List.toArray
        
        Array.append outps inps
        |> Array.sortBy fst
        |> Array.iter (fun (_,p) -> com.Parameters.Add(p) |> ignore)

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

type internal MySqlProvider(resolutionPath, owner, referencedAssemblies) as this =
    let pkLookup =     Dictionary<string,string>()
    let tableLookup =  Dictionary<string,Table>()
    let columnLookup = Dictionary<string,Column list>()
    let relationshipLookup = Dictionary<string,Relationship list * Relationship list>()

    do
        MySql.resolutionPath <- resolutionPath
        MySql.owner <- owner
        MySql.referencedAssemblies <- referencedAssemblies

    interface ISqlProvider with
        member __.CreateConnection(connectionString) = MySql.createConnection connectionString
        member __.CreateCommand(connection,commandText) = MySql.createCommand commandText connection
        member __.CreateCommandParameter(param, value) = MySql.createCommandParameter param value
        member __.ExecuteSprocCommand(com,definition,retCols,values) = MySql.executeSprocCommand com definition retCols values
        member __.CreateTypeMappings(con) = MySql.connect con MySql.createTypeMappings

        member __.GetTables(con) =
            if String.IsNullOrEmpty MySql.owner then MySql.owner <- con.Database
            MySql.connect con (fun con ->
                use reader = MySql.executeSql (sprintf "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = '%s'" MySql.owner) con
                [ while reader.Read() do 
                    let table ={ Schema = reader.GetString(0); Name = reader.GetString(1); Type=reader.GetString(2) } 
                    if tableLookup.ContainsKey table.FullName = false then tableLookup.Add(table.FullName,table)
                    yield table ])

        member __.GetPrimaryKey(table) = 
            match pkLookup.TryGetValue table.FullName with 
            | true, v -> Some v
            | _ -> None

        member __.GetColumns(con,table) = 
            match columnLookup.TryGetValue table.FullName with
            | (true,data) -> data
            | _ -> 
               // note this data can be obtained using con.GetSchema, but with an epic schema we only want to get the data
               // we are interested in on demand
               let baseQuery = @"SELECT DISTINCTROW c.COLUMN_NAME,c.DATA_TYPE, c.character_maximum_length, c.numeric_precision, c.is_nullable
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
               com.Parameters.Add((this:>ISqlProvider).CreateCommandParameter(QueryParameter.Create("@schema", 0), table.Schema)) |> ignore
               com.Parameters.Add((this:>ISqlProvider).CreateCommandParameter(QueryParameter.Create("@table", 1), table.Name)) |> ignore
               if con.State <> ConnectionState.Open then con.Open()
               use reader = com.ExecuteReader()
               let columns =
                  [ while reader.Read() do 
                      let dt = reader.GetString(1)
                      match MySql.findDbType dt with
                      | Some(m) ->
                         let col =
                            { Column.Name = reader.GetString(0) 
                              TypeMapping = m
                              IsNullable = let b = reader.GetString(4) in if b = "YES" then true else false
                              IsPrimarKey = if reader.GetString(5) = "PRIMARY KEY" then true else false } 
                         if col.IsPrimarKey && pkLookup.ContainsKey table.FullName = false then pkLookup.Add(table.FullName,col.Name)
                         yield col 
                      | _ -> ()]  
               columnLookup.Add(table.FullName,columns)
               con.Close()
               columns
        member __.GetRelationships(con,table) = 
            match relationshipLookup.TryGetValue table.FullName with 
            | true,v -> v
            | _ -> 
            let baseQuery = @"SELECT  
                                 KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME                                 
                                ,RC.TABLE_NAME AS FK_TABLE_NAME 
                                ,KCU1.TABLE_SCHEMA AS FK_SCHEMA_NAME 
                                ,KCU1.COLUMN_NAME AS FK_COLUMN_NAME 
                                ,RC.REFERENCED_TABLE_NAME AS REFERENCED_TABLE_NAME 
                                ,KCU1.REFERENCED_TABLE_SCHEMA AS REFERENCED_SCHEMA_NAME 
                                ,KCU1.REFERENCED_COLUMN_NAME AS FK_CONSTRAINT_SCHEMA
                            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC 

                            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU1 
                                ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
                                AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
                                AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME  "

            MySql.connect con (fun con ->
            use reader = (MySql.executeSql (sprintf "%s WHERE RC.TABLE_NAME = '%s'" baseQuery table.Name ) con)
            let children =
                [ while reader.Read() do 
                    yield { Name = reader.GetString(0); PrimaryTable=Table.CreateFullName(reader.GetString(2),reader.GetString(1)); PrimaryKey=reader.GetString(3)
                            ForeignTable=Table.CreateFullName(reader.GetString(5),reader.GetString(4)); ForeignKey=reader.GetString(6) } ] 
            reader.Dispose()
            use reader = MySql.executeSql (sprintf "%s WHERE RC.REFERENCED_TABLE_NAME = '%s'" baseQuery table.Name ) con
            let parents =
                [ while reader.Read() do 
                    yield { Name = reader.GetString(0); PrimaryTable=Table.CreateFullName(reader.GetString(2),reader.GetString(1)); PrimaryKey=reader.GetString(3)
                            ForeignTable= Table.CreateFullName(reader.GetString(5),reader.GetString(4)); ForeignKey=reader.GetString(6) } ] 
            relationshipLookup.Add(table.FullName,(children,parents))
            
            (children,parents))
        
        // todo
        member __.GetSprocs(con) = MySql.connect con MySql.getSprocs

        member this.GetIndividualsQueryText(table,amount) = sprintf "SELECT * FROM %s LIMIT %i;" (table.FullName.Replace("[","`").Replace("]","`")) amount 
        member this.GetIndividualQueryText(table,column) = sprintf "SELECT * FROM `%s`.`%s` WHERE `%s`.`%s`.`%s` = @id" table.Schema table.Name table.Schema table.Name column
        
        member this.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns) = 
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
            let columns = 
                String.Join(",",
                    [|for KeyValue(k,v) in projectionColumns do
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in columnLookup.[(getTable k).FullName] |> List.map(fun c -> c.Name) do 
                                if singleEntity then yield sprintf "`%s`.`%s` as `%s`" k col col
                                else yield sprintf "`%s`.`%s` as '`%s`.`%s`'" k col k col
                        else
                            for col in v do 
                                if singleEntity then yield sprintf "`%s`.`%s` as `%s`" k col col
                                else yield sprintf "`%s`.`%s` as '`%s`.`%s`'" k col k col|]) // F# makes this so easy :)
        
            // next up is the filter expressions
            // make this nicer later.. just try and get the damn thing to work properly (well, at all) for now :D
            // NOTE: really need to assign the parameters their correct sql types
            let param = ref 0
            let nextParam() =
                incr param
                sprintf "@param%i" !param

            let createParam (value:obj) =
                let paramName = nextParam()
                (this:>ISqlProvider).CreateCommandParameter(QueryParameter.Create(paramName, !param), value)

            let rec filterBuilder = function 
                | [] -> ()
                | (cond::conds) ->
                    let build op preds (rest:Condition list option) =
                        ~~ "("
                        preds |> List.iteri( fun i (alias,col,operator,data) ->
                                let extractData data = 
                                     match data with
                                     | Some(x) when (box x :? System.Array) ->
                                         // in and not in operators pass an array
                                         let elements = box x :?> System.Array
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
                                        | FSharp.Data.Sql.In -> (sprintf "`%s`.`%s` IN (%s)") alias col text
                                        | FSharp.Data.Sql.NotIn -> (sprintf "`%s`.`%s` NOT IN (%s)") alias col text
                                        | _ -> failwith "Should not be called with any other operator"

                                let prefix = if i>0 then (sprintf " %s " op) else ""
                                let paras = extractData data
                                ~~(sprintf "%s%s" prefix <|
                                    match operator with
                                    | FSharp.Data.Sql.IsNull -> (sprintf "`%s`.`%s` IS NULL") alias col 
                                    | FSharp.Data.Sql.NotNull -> (sprintf "`%s`.`%s` IS NOT NULL") alias col 
                                    | FSharp.Data.Sql.In -> operatorIn operator paras
                                    | FSharp.Data.Sql.NotIn -> operatorIn operator paras
                                    | _ -> 
                                        parameters.Add paras.[0]
                                        (sprintf "`%s`.`%s`%s %s") alias col 
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
                    ~~  (sprintf "%s `%s`.`%s` as `%s` on `%s`.`%s` = `%s`.`%s` " 
                            joinType destTable.Schema destTable.Name destAlias 
                            (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias)
                            data.ForeignKey  
                            (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias) 
                            data.PrimaryKey))

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) -> 
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "`%s`.`%s` %s" alias column (if not desc then "DESC" else "")))

            // SELECT
            if sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s " columns)
            elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
            else  ~~(sprintf "SELECT %s " columns)
            // FROM
            ~~(sprintf "FROM %s as `%s` " (baseTable.FullName.Replace("[","`").Replace("]","`"))  baseAlias)         
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
            | None, Some skip -> ~~(sprintf " LIMIT %i OFFSET %i;" System.UInt64.MaxValue skip)
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
            let createInsertCommand (entity:SqlEntity) =                 
                let cmd = (this :> ISqlProvider).CreateCommand(con,"")
                cmd.Connection <- con 
                let pk = pkLookup.[entity.Table.FullName] 
                let columnNames, values = 
                    (([],0),entity.ColumnValues)
                    ||> Seq.fold(fun (out,i) (k,v) -> 
                        let name = sprintf "@param%i" i
                        let p = (this :> ISqlProvider).CreateCommandParameter(QueryParameter.Create(name, i),v)
                        (k,p)::out,i+1)
                    |> fun (x,_)-> x 
                    |> List.rev
                    |> List.toArray 
                    |> Array.unzip
                
                sb.Clear() |> ignore
                ~~(sprintf "INSERT INTO %s (%s) VALUES (%s); SELECT LAST_INSERT_ID();" 
                    (entity.Table.FullName.Replace("[","`").Replace("]","`"))
                    (String.Join(",",columnNames))
                    (String.Join(",",values |> Array.map(fun p -> p.ParameterName))))
                
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
                            | Some v -> (this :> ISqlProvider).CreateCommandParameter(QueryParameter.Create(name, i),v)
                            | None -> (this :> ISqlProvider).CreateCommandParameter(QueryParameter.Create(name, i), DBNull.Value)
                        (col,p)::out,i+1)
                    |> fun (x,_)-> x 
                    |> List.rev
                    |> List.toArray 
                    
                
                let pkParam = (this :> ISqlProvider).CreateCommandParameter(QueryParameter.Create("@pk", 0),pkValue)

                ~~(sprintf "UPDATE %s SET %s WHERE %s = @pk;" 
                    (entity.Table.FullName.Replace("[","`").Replace("]","`"))
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
                let p = (this :> ISqlProvider).CreateCommandParameter(QueryParameter.Create("@pk", 0),pkValue)
                cmd.Parameters.Add(p) |> ignore
                ~~(sprintf "DELETE FROM %s WHERE %s = @id" (entity.Table.FullName.Replace("[","`").Replace("]","`")) pk )
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
