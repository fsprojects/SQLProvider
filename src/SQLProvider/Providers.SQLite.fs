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

type internal SQLiteProvider(resolutionPath, referencedAssemblies, runtimeAssembly, sqliteLibrary) as this =
    // note we intentionally do not hang onto a connection object at any time,
    // as the type provider will dicate the connection lifecycles
    let pkLookup = ConcurrentDictionary<string,string list>()
    let tableLookup = ConcurrentDictionary<string,Table>()
    let columnLookup = ConcurrentDictionary<string,ColumnLookup>()
    let relationshipLookup = Dictionary<string,Relationship list * Relationship list>()

    let useMono = 
        match sqliteLibrary with
        | SQLiteLibrary.SystemDataSQLite -> false
        | SQLiteLibrary.MonoDataSQLite -> true
        | SQLiteLibrary.AutoSelect -> Type.GetType ("Mono.Runtime") <> null
        | _ -> failwith ("Unsupported SQLiteLibrary option: " + sqliteLibrary.ToString())

    // Dynamically load the SQLite assembly so we don't have a dependency on it in the project
    let assemblyNames =
        [
           (if useMono then "Mono" else "System") + ".Data.SQLite.dll"
           (if useMono then "Mono" else "System") + ".Data.Sqlite.dll"
        ]

    let assembly =
        lazy Reflection.tryLoadAssemblyFrom resolutionPath (Array.append [|runtimeAssembly|] referencedAssemblies) assemblyNames

    let findType f =
        match assembly.Value with
        | Choice1Of2(assembly) -> assembly.GetTypes() |> Array.find f
        | Choice2Of2(paths, errors) ->
           let details = 
                match errors with 
                | [] -> "" 
                | x -> Environment.NewLine + "Details: " + Environment.NewLine + String.Join(Environment.NewLine, x)
           failwithf "Unable to resolve assemblies. One of %s (e.g. from Nuget package System.Data.SQLite.Core) must exist in the paths: %s %s %s"
                (String.Join(", ", assemblyNames |> List.toArray))
                Environment.NewLine
                (String.Join(Environment.NewLine, paths |> Seq.filter(fun p -> not(String.IsNullOrEmpty p))))
                details

    let connectionType =  (findType (fun t -> t.Name = if useMono then "SqliteConnection" else "SQLiteConnection"))
    let commandType =     (findType (fun t -> t.Name = if useMono then "SqliteCommand" else "SQLiteCommand"))
    let paramterType =    (findType (fun t -> t.Name = if useMono then "SqliteParameter" else "SQLiteParameter"))
    let getSchemaMethod = (connectionType.GetMethod("GetSchema",[|typeof<string>|]))


    let getSchema name conn =
        getSchemaMethod.Invoke(conn,[|name|]) :?> DataTable

    let mutable typeMappings = []
    let mutable findClrType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"

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

        sb.Clear() |> ignore
        ~~(sprintf "INSERT INTO %s (%s) VALUES (%s); SELECT last_insert_rowid();"
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
        let haspk = pkLookup.ContainsKey(entity.Table.FullName)
        let pk = if haspk then pkLookup.[entity.Table.FullName] else []
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

    interface ISqlProvider with
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
                Activator.CreateInstance(connectionType,[|box connectionString|]) :?> IDbConnection
            with
            | :? System.Reflection.ReflectionTypeLoadException as ex ->
                let errorfiles = ex.LoaderExceptions |> Array.map(fun e -> e.Message) |> Seq.distinct |> Seq.toArray
                let msg = ex.Message + "\r\n" + String.Join("\r\n", errorfiles)
                raise(new System.Reflection.TargetInvocationException(msg, ex))
            | :? System.Reflection.TargetInvocationException as ex when (ex.InnerException <> null && ex.InnerException :? DllNotFoundException) ->
                let msg = ex.InnerException.Message + ", Path: " + (Path.GetFullPath resolutionPath)
                raise(new System.Reflection.TargetInvocationException(msg, ex))

        member __.CreateCommand(connection,commandText) = Activator.CreateInstance(commandType,[|box commandText;box connection|]) :?> IDbCommand

        member __.CreateCommandParameter(param,value) =
            let p = Activator.CreateInstance(paramterType,[|box param.Name;box value|]) :?> IDbDataParameter
            p.DbType <- param.TypeMapping.DbType
            p.Direction <- param.Direction
            Option.iter (fun l -> p.Size <- l) param.Length
            p

        member __.ExecuteSprocCommand(_,_,_,_) =  raise(NotImplementedException())

        member __.CreateTypeMappings(con) =
            if con.State <> ConnectionState.Open then con.Open()
            createTypeMappings con
            con.Close()

        member __.GetTables(con,_) =
            if con.State <> ConnectionState.Open then con.Open()
            let ret =
                [ for row in (getSchemaMethod.Invoke(con,[|"Tables"|]) :?> DataTable).Rows do
                    let ty = string row.["TABLE_TYPE"]
                    if ty <> "SYSTEM_TABLE" then
                        let table = { Schema = string row.["TABLE_CATALOG"] ; Name = string row.["TABLE_NAME"]; Type=ty }
                        yield tableLookup.GetOrAdd(table.FullName,table)
                        ]
            con.Close()
            ret

        member __.GetPrimaryKey(table) =
            match pkLookup.TryGetValue table.FullName with
            | true, [v] -> Some v
            | _ -> None

        member __.GetColumns(con,table) =
            match columnLookup.TryGetValue table.FullName with
            | (true,data) when data.Count > 0 -> data
            | _ ->
                if con.State <> ConnectionState.Open then con.Open()
                let query = sprintf "pragma table_info(%s)" table.Name
                use com = (this:>ISqlProvider).CreateCommand(con,query)
                use reader = com.ExecuteReader()
                let columns =
                    [ while reader.Read() do
                        let dt = reader.GetString(2).ToLower()
                        let dt = if dt.Contains("(") then dt.Substring(0,dt.IndexOf("(")) else dt
                        let dt = dt.Trim()
                        match findDbType dt with
                        | Some(m) ->
                            let col =
                                { Column.Name = reader.GetString(1);
                                  TypeMapping = m
                                  IsNullable = not <| reader.GetBoolean(3);
                                  IsPrimaryKey = if reader.GetBoolean(5) then true else false }
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
          System.Threading.Monitor.Enter relationshipLookup
          try
            match relationshipLookup.TryGetValue(table.FullName) with
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
                let relData = (getSchemaMethod.Invoke(con,[|"ForeignKeys"|]) :?> DataTable)
                for row in relData.Rows do
                    let pTable =
                        { Schema = string row.["FKEY_TO_CATALOG"]     //I've not seen a schema column populated in SQLite so I'm using catalog instead
                          Name = string row.["FKEY_TO_TABLE"]
                          Type = ""}
                    let fTable =
                        { Schema = string row.["TABLE_CATALOG"]
                          Name = string row.["TABLE_NAME"]
                          Type = ""}

                    if not <| relationshipLookup.ContainsKey pTable.FullName then relationshipLookup.Add(pTable.FullName,([],[]))
                    if not <| relationshipLookup.ContainsKey fTable.FullName then relationshipLookup.Add(fTable.FullName,([],[]))

                    let rel = { Name = string row.["CONSTRAINT_NAME"]; PrimaryTable= pTable.FullName; PrimaryKey=string row.["FKEY_TO_COLUMN"]
                                ForeignTable=fTable.FullName; ForeignKey=string row.["FKEY_FROM_COLUMN"] }

                    let (c,p) = relationshipLookup.[pTable.FullName]
                    relationshipLookup.[pTable.FullName] <- (rel::c,p)
                    let (c,p) = relationshipLookup.[fTable.FullName]
                    relationshipLookup.[fTable.FullName] <- (c,rel::p)
                con.Close()
                match relationshipLookup.TryGetValue table.FullName with
                | true,v -> v
                | _ -> [],[]
          finally
            System.Threading.Monitor.Exit relationshipLookup

        member __.GetSprocs(_) = [] // SQLite does not support stored procedures.
        member __.GetIndividualsQueryText(table,amount) = sprintf "SELECT * FROM %s LIMIT %i;" table.FullName amount
        member __.GetIndividualQueryText(table,column) = sprintf "SELECT * FROM [%s].[%s] WHERE [%s].[%s].[%s] = @id" table.Schema table.Name table.Schema table.Name column

        member this.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns) =
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
                                else yield sprintf "[%s].[%s] as '[%s].[%s]'" k col k col|]) // F# makes this so easy :)

            // Create sumBy, minBy, maxBy, ... field columns
            let columns =
                let extracolumns =
                    let fieldNotation(al:alias,col:string) =
                        match String.IsNullOrEmpty(al) with
                        | true -> sprintf "[%s]" col
                        | false -> sprintf "[%s].[%s]" al col
                    let fieldNotationAlias(al:alias,col:string) =
                        match String.IsNullOrEmpty(al) with
                        | true -> sprintf "'%s'" col
                        | false -> sprintf "'[%s].[%s]'" al col
                    
                    match sqlQuery.Grouping with
                    | [] -> FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation fieldNotationAlias sqlQuery.AggregateOp
                    | g  -> 
                        let keys = g |> List.map(fst) |> List.concat |> List.map(fieldNotation)
                        let aggs = g |> List.map(snd) |> List.concat
                        let res2 = FSharp.Data.Sql.Common.Utilities.parseAggregates fieldNotation fieldNotationAlias aggs |> List.toSeq
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

            

            let rec filterBuilder = function
                | [] -> ()
                | (cond::conds) ->
                    let build op preds (rest:Condition list option) =
                        ~~ "("
                        preds |> List.iteri( fun i (alias,col,operator,data) ->
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
                                    | FSharp.Data.Sql.IsNull -> (sprintf "[%s].[%s] IS NULL") alias col
                                    | FSharp.Data.Sql.NotNull -> (sprintf "[%s].[%s] IS NOT NULL") alias col
                                    | FSharp.Data.Sql.In ->
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "[%s].[%s] IN (%s)") alias col text
                                    | FSharp.Data.Sql.NestedIn when data.IsSome ->
                                        let innersql, innerpars = data.Value |> box :?> string * IDbDataParameter[]
                                        Array.iter parameters.Add innerpars
                                        (sprintf "[%s].[%s] IN (%s)") alias col innersql
                                    | FSharp.Data.Sql.NotIn ->
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "[%s].[%s] NOT IN (%s)") alias col text
                                    | FSharp.Data.Sql.NestedNotIn when data.IsSome ->
                                        let innersql, innerpars = data.Value |> box :?> string * IDbDataParameter[]
                                        Array.iter parameters.Add innerpars
                                        (sprintf "[%s].[%s] NOT IN (%s)") alias col innersql
                                    | _ ->
                                        let aliasformat = if alias<>"" then (sprintf "[%s].[%s]%s %s") alias col else (sprintf "%s %s %s") col
                                        match data with 
                                        | Some d when (box d :? alias * string) ->
                                            let alias2, col2 = box d :?> (alias * string)
                                            let alias2f = if alias2<>"" then (sprintf "[%s].[%s]") alias2 col2 else col2
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
                        sprintf "[%s].[%s] = [%s].[%s] "
                            (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias)
                            foreignKey
                            (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias)
                            primaryKey))))

            let groupByBuilder() =
                sqlQuery.Grouping |> List.map(fst) |> List.concat
                |> List.iteri(fun i (alias,column) ->
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "[%s].[%s]" alias column))

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) ->
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "[%s].[%s]%s" alias column (if not desc then "DESC" else "")))

            // SELECT
            if sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s " columns)
            elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
            else  ~~(sprintf "SELECT %s " columns)
            // FROM
            ~~(sprintf "FROM %s as [%s] " baseTable.FullName baseAlias)
            fromBuilder()
            // WHERE
            if sqlQuery.Filters.Length > 0 then
                // each filter is effectively the entire contents of each where clause in the LINQ  query,
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

                let f = [And([],Some (sqlQuery.HavingFilters |> CommonTasks.parseHaving keys))]
                ~~" HAVING "
                filterBuilder f

            // ORDER BY
            if sqlQuery.Ordering.Length > 0 then
                ~~" ORDER BY "
                orderByBuilder()

            match sqlQuery.Union with
            | Some(true, suquery) -> ~~(sprintf " UNION ALL %s " suquery)
            | Some(false, suquery) -> ~~(sprintf " UNION %s " suquery)
            | None -> ()

            match sqlQuery.Take, sqlQuery.Skip with
            | Some take, Some skip ->  ~~(sprintf " LIMIT %i OFFSET %i;" take skip)
            | Some take, None ->  ~~(sprintf " LIMIT %i;" take)
            | None, Some skip -> ~~(sprintf " LIMIT %i OFFSET %i;" System.UInt32.MaxValue skip)
            | None, None -> ()

            let sql = sb.ToString()
            (sql,parameters)

        member this.ProcessUpdates(con, entities, transactionOptions) =
            let sb = Text.StringBuilder()

            CommonTasks.``ensure columns have been loaded`` (this :> ISqlProvider) con entities

            if entities.Count = 0 then 
                ()
            else

            con.Open()

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
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        let id = cmd.ExecuteScalar()
                        CommonTasks.checkKey pkLookup id e
                        e._State <- Unchanged
                    | Modified fields ->
                        use cmd = createUpdateCommand con sb e fields
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        cmd.ExecuteNonQuery() |> ignore
                        e._State <- Unchanged
                    | Delete ->
                        use cmd = createDeleteCommand con sb e
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        cmd.ExecuteNonQuery() |> ignore
                        // remove the pk to prevent this attempting to be used again
                        e.SetPkColumnOptionSilent(pkLookup.[e.Table.FullName], None)
                        e._State <- Deleted
                    | Deleted | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!")
                scope.Complete()

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
                                use cmd = createInsertCommand con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                let! id = cmd.ExecuteScalarAsync() |> Async.AwaitTask
                                CommonTasks.checkKey pkLookup id e
                                e._State <- Unchanged
                            }
                        | Modified fields ->
                            async {
                                use cmd = createUpdateCommand con sb e fields :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                e._State <- Unchanged
                            }
                        | Delete ->
                            async {
                                use cmd = createDeleteCommand con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                // remove the pk to prevent this attempting to be used again
                                e.SetPkColumnOptionSilent(pkLookup.[e.Table.FullName], None)
                                e._State <- Deleted
                            }
                        | Deleted | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!"

                    do! Utilities.executeOneByOne handleEntity (entities.Keys|>Seq.toList)
                    scope.Complete()

                finally
                    con.Close()
            }
