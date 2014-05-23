﻿namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Generic
open System.Data

open FSharp.Data.Sql
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common

type internal PostgresqlProvider(toolPath) as this =
    let pkLookup =     Dictionary<string,string>()
    let tableLookup =  Dictionary<string,Table>()
    let columnLookup = Dictionary<string,Column list>()    
    let relationshipLookup = Dictionary<string,Relationship list * Relationship list>()

    let loadAssembly (name:string) = 
        // first try the gac (naively)
        try Reflection.Assembly.Load name
        with
        | _ ->
             Reflection.Assembly.LoadFrom(
                if String.IsNullOrEmpty toolPath then name + ".dll" 
                else System.IO.Path.Combine(toolPath,name+".dll"))
    // Dynamically load the Npgsql assembly so we don't have a dependency on it in the project
    let assembly =  loadAssembly "Npgsql"
    let monoSecurity = loadAssembly "Mono.Security"
   
    let connectionType =  (assembly.GetTypes() |> Array.find(fun t -> t.Name = "NpgsqlConnection"))
    let commandType =     (assembly.GetTypes() |> Array.find(fun t -> t.Name = "NpgsqlCommand"))
    let paramterType =    (assembly.GetTypes() |> Array.find(fun t -> t.Name = "NpgsqlParameter"))
    let getSchemaMethod = (connectionType.GetMethod("GetSchema",[|typeof<string>|]))

    let mutable clrToEnum : (string -> DbType option)  = fun _ -> failwith "!"
    let mutable sqlToEnum : (string -> DbType option)  = fun _ -> failwith "!"
    let mutable sqlToClr :  (string -> Type option)    = fun _ -> failwith "!"

    let createTypeMappings () =        
        // there doesn't seem to be any mapping in the schema
        // so we are stuck doing this the old fashioned way
        let sqlToClr' = function
            | "bigint"
            | "int8"       -> Some typeof<Int64>
            | "bit"           // Doesn't seem to correspond to correct type - fixed-length bit string (Npgsql.BitString)
            | "varbit"        // Doesn't seem to correspond to correct type - variable-length bit string (Npgsql.BitString)
            | "boolean"
            | "bool"       -> Some typeof<Boolean>
            | "Box"
            | "Circle"
            | "Line"
            | "LSeg"
            | "Path"
            | "Point"
            | "Polygon"    -> Some typeof<Object>
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
        
        let sqlToEnum' = function
            | "bigint"
            | "int8"       -> Some DbType.Int64
            | "bit"           // Doesn't seem to correspond to correct type - fixed-length bit string (Npgsql.BitString)
            | "varbit"        // Doesn't seem to correspond to correct type - variable-length bit string (Npgsql.BitString)
            | "boolean"
            | "bool"       -> Some DbType.Boolean
            | "Box"
            | "Circle"
            | "Line"
            | "LSeg"
            | "Path"
            | "Point"
            | "interval"
            | "inet"
            | "Polygon"    -> Some DbType.Object
            | "bytea"      -> Some DbType.Binary
            | "double"
            | "float8"     -> Some DbType.Double
            | "integer"
            | "int"
            | "int4"       -> Some DbType.Int32
            | "money"
            | "numeric"    -> Some DbType.Decimal
            | "real"
            | "float4"     -> Some DbType.Single
            | "smallint"
            | "int2"       -> Some DbType.Int16
            | "timestamp" 
            | "timestamptz"
            | "date"       -> Some DbType.DateTime
            | "time"
            | "timetz"     -> Some DbType.Time 
            | "text"
            | "character"
            | "varchar"    -> Some DbType.String
            | "uuid"       -> Some DbType.Guid
            | "xml"        -> Some DbType.Xml
            | _ -> None
        
        let clrToEnum' n = 
            if   n = typeof<Int64>.Name then Some DbType.Int64
            elif n = typeof<Int32>.Name then Some DbType.Int32
            elif n = typeof<Int16>.Name then Some DbType.Int16
            elif n = typeof<Boolean>.Name then Some DbType.Boolean
            elif n = typeof<Object>.Name then Some DbType.Object
            elif n = typeof<Byte[]>.Name then Some DbType.Binary
            elif n = typeof<Double>.Name then Some DbType.Double
            elif n = typeof<Decimal>.Name then Some DbType.Decimal
            elif n = typeof<Single>.Name then Some DbType.Single
            elif n = typeof<String>.Name then Some DbType.String
            elif n = typeof<DateTime>.Name then Some DbType.DateTime
            elif n = typeof<TimeSpan>.Name then Some DbType.Object
            elif n = typeof<System.Net.IPAddress>.Name then Some DbType.Object
            elif n = typeof<Guid>.Name then Some DbType.Guid
            else None

        // set lookup functions         
        sqlToClr <-  sqlToClr'
        sqlToEnum <- sqlToEnum' 
        clrToEnum <- clrToEnum' 
    
    let executeSql (con:IDbConnection) sql =        
        use com = (this:>ISqlProvider).CreateCommand(con,sql)    
        com.ExecuteReader()

    interface ISqlProvider with
        member __.CreateConnection(connectionString) = Activator.CreateInstance(connectionType,[|box connectionString|]) :?> IDbConnection
        member __.CreateCommand(connection,commandText) =  Activator.CreateInstance(commandType,[|box commandText;box connection|]) :?> IDbCommand
        member __.CreateCommandParameter(name,value,dbType) = 
            let p = Activator.CreateInstance(paramterType,[|box name;box value|]) :?> IDbDataParameter
            if dbType.IsSome then p.DbType <- dbType.Value 
            upcast p
        member __.CreateTypeMappings(_) = 
            createTypeMappings()
        member __.ClrToEnum = clrToEnum
        member __.SqlToEnum = sqlToEnum
        member __.SqlToClr = sqlToClr
        member __.GetTables(con) =            
            use reader = executeSql con "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE from INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'public'"
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
                let p =  (this:>ISqlProvider).CreateCommandParameter("@schema",table.Schema,None)
                com.Parameters.Add p |> ignore
                let p =  (this:>ISqlProvider).CreateCommandParameter("@table",table.Name,None)
                com.Parameters.Add p |> ignore
                if con.State <> ConnectionState.Open then con.Open()
                use reader = com.ExecuteReader()
                let columns =
                   [ while reader.Read() do 
                       let dt = reader.GetString(1).ToLower().Replace("\"","")
                       // postgre gives some really weird type names here like  "double precision" and  "timestamp with time zone"
                       // this is a simple first implementation, there's also some complex types that i don't think are supported
                       // with this .net connector, but this needs examining in detail (probably by someone else!)
                       let dt = if dt.Contains(" ") then dt.Substring(0,dt.IndexOf(" ")).Trim() else dt
                       match sqlToClr dt, sqlToEnum dt with
                       | Some(clr),Some(sql) ->
                          let col =
                             { Column.Name = reader.GetString(0)
                               ClrType = clr; 
                               DbType = sql; 
                               IsNullable = let b = reader.GetString(4) in if b = "YES" then true else false
                               IsPrimarKey = if reader.GetString(5) = "PRIMARY KEY" then true else false } 
                          if col.IsPrimarKey && pkLookup.ContainsKey table.FullName = false then pkLookup.Add(table.FullName,col.Name)
                          yield col 
                       | _ -> ()]  
                columnLookup.Add(table.FullName,columns)
                con.Close()
                columns
        member __.GetRelationships(con,table) =
            match relationshipLookup.TryGetValue(table.FullName) with
            | true,v -> v
            | _ ->
                let toSchema schema table = sprintf "[%s].[%s]" schema table
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
                        yield { Name = reader.GetString(0); PrimaryTable=toSchema (reader.GetString(9)) (reader.GetString(5)); PrimaryKey=reader.GetString(6)
                                ForeignTable=toSchema (reader.GetString(8)) (reader.GetString(1)); ForeignKey=reader.GetString(2) } ] 
                reader.Dispose()
                use reader = executeSql con (sprintf "%s WHERE KCU1.TABLE_NAME = '%s'" baseQuery table.Name )
                let parents =
                    [ while reader.Read() do 
                        yield { Name = reader.GetString(0); PrimaryTable=toSchema (reader.GetString(9)) (reader.GetString(5)); PrimaryKey=reader.GetString(6)
                                ForeignTable=toSchema (reader.GetString(8)) (reader.GetString(1)); ForeignKey=reader.GetString(2) } ] 
                relationshipLookup.Add(table.FullName,(children,parents))
                con.Close()
                (children,parents)    
        
        /// Have not attempted stored procs yet
        member __.GetSprocs(con) = [] 

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
                (this:>ISqlProvider).CreateCommandParameter(paramName,value,None)

            let rec filterBuilder = function 
                | [] -> ()
                | (cond::conds) ->
                    let build op preds (rest:Condition list option) =
                        ~~ "("
                        preds |> List.iteri( fun i (alias,col,operator,data) ->
                                let extractData data = 
                                     match data with
                                     | Some(x) when (box x :? string array) -> 
                                         // in and not in operators pass an array
                                         let strings = box x :?> string array
                                         strings |> Array.map createParam
                                     | Some(x) -> [|createParam (box x)|]
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
                        let p = (this :> ISqlProvider).CreateCommandParameter(name,v,None)
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
                            | Some v -> (this :> ISqlProvider).CreateCommandParameter(name,v,None)
                            | None -> (this :> ISqlProvider).CreateCommandParameter(name,DBNull.Value, None)
                        (col,p)::out,i+1)
                    |> fun (x,_)-> x 
                    |> List.rev
                    |> List.toArray 
                    
                
                let pkParam = (this :> ISqlProvider).CreateCommandParameter("@pk", pkValue, None)

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
                let p = (this :> ISqlProvider).CreateCommandParameter("@id",pkValue,None)
                cmd.Parameters.Add(p) |> ignore
                ~~(sprintf "DELETE FROM %s WHERE %s = @id" (entity.Table.FullName.Replace("[","\"").Replace("]","\"")) pk )
                cmd.CommandText <- sb.ToString()
                cmd

            use scope = new Transactions.TransactionScope()
            try
                
                if con.State <> ConnectionState.Open then con.Open()         
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
