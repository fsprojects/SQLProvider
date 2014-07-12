namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Generic
open System.Data
open System.Data.Odbc
open FSharp.Data.Sql
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common

type internal OdbcProvider(resolutionPath) =
    let pkLookup =     Dictionary<string,string>()
    let tableLookup =  Dictionary<string,Table>()
    let columnLookup = Dictionary<string,Column list>()
    let relationshipLookup = Dictionary<string,Relationship list * Relationship list>()

    let mutable typeMappings = []
    let mutable findClrType : (string -> TypeMapping option)  = fun _ -> failwith "!"
    let mutable findDbType : (string -> TypeMapping option)  = fun _ -> failwith "!"

    let createTypeMappings (con:OdbcConnection) =
        let dt = con.GetSchema("DataTypes")

        let getDbType(providerType:int) =
            let p = new OdbcParameter()
            p.OdbcType <- (Enum.ToObject(typeof<OdbcType>, providerType) :?> OdbcType)
            p.DbType

        let getClrType (input:string) = Type.GetType(input).ToString()

        let mappings =             
            [
                for r in dt.Rows do
                    let clrType = getClrType (string r.["DataType"])
                    let oleDbType = string r.["TypeName"]
                    let providerType = unbox<int> r.["ProviderDbType"]
                    let dbType = getDbType providerType
                    yield { ProviderTypeName = oleDbType; ClrType = clrType; DbType = dbType; ProviderType = providerType; }
            ]

        let clrMappings =
            mappings
            |> List.map (fun m -> m.ClrType, m)
            |> Map.ofList

        let dbMappings = 
            mappings
            |> List.map (fun m -> m.ProviderTypeName, m)
            |> Map.ofList
            
        typeMappings <- mappings
        findClrType <- clrMappings.TryFind
        findDbType <- dbMappings.TryFind 
    
    let executeSql (con:IDbConnection) sql =
        use com = new OdbcCommand(sql,con:?>OdbcConnection)    
        com.ExecuteReader()

    interface ISqlProvider with
        member __.CreateConnection(connectionString) = upcast new OdbcConnection(connectionString)
        member __.ReadDatabaseParameter(reader:IDataReader,parameter:IDbDataParameter) = raise(NotImplementedException())
        member __.CreateCommand(connection,commandText) = upcast new OdbcCommand(commandText, connection:?>OdbcConnection)
        member __.CreateCommandParameter(name,value,dbType, direction, length) = 
            let p = OdbcParameter()            
            p.Value <- value
            p.ParameterName <- name
            if dbType.IsSome then p.DbType <- dbType.Value.DbType
            if direction.IsSome then p.Direction <- direction.Value
            if length.IsSome then p.Size <- length.Value
            upcast p
        member __.CreateTypeMappings(con) = createTypeMappings (con:?>OdbcConnection)     
        member __.GetTables(con) =
            let con = con :?> OdbcConnection
            if con.State <> ConnectionState.Open then con.Open()
            let dataTables = con.GetSchema("Tables").Rows |> Seq.cast<DataRow> |> Seq.map (fun i -> i.ItemArray)
            [ for dataTable in dataTables do 
                let table ={ Schema = string dataTable.[1] ; Name = string dataTable.[2] ; Type=(string dataTable.[3]).ToLower() } 
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
               let con = con :?> OdbcConnection
               if con.State <> ConnectionState.Open then con.Open()
               let primaryKey = con.GetSchema("Indexes", [| null; null; table.Name |]).Rows |> Seq.cast<DataRow> |> Seq.map (fun i -> i.ItemArray) |> Array.ofSeq
               let dataTable = con.GetSchema("Columns", [| null; null; table.Name; null|]).Rows |> Seq.cast<DataRow> |> Seq.map (fun i -> i.ItemArray)
               let columns =
                  [ for i in dataTable do 
                      let dt = i.[5] :?> string
                      match findDbType dt with
                      | Some(m) ->
                         let name = i.[3] :?> string
                         let col =
                            { Column.Name = name 
                              TypeMapping = m
                              IsNullable = let b = i.[17] :?> string in if b = "YES" then true else false
                              IsPrimarKey = if primaryKey.Length > 0 && primaryKey.[0].[8] = box name then true else false } 
                         if col.IsPrimarKey && pkLookup.ContainsKey table.FullName = false then pkLookup.Add(table.FullName,col.Name)
                         yield col 
                      | _ -> ()]  
               columnLookup.Add(table.FullName,columns)
               columns
        member __.GetRelationships(con,table) =
            match relationshipLookup.TryGetValue table.FullName with 
            | true,v -> v
            | _ -> 
            // mostly stolen from
            // http://msdn.microsoft.com/en-us/library/aa175805(SQL.80).aspx
            let con = con :?> OdbcConnection
            if con.State <> ConnectionState.Open then con.Open()
            let x = con.GetSchema().Rows |> Seq.cast<DataRow> |> Seq.map (fun i -> i.ItemArray.[0]) |> Seq.cast<string>
            match x |> Seq.exists (fun i -> i = "ForeignKeys") with
            | false -> ([],[])
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
            (children,parents)    

        member __.GetSprocs(con) =
            failwith "The ODBC type provider does not currently support Stored Procedures operations."

        member this.GetIndividualsQueryText(table,amount) =
            sprintf "SELECT * FROM `%s`" table.Name

        member this.GetIndividualQueryText(table,column) =
            sprintf "SELECT * FROM `%s` WHERE `%s`.`%s` = ?" table.Name table.Name column
        
        member this.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns) = 
            let sb = System.Text.StringBuilder()
            let parameters = ResizeArray<_>()
            let (~~) (t:string) = sb.Append t |> ignore
            
            let getTable x =
                match sqlQuery.Aliases.TryFind x with
                | Some(a) -> a
                | None -> baseTable

            let singleEntity = sqlQuery.Aliases.Count = 0
            
            let columns = 
                String.Join(",",
                    [|for KeyValue(k,v) in projectionColumns do
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in columnLookup.[(getTable k).FullName] |> List.map(fun c -> c.Name) do 
                                if singleEntity then yield sprintf "`%s`" col
                                else yield sprintf "`%s`.`%s` as `%s_%s`" k col k col
                        else
                            for col in v do 
                                if singleEntity then yield sprintf "`%s`" col
                                else yield sprintf "`%s`.`%s` as `%s_%s`" k col k col |]) // F# makes this so easy :)
        
            // make this nicer later.. just try and get the damn thing to work properly (well, at all) for now :D
            // NOTE: really need to assign the parameters their correct sql types

            let createParam (value:obj) =
                let paramName = "?"
                OdbcParameter(paramName,value):> IDataParameter

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
                                    | FSharp.Data.Sql.IsNull -> (sprintf "`%s`.`%s` IS NULL") alias col 
                                    | FSharp.Data.Sql.NotNull -> (sprintf "`%s`.`%s` IS NOT NULL") alias col 
                                    | FSharp.Data.Sql.In ->                                     
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "`%s`.`%s` IN (%s)") alias col text
                                    | FSharp.Data.Sql.NotIn ->                                    
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "`%s`.`%s` NOT IN (%s)") alias col text 
                                    | _ -> 
                                        parameters.Add paras.[0]
                                        (sprintf "`%s`.%s %s %s") alias col 
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
                    ~~  (sprintf "%s `%s` as `%s` on `%s`.`%s` = `%s`.`%s` " 
                            joinType destTable.Name destAlias 
                            (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias)
                            data.ForeignKey  
                            (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias) 
                            data.PrimaryKey))

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) -> 
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "`%s`.`%s` %s" alias column (if not desc then "DESC" else "")))

            // Certain ODBC drivers (excel) don't like special characters in aliases, so we need to strip them
            // or else it will fail
            let stripSpecialCharacters (s:string) =
                String(s.ToCharArray() |> Array.filter(fun c -> Char.IsLetterOrDigit c || c = ' ' || c = '_'))

            // SELECT
            if sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s " columns)
            elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
            else  ~~(sprintf "SELECT %s " columns)
            // FROM
            ~~(sprintf "FROM `%s` as `%s` " baseTable.Name (stripSpecialCharacters baseAlias))
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

            let sql = sb.ToString()
            (sql,parameters)

        member this.ProcessUpdates(con, entities) =            
            let sb = Text.StringBuilder()
            let (~~) (t:string) = sb.Append t |> ignore

            // ensure columns have been loaded
            entities |> List.map(fun e -> e.Table) 
                     |> Seq.distinct 
                     |> Seq.iter(fun t -> (this :> ISqlProvider).GetColumns(con,t) |> ignore )

            if con.State <> ConnectionState.Open then con.Open()

            let createInsertCommand (entity:SqlEntity) =     
                let cmd = new OdbcCommand()
                cmd.Connection <- con :?> OdbcConnection
                let pk = pkLookup.[entity.Table.FullName] 
                let columnNames, values = 
                    (([],0),entity.ColumnValues)
                    ||> Seq.fold(fun (out,i) (key,value) ->                         
                        let name = sprintf "@param%i" i
                        let p = OdbcParameter(name,value)
                        (key,p)::out,i+1)
                    |> fun (x,_)-> x 
                    |> List.rev
                    |> List.toArray 
                    |> Array.unzip
                
                sb.Clear() |> ignore
                ~~(sprintf "INSERT INTO %s (%s) VALUES (%s);" 
                    entity.Table.Name
                    (String.Join(",",columnNames))
                    (String.Join(",",values |> Array.map(fun p -> "?"))))
                cmd.Parameters.AddRange(values)
                cmd.CommandText <- sb.ToString()
                cmd

            let lastInsertId () =
                let cmd = new OdbcCommand()
                cmd.Connection <- con :?> OdbcConnection
                cmd.CommandText <- "SELECT @@IDENTITY AS id;"
                cmd

            let createUpdateCommand (entity:SqlEntity) changedColumns =
                let cmd = new OdbcCommand()
                cmd.Connection <- con :?> OdbcConnection
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
                        let p = 
                            match entity.GetColumnOption<obj> col with
                            | Some v -> OdbcParameter(null,v)
                            | None -> OdbcParameter(null,DBNull.Value)
                        (col,p)::out,i+1)
                    |> fun (x,_)-> x 
                    |> List.rev
                    |> List.toArray 
                
                let pkParam = OdbcParameter(null, pkValue)

                ~~(sprintf "UPDATE %s SET %s WHERE %s = ?;" 
                    entity.Table.Name
                    (String.Join(",", data |> Array.map(fun (c,p) -> sprintf "%s = %s" c "?" ) ))
                    pk)

                cmd.Parameters.AddRange(data |> Array.map snd)
                cmd.Parameters.Add pkParam |> ignore
                cmd.CommandText <- sb.ToString()
                cmd
            
            let createDeleteCommand (entity:SqlEntity) =
                let cmd = new OdbcCommand()
                cmd.Connection <- con :?> OdbcConnection
                sb.Clear() |> ignore
                let pk = pkLookup.[entity.Table.FullName] 
                sb.Clear() |> ignore
                let pkValue = 
                    match entity.GetColumnOption<obj> pk with
                    | Some v -> v
                    | None -> failwith "Error - you cannot delete an entity that does not have a primary key."
                cmd.Parameters.AddWithValue("@id",pkValue) |> ignore
                ~~(sprintf "DELETE FROM %s WHERE %s = ?" entity.Table.Name pk )
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
                        cmd.ExecuteNonQuery() |> ignore
                        let id = (lastInsertId()).ExecuteScalar()
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