namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Generic
open System.Data
open System.Reflection
open FSharp.Data.Sql
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common

module internal Oracle =
    
    let mutable resolutionPath = String.Empty
    let mutable owner = String.Empty

    let assemblyNames = 
        [
            "Oracle.ManagedDataAccess.dll"
            "Oracle.DataAccess.dll"
        ]

    let assembly =
        lazy 
            assemblyNames 
            |> List.pick (fun asm ->
                try 
                    let loadedAsm =              
                        Assembly.LoadFrom(
                            if String.IsNullOrEmpty resolutionPath then asm
                            else System.IO.Path.Combine(resolutionPath,asm)
                            ) 
                    if loadedAsm <> null
                    then Some loadedAsm
                    else None
                with e ->
                    None)
    
    let findType name = (assembly.Value.GetTypes() |> Array.find(fun t -> t.Name = name))

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

    let createTypeMappings con =
        let dt = getSchema "DataTypes" [||] con

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
 
    let tryReadValueProperty instance = 
        let typ = instance.GetType()
        let prop = typ.GetProperty("Value")
        if prop <> null
        then prop.GetGetMethod().Invoke(instance, [||]) |> Some
        else None         

    let quoteWhiteSpace (str:String) = 
        (if str.Contains(" ") then sprintf "\"%s\"" str else str)
    
    let tableFullName (table:Table) = 
        table.Schema + "." + (quoteWhiteSpace table.Name)

    let dbUnbox<'a> (v:obj) : 'a = 
        if Convert.IsDBNull(v) then Unchecked.defaultof<'a> else unbox v
    
    let dbUnboxWithDefault<'a> def (v:obj) : 'a = 
        if Convert.IsDBNull(v) then def else unbox v

    let createConnection connectionString = 
        Activator.CreateInstance(connectionType.Value,[|box connectionString|]) :?> IDbConnection

    let connect (con:IDbConnection) f =
        if con.State <> ConnectionState.Open then con.Open()
        let result = f con
        con.Close(); result

    let createCommand commandText connection = 
        Activator.CreateInstance(commandType.Value,[|box commandText;box connection|]) :?> IDbCommand

    let createCommandParameter (param:QueryParameter) value = 
        let value = if value = null then (box System.DBNull.Value) else value
        let parameterType = parameterType.Value
        let oracleDbTypeSetter = 
            parameterType.GetProperty("OracleDbType").GetSetMethod()
        
        let p = Activator.CreateInstance(parameterType,[|box param.Name;value|]) :?> IDbDataParameter
        
        p.Direction <- param.Direction 
        
        p.DbType <- param.TypeMapping.DbType
        oracleDbTypeSetter.Invoke(p, [|param.TypeMapping.ProviderType|]) |> ignore
        
        match param.Length with
        | Some(length) when length >= 0 -> p.Size <- length
        | _ -> 
               match param.TypeMapping.DbType with
               | DbType.String -> p.Size <- 32767
               | _ -> ()
        p 

    let readParameter (reader:IDataReader) (parameter:IDbDataParameter) = 
        let parameterType = parameterType.Value
        let oracleDbTypeGetter = 
            parameterType.GetProperty("OracleDbType").GetGetMethod()

        match parameter.DbType, (oracleDbTypeGetter.Invoke(parameter, [||]) :?> int) with
        | DbType.Object, 121 -> 
            if parameter.Value = null
            then ReturnValueType.Reader(reader)
            else ReturnValueType.Reader(getDataReaderForRefCursor.Value.Invoke(parameter.Value, [||]) :?> IDataReader)
        | _, _ ->
            match tryReadValueProperty parameter.Value with
            | Some(obj) -> Native(obj)
            | _ -> Native(parameter.Value)
    
    let getPrimaryKeys con =
        let indexColumns = 
            getSchema "IndexColumns" [|owner|] con
            |> DataTable.map (fun row -> dbUnbox row.[1], dbUnbox row.[4])
            |> Map.ofList
        
        getSchema "PrimaryKeys" [|owner|] con
        |> DataTable.mapChoose (fun row ->
            let indexName = dbUnbox row.[15]
            let tableName = dbUnbox row.[2]
            match Map.tryFind indexName indexColumns with
            | Some(column) -> 
                let pk = { Name = unbox row.[1]; Table = tableName; Column = column; IndexName = indexName }
                Some(tableName, pk)
            | None -> None)

    let getTables con = 
        getSchema "Tables" [|owner|] con
        |> DataTable.map (fun row -> 
                              let name = dbUnbox row.[1]
                              { Schema = dbUnbox row.[0]; Name = name; Type = dbUnbox row.[2] })

    let getColumns (primaryKeys:IDictionary<_,_>) table con = 
        getSchema "Columns" [|owner; table|] con
        |> DataTable.mapChoose (fun row -> 
                let typ = dbUnbox row.[4]
                let nullable = (dbUnbox row.[8]) = "Y"
                let colName =  (dbUnbox row.[2])
                match findDbType typ with
                | Some(m) -> 
                        { Name = colName
                          TypeMapping = m
                          IsPrimarKey = primaryKeys.Values |> Seq.exists (fun x -> x.Table = table && x.Column = colName)
                          IsNullable = nullable } |> Some
                | _ -> None)

    let getRelationships (primaryKeys:IDictionary<_,_>) table con =
        let foreignKeyCols = 
            getSchema "ForeignKeyColumns" [|owner;table|] con
            |> DataTable.map (fun row -> (dbUnbox row.[1], dbUnbox row.[3])) 
            |> Map.ofList
        let rels = 
            getSchema "ForeignKeys" [|owner;table|] con
            |> DataTable.mapChoose (fun row -> 
                let name = dbUnbox row.[4]
                let pkName = dbUnbox row.[0]
                match primaryKeys.TryGetValue(table) with
                | true, pk ->
                    match foreignKeyCols.TryFind name with
                    | Some(fk) ->
                         { Name = name 
                           PrimaryTable = Table.CreateFullName(owner,dbUnbox row.[2]) 
                           PrimaryKey = pk.Column
                           ForeignTable = Table.CreateFullName(owner,dbUnbox row.[5])
                           ForeignKey = fk } |> Some
                    | None -> None
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

    let getIndivdualsQueryText amount table = 
        sprintf "select * from ( select * from %s order by 1 desc) where ROWNUM <= %i" (tableFullName table) amount

    let getIndivdualQueryText table column = 
        let tName = tableFullName table
        sprintf "SELECT * FROM %s WHERE %s.%s = :id" tName tName (quoteWhiteSpace column)

    let getSprocs con =

        let functions = getSchema "Functions" [|owner|] con |> DataTable.map (fun row -> dbUnbox<string> row.["OBJECT_NAME"]) |> Set.ofList
        let procedures = getSchema "Procedures" [|owner|] con |> DataTable.map (fun row -> dbUnbox<string> row.["OBJECT_NAME"]) |> Set.ofList

        let getName (row:DataRow) = 
            let owner = dbUnbox row.["OWNER"]
            let (procName, packageName) = (dbUnbox row.["OBJECT_NAME"], dbUnbox row.["PACKAGE_NAME"])
            { ProcName = procName; Owner = owner; PackageName = packageName }

        let createSprocParameters (row:DataRow) = 
            let dataType = dbUnbox row.["DATA_TYPE"]
            let argumentName = dbUnbox row.["ARGUMENT_NAME"]
            let maxLength = Some(int(dbUnboxWithDefault<decimal> -1M row.["DATA_LENGTH"]))

            findDbType dataType 
            |> Option.map (fun m ->
                let direction = 
                    match dbUnbox row.["IN_OUT"] with
                    | "IN" -> ParameterDirection.Input
                    | "OUT" when String.IsNullOrEmpty(argumentName) -> ParameterDirection.ReturnValue
                    | "OUT" -> ParameterDirection.Output
                    | "IN/OUT" -> ParameterDirection.InputOutput
                    | a -> failwithf "Direction not supported %s" a
                { Name = dbUnbox row.["ARGUMENT_NAME"]
                  TypeMapping = m
                  Direction = direction
                  Length = maxLength
                  Ordinal = int(dbUnbox<decimal> row.["POSITION"]) }
            )

        let parameters = 
            let withParameters = getSchema "ProcedureParameters" [|owner|] con |> DataTable.groupBy (fun row -> getName row, createSprocParameters row)
            (Set.union procedures functions)
            |> Set.toSeq 
            |> Seq.choose (fun proc -> 
                if withParameters |> Seq.exists (fun (name,_) -> name.ProcName = proc)
                then None
                else Some({ ProcName = proc; Owner = owner; PackageName = String.Empty }, Seq.empty)
                )
            |> Seq.append withParameters

        parameters
        |> Seq.map (fun (name, parameters) -> 
                        let sparams = 
                            parameters
                            |> Seq.choose id
                            |> Seq.sortBy (fun p -> p.Ordinal)
                            |> Seq.toList
                            
                        let retCols = 
                            sparams
                            |> List.filter (fun x -> x.Direction <> ParameterDirection.Input)
                            |> List.mapi (fun i p -> { Name = (if (String.IsNullOrEmpty p.Name) && (i > 0)
                                                               then "ReturnValue" + (string i)
                                                               elif (String.IsNullOrEmpty p.Name)
                                                               then "ReturnValue"
                                                               else p.Name); 
                                                       TypeMapping = p.TypeMapping; 
                                                       Direction = p.Direction; 
                                                       Ordinal = p.Ordinal;
                                                       Length = None 
                                                     })
                        
                        match Set.contains name.ProcName functions, Set.contains name.ProcName procedures with
                        | true, false -> Root("Functions", Sproc({ Name = name; Params = sparams; ReturnColumns = retCols }))
                        | false, true ->  Root("Procedures", Sproc({ Name = name; Params = sparams; ReturnColumns = retCols }))
                        | _, _ ->  Root("Packages", SprocPath(name.PackageName, Sproc({ Name = name; Params = sparams; ReturnColumns = retCols })))
                      ) 
        |> Seq.toList


type internal OracleProvider(resolutionPath, owner) =
    
    let mutable primaryKeyCache = new Dictionary<string,PrimaryKey>()
    let relationshipCache = new Dictionary<string, Relationship list * Relationship list>()
    let columnCache = new Dictionary<string, Column list>()
    let mutable tableCache : Table list = []

    do
        Oracle.owner <- owner
        Oracle.resolutionPath <- resolutionPath
    
    interface ISqlProvider with
        member __.CreateConnection(connectionString) = Oracle.createConnection connectionString
        member __.CreateCommand(connection,commandText) =  Oracle.createCommand commandText connection
        member __.ReadDatabaseParameter(reader:IDataReader, parameter:IDbDataParameter) = Oracle.readParameter reader parameter
        member __.CreateCommandParameter(param, value) = Oracle.createCommandParameter param value

        member __.CreateTypeMappings(con) = 
            Oracle.connect con (fun con -> 
                Oracle.createTypeMappings con
                primaryKeyCache <- downcast (dict (Oracle.getPrimaryKeys con))
            )

        member __.GetTables(con) =
               match tableCache with
               | [] ->
                    let tables = Oracle.connect con Oracle.getTables
                    tableCache <- tables
                    tables
                | a -> a

        member __.GetPrimaryKey(table) = 
            match primaryKeyCache.TryGetValue table.Name with 
            | true, v -> Some v.Column
            | _ -> None

        member __.GetColumns(con,table) = 
            match columnCache.TryGetValue table.FullName  with
            | true, cols -> cols
            | false, _ ->
                let cols = Oracle.connect con (Oracle.getColumns primaryKeyCache table.Name)
                columnCache.Add(table.FullName, cols)
                cols

        member __.GetRelationships(con,table) =
                match relationshipCache.TryGetValue(table.FullName) with
                | true, rels -> rels
                | false, _ ->
                    let rels = Oracle.connect con (Oracle.getRelationships primaryKeyCache table.Name)
                    relationshipCache.Add(table.FullName, rels)
                    rels
        
        member __.GetSprocs(con) = Oracle.connect con Oracle.getSprocs

        member __.GetIndividualsQueryText(table,amount) = Oracle.getIndivdualsQueryText amount table

        member __.GetIndividualQueryText(table,column) = Oracle.getIndivdualQueryText table column

        member this.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns) =
            let sb = System.Text.StringBuilder()
            let parameters = ResizeArray<_>()
            let (~~) (t:string) = sb.Append t |> ignore
            
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
                            for col in columnCache.[baseTable.FullName] |> List.map(fun c -> c.Name) do 
                                if singleEntity then yield sprintf "%s.%s as \"%s\"" k col col
                                else yield sprintf "%s.%s as \"%s.%s\"" k col k col
                        else
                            for col in v do 
                                if singleEntity then yield sprintf "%s.%s as \"%s\"" k (Oracle.quoteWhiteSpace col) col
                                else yield sprintf "%s.%s as \"%s.%s\"" k (Oracle.quoteWhiteSpace col) k col|]) // F# makes this so easy :)
        
            // next up is the filter expressions
            // NOTE: really need to assign the parameters their correct db types
            let param = ref 0
            let nextParam() =
                incr param
                sprintf ":param%i" !param

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
                                     | Some(x) when (box x :? string array) -> 
                                         // in and not in operators pass an array
                                         let strings = box x :?> string array
                                         strings |> Array.map createParam
                                     | Some(x) -> [|createParam (box x)|]
                                     | None ->    [|createParam null|]

                                let prefix = if i>0 then (sprintf " %s " op) else ""
                                let paras = extractData data
                                ~~(sprintf "%s%s" prefix <|
                                    match operator with
                                    | FSharp.Data.Sql.IsNull -> (sprintf "%s.%s IS NULL") alias (Oracle.quoteWhiteSpace col) 
                                    | FSharp.Data.Sql.NotNull -> (sprintf "%s.%s IS NOT NULL") alias (Oracle.quoteWhiteSpace col) 
                                    | FSharp.Data.Sql.In ->                                     
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "%s.%s IN (%s)") alias (Oracle.quoteWhiteSpace col) text
                                    | FSharp.Data.Sql.NotIn ->                                    
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "%s.%s NOT IN (%s)") alias (Oracle.quoteWhiteSpace col) text 
                                    | _ -> 
                                        parameters.Add paras.[0]
                                        (sprintf "%s.%s %s %s") alias (Oracle.quoteWhiteSpace col) 
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
                    ~~  (sprintf "%s %s %s on %s.%s = %s.%s " 
                            joinType (Oracle.tableFullName destTable) destAlias 
                            (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias)
                            data.ForeignKey  
                            (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias) 
                            data.PrimaryKey))

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) -> 
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "%s.%s%s" alias (Oracle.quoteWhiteSpace column) (if not desc then " DESC NULLS LAST" else " ASC NULLS FIRST")))

            // SELECT
            if sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s " columns)
            elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
            else  ~~(sprintf "SELECT %s " columns)
            // FROM
            ~~(sprintf "FROM %s %s " (Oracle.tableFullName baseTable) baseAlias)         
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

        member this.ProcessUpdates(con, entities) =
            let sb = Text.StringBuilder()
            let provider = this :> ISqlProvider
            let (~~) (t:string) = sb.Append t |> ignore

            // ensure columns have been loaded
            entities |> List.map(fun e -> e.Table) 
                     |> Seq.distinct 
                     |> Seq.iter(fun t -> provider.GetColumns(con,t) |> ignore )

            con.Open()

            let createInsertCommand (entity:SqlEntity) =     
                let pk = primaryKeyCache.[entity.Table.Name] 
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
                    (Oracle.tableFullName entity.Table)
                    (String.Join(",",columnNames))
                    (String.Join(",",values |> Array.map(fun p -> p.ParameterName))))
                let cmd = provider.CreateCommand(con, sb.ToString())
                values |> Array.iter (cmd.Parameters.Add >> ignore)
                cmd

            let createUpdateCommand (entity:SqlEntity) changedColumns =
                let pk = primaryKeyCache.[entity.Table.Name] 
                sb.Clear() |> ignore

                if changedColumns |> List.exists ((=)pk.Column) then failwith "Error - you cannot change the primary key of an entity."

                let pkValue = 
                    match entity.GetColumnOption<obj> pk.Column with
                    | Some v -> v
                    | None -> failwith "Error - you cannot update an entity that does not have a primary key."
                
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
                
                let pkParam = provider.CreateCommandParameter(QueryParameter.Create(":pk",0), pkValue)

                ~~(sprintf "UPDATE %s SET (%s) = (%s) WHERE %s = :pk" 
                    (Oracle.tableFullName entity.Table)
                    (String.Join(",", columns))
                    (String.Join(",", parameters |> Array.map (fun p -> p.ParameterName)))
                    pk.Column)
                
                let cmd = provider.CreateCommand(con, sb.ToString())
                parameters |> Array.iter (cmd.Parameters.Add >> ignore)
                cmd.Parameters.Add pkParam |> ignore
                cmd
            
            let createDeleteCommand (entity:SqlEntity) =
                let pk = primaryKeyCache.[entity.Table.Name] 
                sb.Clear() |> ignore
                let pkValue = 
                    match entity.GetColumnOption<obj> pk.Column with
                    | Some v -> v
                    | None -> failwith "Error - you cannot delete an entity that does not have a primary key."
                ~~(sprintf "DELETE FROM %s WHERE %s = :id" (Oracle.tableFullName entity.Table) pk.Column )
                let cmd = provider.CreateCommand(con, sb.ToString())
                cmd.CommandType <- CommandType.Text
                let pkType = pkValue.GetType().ToString();
                match Oracle.findClrType pkType with
                | Some(m) ->
                    cmd.Parameters.Add(provider.CreateCommandParameter(QueryParameter.Create(":id",0, m), pkValue)) |> ignore
                | None -> ()
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
                        match e.GetColumnOption primaryKeyCache.[e.Table.Name].Column with
                        | Some v -> () // if the primary key exists, do nothing
                                       // this is because non-identity columns will have been set 
                                       // manually and in that case scope_identity would bring back 0 "" or whatever
                        | None ->  e.SetColumnSilent(primaryKeyCache.[e.Table.Name].Column, id)
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
                        e.SetColumnOptionSilent(primaryKeyCache.[e.Table.Name].Column, None)
                    | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!")
                scope.Complete()
            finally
                con.Close()
