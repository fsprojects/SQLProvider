namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Data
open FSharp.Data.Sql
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Common.Utilities

module internal Oracle =
    let mutable resolutionPath = String.Empty
    let mutable referencedAssemblies : string array = [||]
    let mutable owner = String.Empty

    let assemblyNames =
        [
            "Oracle.ManagedDataAccess.dll"
            "Oracle.DataAccess.dll"
        ]

    let assembly =
        lazy Reflection.tryLoadAssemblyFrom resolutionPath referencedAssemblies assemblyNames

    let findType name =
        match assembly.Value with
        | Choice1Of2(assembly) -> assembly.GetTypes() |> Array.find(fun t -> t.Name = name)
        | Choice2Of2(paths, errors) ->
           let details = 
                match errors with 
                | [] -> "" 
                | x -> Environment.NewLine + "Details: " + Environment.NewLine + String.Join(Environment.NewLine, x)
           failwithf "Unable to resolve assemblies. One of %s must exist in the paths: %s %s %s"
                (String.Join(", ", assemblyNames |> List.toArray))
                Environment.NewLine
                (String.Join(Environment.NewLine, paths |> Seq.filter(fun p -> not(String.IsNullOrEmpty p))))
                details

    let systemNames =
        [
            "SYSTEM"; "SYS"; "XDB"
        ]

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
                let dt = getSchema "DataTypes" [||] con
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

    let createConnection connectionString =
        try
            Activator.CreateInstance(connectionType.Value,[|box connectionString|]) :?> IDbConnection
        with
        | :? System.Reflection.ReflectionTypeLoadException as ex ->
            let errorfiles = ex.LoaderExceptions |> Array.map(fun e -> e.Message) |> Seq.distinct |> Seq.toArray
            let msg = ex.Message + "\r\n" + String.Join("\r\n", errorfiles)
            raise(new System.Reflection.TargetInvocationException(msg, ex))
        | :? System.Reflection.TargetInvocationException as ex when (ex.InnerException <> null && ex.InnerException :? DllNotFoundException) ->
            let msg = ex.InnerException.Message + ", Path: " + (System.IO.Path.GetFullPath resolutionPath)
            raise(new System.Reflection.TargetInvocationException(msg, ex))

    let createCommand commandText connection =
        Activator.CreateInstance(commandType.Value,[|box commandText;box connection|]) :?> IDbCommand

    let createCommandParameter (param:QueryParameter) value =
        let value = if value = null then (box System.DBNull.Value) else value
        let parameterType = parameterType.Value
        let oracleDbTypeSetter =
            parameterType.GetProperty("OracleDbType").GetSetMethod()

        let p = Activator.CreateInstance(parameterType,[|box param.Name; box value|]) :?> IDbDataParameter
        p.Direction <- param.Direction

        match param.TypeMapping.ProviderTypeName with
        | Some _ ->
            p.DbType <- param.TypeMapping.DbType
            param.TypeMapping.ProviderType |> Option.iter (fun pt -> oracleDbTypeSetter.Invoke(p, [|pt|]) |> ignore)
        | None -> ()

        match param.Length with
        | Some(length) when length >= 0 -> p.Size <- length
        | _ ->
               match param.TypeMapping.DbType with
               | DbType.String -> p.Size <- 32767
               | _ -> ()
        p

    let readParameter (parameter:IDbDataParameter) =
        let parameterType = parameterType.Value
        let oracleDbTypeGetter =
            parameterType.GetProperty("OracleDbType").GetGetMethod()

        match parameter.DbType, (oracleDbTypeGetter.Invoke(parameter, [||]) :?> int) with
        | DbType.Object, 121 ->
             if parameter.Value = null
             then null
             else
                let data =
                    Sql.dataReaderToArray (getDataReaderForRefCursor.Value.Invoke(parameter.Value, [||]) :?> IDataReader)
                    |> Seq.ofArray
                data |> box
        | _, _ ->
            match tryReadValueProperty parameter.Value with
            | Some(obj) -> obj |> box
            | _ -> parameter.Value |> box

    let buildTableNameWhereFilter columnName (tableNames : string) =
        let trim (s:string) = s.Trim()
        let names = tableNames.Split([|","|], StringSplitOptions.RemoveEmptyEntries)
                    |> Seq.map trim
                    |> Seq.toArray
        match names with
        | [||] -> ""
        | [|name|] -> sprintf "and %s like '%s'" columnName name
        | _ -> names |> Array.map (sprintf "%s like '%s'" columnName)
                     |> String.concat " or "
                     |> sprintf "and (%s)"

    let read conn f sql =
      seq { use cmd = createCommand sql conn
            use reader = cmd.ExecuteReader()
            while reader.Read()
              do yield f reader }

    let getIndexColumns tableNames conn = 
        let whereTableName = buildTableNameWhereFilter "table_name" tableNames
        let whereNotSystemOwner = 
          systemNames 
          |> List.map (sprintf "'%s'") 
          |> String.concat ","
          |> sprintf "index_owner not in (%s)"

        sprintf """select index_name, column_name
                   from all_ind_columns
                   where %s
                   %s'""" whereNotSystemOwner whereTableName
        |> read conn (fun row -> row.[0], row.[1]) 
        |> dict
               
    let getPrimaryKeys tableNames conn =
        let whereTableName = buildTableNameWhereFilter "a.table_name" tableNames
        sprintf """select c.constraint_name, a.table_name, a.column_name, c.index_name 
                   from all_cons_columns a
                   join all_constraints c on a.constraint_name = c.constraint_name
                   where c.constraint_type = 'P' and a.table_name not like 'BIN$%%'
                   %s""" whereTableName
        |> read conn (fun row -> 
            let pkName     = Sql.dbUnbox row.[0]
            let tableName  = Sql.dbUnbox row.[1]
            let columnName = Sql.dbUnbox row.[2]
            let indexName  = Sql.dbUnbox row.[3]
            tableName, { PrimaryKey.Name = pkName
                         Table = tableName
                         Column = columnName
                         IndexName = indexName })
        |> dict

    let getTables tableNames conn = 
        let whereTableName = buildTableNameWhereFilter "table_name" tableNames
        sprintf """select owner, table_name, tablespace_name
                   from all_tables
                   where owner != 'System'
                   %s""" whereTableName
        |> read conn (fun row -> 
            { Schema = Sql.dbUnbox row.[0];
              Name   = Sql.dbUnbox row.[1];
              Type   = Sql.dbUnbox row.[2] })
        |> Seq.toList

    let getColumns (primaryKeys:IDictionary<_,_>) tableName conn = 
        sprintf """select data_type, nullable, column_name from all_tab_columns where table_name = '%s'""" tableName
        |> read conn (fun row ->
                let columnType = Sql.dbUnbox row.[0]
                let nullable   = (Sql.dbUnbox row.[1]) = "Y"
                let columnName = Sql.dbUnbox row.[2]
                findDbType columnType
                |> Option.map (fun m ->
                    { Name = columnName
                      TypeMapping = m
                      IsPrimaryKey = primaryKeys.Values |> Seq.exists (fun x -> x.Table = tableName && x.Column = [columnName])
                      IsNullable = nullable }
                ))
        |> Seq.choose id
        |> Seq.map (fun c -> c.Name, c)
        |> Map.ofSeq

    let getRelationships (primaryKeys:IDictionary<_,_>) table con =
        let foreignKeyCols =
            getSchema "ForeignKeyColumns" [|owner;table|] con
            |> DataTable.map (fun row -> (Sql.dbUnbox row.[1], Sql.dbUnbox row.[3]))
            |> Map.ofList
        let rels =
            getSchema "ForeignKeys" [|owner;table|] con
            |> DataTable.mapChoose (fun row ->
                let name = Sql.dbUnbox row.[4]
                match primaryKeys.TryGetValue(table) with
                | true, pks ->
                    match pks.Column, foreignKeyCols.TryFind name with
                    | [pk], Some(fk) ->
                         { Name = name
                           PrimaryTable = Table.CreateFullName(Sql.dbUnbox row.[1],Sql.dbUnbox row.[2])
                           PrimaryKey = pk
                           ForeignTable = Table.CreateFullName(Sql.dbUnbox row.[3],Sql.dbUnbox row.[5])
                           ForeignKey = fk } |> Some
                    | _, Some(fk) -> None
                    | _, None -> None
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

    let getIndivdualsQueryText amount (table:Table) =
        sprintf "select * from ( select * from %s order by 1 desc) where ROWNUM <= %i" table.FullName amount

    let getIndivdualQueryText (table:Table) column =
        let tName = table.FullName
        sprintf "SELECT * FROM %s WHERE %s.%s = :id" tName tName (quoteWhiteSpace column)

    let getSprocReturnColumns (sparams: QueryParameter list) =
        sparams
        |> List.filter (fun x -> x.Direction <> ParameterDirection.Input)
        |> List.mapi (fun i p ->
            let name = if (String.IsNullOrEmpty p.Name) && (i > 0) then "ReturnValue" + (string i)
                       elif (String.IsNullOrEmpty p.Name) then "ReturnValue"
                       else p.Name
            QueryParameter.Create(name,p.Ordinal,p.TypeMapping,p.Direction))

    let getSprocName (row:DataRow) =
        let owner = Sql.dbUnbox row.["OWNER"]
        let procName = Sql.dbUnbox row.["OBJECT_NAME"]
        let packageName = 
            match row.Table.Columns.Contains("PACKAGE_NAME") with
            | true -> Sql.dbUnbox row.["PACKAGE_NAME"]
            | false -> ""
        { ProcName = procName; Owner = owner; PackageName = packageName }

    let getSprocParameters (con:IDbConnection) (name:SprocName) =
        let createSprocParameters (row:DataRow) =
           let dataType = Sql.dbUnbox row.["DATA_TYPE"]
           let argumentName = Sql.dbUnbox row.["ARGUMENT_NAME"]
           let maxLength = Some(int(Sql.dbUnboxWithDefault<decimal> -1M row.["DATA_LENGTH"]))

           findDbType dataType
           |> Option.map (fun m ->
               let direction =
                   match Sql.dbUnbox row.["IN_OUT"] with
                   | "IN" -> ParameterDirection.Input
                   | "OUT" when String.IsNullOrEmpty(argumentName) -> ParameterDirection.ReturnValue
                   | "OUT" -> ParameterDirection.Output
                   | "IN/OUT" -> ParameterDirection.InputOutput
                   | a -> failwithf "Direction not supported %s" a
               { Name = Sql.dbUnbox row.["ARGUMENT_NAME"]
                 TypeMapping = m
                 Direction = direction
                 Length = maxLength
                 Ordinal = int(Sql.dbUnbox<decimal> row.["POSITION"]) }
           )
        getSchema "ProcedureParameters" [|owner|] con
        |> DataTable.groupBy (fun row -> getSprocName row, createSprocParameters row)
        |> Seq.filter (fun (n, _) -> n.ProcName = name.ProcName)
        |> Seq.collect (snd >> Seq.choose id)
        |> Seq.sortBy (fun x -> x.Ordinal)
        |> Seq.toList

    let getSprocs con =

        let buildDef classType row =
            let name = getSprocName row
            Root(classType, Sproc({ Name = name; Params = (fun con -> getSprocParameters con name); ReturnColumns = (fun _ sparams -> getSprocReturnColumns sparams) }))

        let functions =
            (getSchema "Functions" [|owner|] con)
            |> DataTable.map (fun row -> buildDef "Functions" row)

        let procs =
            (getSchema "Procedures" [|owner|] con)
            |> DataTable.map (fun row -> buildDef "Procedures" row)

        functions @ procs

    let executeSprocCommand (com:IDbCommand) (inputParameters:QueryParameter[]) (retCols:QueryParameter[]) (values:obj[]) =
        let inputParameters = inputParameters |> Array.filter (fun p -> p.Direction = ParameterDirection.Input)

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

        let entities =
            match retCols with
            | [||] -> com.ExecuteNonQuery() |> ignore; Unit
            | [|col|] ->
                use reader = com.ExecuteReader()
                match col.TypeMapping.ProviderTypeName with
                | Some "REF CURSOR" -> SingleResultSet(col.Name, Sql.dataReaderToArray reader)
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
                            | Some "REF CURSOR" -> ResultSet(col.Name, readParameter p :?> ResultSet)
                            | _ -> ScalarResultSet(col.Name, readParameter p)
                        | None -> failwithf "Excepted return column %s but could not find it in the parameter set" col.Name
                    )
                Set(returnValues)
        entities

type internal OracleProvider(resolutionPath, owner, referencedAssemblies, tableNames) =
    let mutable primaryKeyColumn : IDictionary<string,PrimaryKey> = null
    let relationshipCache = new ConcurrentDictionary<string, Relationship list * Relationship list>()
    let columnCache = new ConcurrentDictionary<string,ColumnLookup>()
    let mutable tableCache : Table list = []

    let isPrimaryKey tableName columnName = 
        match primaryKeyColumn.TryGetValue tableName with
        | true, pk when pk.Column = [columnName] -> true
        | _ -> false

    let createInsertCommand (provider:ISqlProvider) (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore

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
            (entity.Table.FullName)
            (String.Join(",",columnNames))
            (String.Join(",",values |> Array.map(fun p -> p.ParameterName))))
        let cmd = provider.CreateCommand(con, sb.ToString())
        values |> Array.iter (cmd.Parameters.Add >> ignore)
        cmd

    let createUpdateCommand (provider:ISqlProvider) (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) (changedColumns: string list) =
        let (~~) (t:string) = sb.Append t |> ignore
        sb.Clear() |> ignore

        if changedColumns |> List.exists (isPrimaryKey entity.Table.Name) 
        then failwith "Error - you cannot change the primary key of an entity."

        let pk = primaryKeyColumn.[entity.Table.Name]
        let pkValues =
            match entity.GetPkColumnOption<obj> pk.Column with
            | [] -> failwith ("Error - you cannot update an entity that does not have a primary key. (" + entity.Table.FullName + ")")
            | v -> v

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

        match pk.Column with
        | [] -> ()
        | ks -> 
            ~~(sprintf "UPDATE %s SET (%s) = (%s) WHERE "
                (entity.Table.FullName)
                (String.Join(",", columns))
                (String.Join(",", parameters |> Array.map (fun p -> p.ParameterName))))
            ~~(String.Join(" AND ", ks |> List.mapi(fun i k -> (sprintf "%s = :pk%i" k i))))

        let cmd = provider.CreateCommand(con, sb.ToString())
        parameters |> Array.iter (cmd.Parameters.Add >> ignore)
        pkValues |> List.iteri(fun i pkValue ->
            let pkParam = provider.CreateCommandParameter(QueryParameter.Create((":pk"+i.ToString()),i), pkValue)
            cmd.Parameters.Add pkParam |> ignore
        )
        cmd

    let createDeleteCommand (provider:ISqlProvider) (con:IDbConnection) (sb:Text.StringBuilder) (entity:SqlEntity) =
        let (~~) (t:string) = sb.Append t |> ignore
        let pk = primaryKeyColumn.[entity.Table.Name]
        sb.Clear() |> ignore
        let pkValues =
            match entity.GetPkColumnOption<obj> pk.Column with
            | [] -> failwith ("Error - you cannot delete an entity that does not have a primary key. (" + entity.Table.FullName + ")")
            | v -> v

        match pk.Column with
        | [] -> ()
        | ks -> 
            ~~(sprintf "DELETE FROM %s WHERE " entity.Table.FullName)
            ~~(String.Join(" AND ", ks |> List.mapi(fun i k -> (sprintf "%s = :id%i" k i))))

        let cmd = provider.CreateCommand(con, sb.ToString())
        cmd.CommandType <- CommandType.Text
        pkValues |> List.iteri(fun i pkValue ->
            let pkType = pkValue.GetType().ToString();
            match Oracle.findClrType pkType with
            | Some(m) ->
                cmd.Parameters.Add(provider.CreateCommandParameter(QueryParameter.Create((":id"+i.ToString()),i, m), pkValue)) |> ignore
            | None -> ())
        cmd

    do
        Oracle.owner <- owner
        Oracle.referencedAssemblies <- referencedAssemblies
        Oracle.resolutionPath <- resolutionPath

    interface ISqlProvider with
        member __.CreateConnection(connectionString) = Oracle.createConnection connectionString
        member __.CreateCommand(connection,commandText) =  Oracle.createCommand commandText connection
        member __.CreateCommandParameter(param, value) = Oracle.createCommandParameter param value
        member __.ExecuteSprocCommand(con, param ,retCols, values:obj array) = Oracle.executeSprocCommand con param retCols values

        member __.CreateTypeMappings(con) =
            Sql.connect con (fun con ->
                Oracle.createTypeMappings con
                primaryKeyColumn <- (Oracle.getPrimaryKeys tableNames con))

        member __.GetTables(con,_) =
               match tableCache with
               | [] ->
                    let tables = Sql.connect con (Oracle.getTables tableNames)
                    tableCache <- tables
                    tables
                | a -> a

        member __.GetPrimaryKey(table) =
            match primaryKeyColumn.TryGetValue table.Name with
            | true, v -> match v.Column with [x] -> Some(x) | _ -> None
            | _ -> None

        member __.GetColumns(con,table) =
            match columnCache.TryGetValue table.FullName  with
            | true, cols when cols.Count > 0 -> cols
            | _ ->
                let cols = Sql.connect con (Oracle.getColumns primaryKeyColumn table.Name)
                columnCache.GetOrAdd(table.FullName, cols)

        member __.GetRelationships(con,table) =
            relationshipCache.GetOrAdd(table.FullName, fun name ->
                    let rels = Sql.connect con (Oracle.getRelationships primaryKeyColumn table.Name)
                    rels)

        member __.GetSprocs(con) = Sql.connect con Oracle.getSprocs
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
            let selectcolumns =
                if projectionColumns |> Seq.isEmpty then "1" else
                String.Join(",",
                    [|for KeyValue(k,v) in projectionColumns do
                        if v.Count = 0 then   // if no columns exist in the projection then get everything
                            for col in columnCache.[baseTable.FullName] |> Seq.map (fun c -> c.Key) do
                                if singleEntity then yield sprintf "%s.%s as \"%s\"" k col col
                                else yield sprintf "%s.%s as \"%s.%s\"" k col k col
                        else
                            for col in v do
                                if singleEntity then yield sprintf "%s.%s as \"%s\"" k (quoteWhiteSpace col) col
                                else yield sprintf "%s.%s as \"%s.%s\"" k (quoteWhiteSpace col) k col|]) // F# makes this so easy :)

            // Create sumBy, minBy, maxBy, ... field columns
            let columns =
                let extracolumns =
                    let fieldNotation(al:alias,col:string) =
                        match String.IsNullOrEmpty(al) with
                        | true -> sprintf "%s" col
                        | false -> sprintf "%s.%s" al col
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
                                     | Some(x) when (box x :? System.Linq.IQueryable) -> [||]
                                     | Some(x) when (box x :? obj array) ->
                                         // in and not in operators pass an array
                                         let elements = box x :?> obj array
                                         Array.init (elements.Length) (fun i -> createParam (elements.GetValue(i)))
                                     | Some(x) -> [|createParam (box x)|]
                                     | None ->    [|createParam null|]

                                let prefix = if i>0 then (sprintf " %s " op) else ""
                                let paras = extractData data
                                ~~(sprintf "%s%s" prefix <|
                                    match operator with
                                    | FSharp.Data.Sql.IsNull -> (sprintf "%s.%s IS NULL") alias (quoteWhiteSpace col)
                                    | FSharp.Data.Sql.NotNull -> (sprintf "%s.%s IS NOT NULL") alias (quoteWhiteSpace col)
                                    | FSharp.Data.Sql.In ->
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "%s.%s IN (%s)") alias (quoteWhiteSpace col) text
                                    | FSharp.Data.Sql.NestedIn ->
                                        let innersql, innerpars = data.Value |> box :?> string * IDbDataParameter[]
                                        Array.iter parameters.Add innerpars
                                        (sprintf "%s.%s IN (%s)") alias (quoteWhiteSpace col) innersql
                                    | FSharp.Data.Sql.NotIn ->
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "%s.%s NOT IN (%s)") alias (quoteWhiteSpace col) text
                                    | FSharp.Data.Sql.NestedNotIn ->
                                        let innersql, innerpars = data.Value |> box :?> string * IDbDataParameter[]
                                        Array.iter parameters.Add innerpars
                                        (sprintf "%s.%s NOT IN (%s)") alias (quoteWhiteSpace col) innersql
                                    | _ ->
                                        parameters.Add paras.[0]
                                        (sprintf "%s.%s %s %s") alias (quoteWhiteSpace col)
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
                            joinType destTable.FullName destAlias
                            (if data.RelDirection = RelationshipDirection.Parents then fromAlias else destAlias)
                            data.ForeignKey
                            (if data.RelDirection = RelationshipDirection.Parents then destAlias else fromAlias)
                            data.PrimaryKey))

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) ->
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "%s.%s%s" alias (quoteWhiteSpace column) (if not desc then " DESC NULLS LAST" else " ASC NULLS FIRST")))

            // SELECT
            if sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s " columns)
            elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
            else  ~~(sprintf "SELECT %s " columns)
            // FROM
            ~~(sprintf "FROM %s %s " baseTable.FullName baseAlias)
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

            CommonTasks.``ensure columns have been loaded`` (this :> ISqlProvider) con entities

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
                        let cmd = createInsertCommand provider con sb e
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        let id = cmd.ExecuteScalar()
                        match e.GetPkColumnOption primaryKeyColumn.[e.Table.Name].Column with
                        | [] ->  e.SetPkColumnSilent(primaryKeyColumn.[e.Table.Name].Column, id)
                        | _ -> () // if the primary key exists, do nothing
                                        // this is because non-identity columns will have been set
                                        // manually and in that case scope_identity would bring back 0 "" or whatever
                        e._State <- Unchanged
                    | Modified fields ->
                        let cmd = createUpdateCommand provider con sb e fields
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        cmd.ExecuteNonQuery() |> ignore
                        e._State <- Unchanged
                    | Delete ->
                        let cmd = createDeleteCommand provider con sb e
                        Common.QueryEvents.PublishSqlQuery cmd.CommandText
                        cmd.ExecuteNonQuery() |> ignore
                        // remove the pk to prevent this attempting to be used again
                        e.SetPkColumnOptionSilent(primaryKeyColumn.[e.Table.Name].Column, None)
                        e._State <- Deleted
                    | Deleted | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!")
                scope.Complete()

            finally
                con.Close()

        member this.ProcessUpdatesAsync(con, entities) =
            let sb = Text.StringBuilder()
            let provider = this :> ISqlProvider

            CommonTasks.``ensure columns have been loaded`` (this :> ISqlProvider) con entities

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
                                let cmd = createInsertCommand provider con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                let! id = cmd.ExecuteScalarAsync() |> Async.AwaitTask
                                match e.GetPkColumnOption primaryKeyColumn.[e.Table.Name].Column with
                                | [] ->  e.SetPkColumnSilent(primaryKeyColumn.[e.Table.Name].Column, id)
                                | _ -> () // if the primary key exists, do nothing
                                                // this is because non-identity columns will have been set
                                                // manually and in that case scope_identity would bring back 0 "" or whatever
                                e._State <- Unchanged
                            }
                        | Modified fields ->
                            async {
                                let cmd = createUpdateCommand provider con sb e fields :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                e._State <- Unchanged
                            }
                        | Delete ->
                            async {
                                let cmd = createDeleteCommand provider con sb e :?> System.Data.Common.DbCommand
                                Common.QueryEvents.PublishSqlQuery cmd.CommandText
                                do! cmd.ExecuteNonQueryAsync() |> Async.AwaitTask |> Async.Ignore
                                // remove the pk to prevent this attempting to be used again
                                e.SetPkColumnOptionSilent(primaryKeyColumn.[e.Table.Name].Column, None)
                                e._State <- Deleted
                            }
                        | Deleted | Unchanged -> failwith "Unchanged entity encountered in update list - this should not be possible!"

                    do! Utilities.executeOneByOne handleEntity (entities.Keys|>Seq.toList)
                    scope.Complete()

                finally
                    con.Close()
            }
