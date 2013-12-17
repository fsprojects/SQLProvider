namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Generic
open System.Data
open System.Data.SqlClient
open FSharp.Data.Sql
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common

type internal MSSqlServerProvider() =
    // note we intentionally do not hang onto a connection object at any time,
    // as the type provider will dicate the connection lifecycles 
    let pkLookup =     Dictionary<string,string>()
    let tableLookup =  Dictionary<string,Table>()
    let columnLookup = Dictionary<string,Column list>()
    let relationshipLookup = Dictionary<string,Relationship list * Relationship list>()

    let mutable clrToEnum : (string -> DbType option)  = fun _ -> failwith "!"
    let mutable sqlToEnum : (string -> DbType option)  = fun _ -> failwith "!"
    let mutable sqlToClr :  (string -> Type option)       = fun _ -> failwith "!"

    let createTypeMappings (con:SqlConnection) =
        let clr = 
            [for r in con.GetSchema("DataTypes").Rows -> 
                string r.["TypeName"],  unbox<int> r.["ProviderDbType"], string r.["DataType"]]

        // create map from sql name to clr type, and type to lDbType enum
        let sqlToClr', sqlToEnum', clrToEnum' =
            clr
            |> List.choose( fun (tn,ev,dt) ->
                if String.IsNullOrWhiteSpace dt then None else
                let ty = Type.GetType dt
                // we need to convert the sqldbtype enum value to dbtype.
                // the sql param will do this for us but it might throw if not mapped -
                // this is a bit hacky but I don't want to write a big conversion mapping right now
                let p = SqlParameter()
                try
                    p.SqlDbType <- enum<SqlDbType> ev
                    Some ((tn,ty),(tn,p.DbType),(ty.FullName,p.DbType))
                with
                | ex -> None
            )
            |> fun x ->  
                let fst (x,_,_) = x
                let snd (_,y,_) = y
                let trd (_,_,z) = z
                (Map.ofList (List.map fst x), 
                 Map.ofList (List.map snd x),
                 Map.ofList (List.map trd x))

        // set lookup functions         
        sqlToClr <-  (fun name -> Map.tryFind name sqlToClr')
        sqlToEnum <- (fun name -> Map.tryFind name sqlToEnum' )
        clrToEnum <- (fun name -> Map.tryFind name clrToEnum' )
    
    let executeSql (con:IDbConnection) sql =
        use com = new SqlCommand(sql,con:?>SqlConnection)    
        com.ExecuteReader()

    interface ISqlProvider with
        member __.CreateConnection(connectionString) = upcast new SqlConnection(connectionString)
        member __.CreateCommand(connection,commandText) = upcast new SqlCommand(commandText,connection:?>SqlConnection)
        member __.CreateCommandParameter(name,value,dbType) = 
            let p = SqlParameter(name,value)            
            if dbType.IsSome then p.DbType <- dbType.Value 
            upcast p
        member __.CreateTypeMappings(con) = createTypeMappings (con:?>SqlConnection)
        member __.ClrToEnum = clrToEnum
        member __.SqlToEnum = sqlToEnum
        member __.SqlToClr = sqlToClr
        member __.GetTables(con) =
            use reader = executeSql con "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE from INFORMATION_SCHEMA.TABLES"
            [ while reader.Read() do 
                let table ={ Schema = reader.GetSqlString(0).Value ; Name = reader.GetSqlString(1).Value ; Type=reader.GetSqlString(2).Value.ToLower() } 
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
               // note this data can be obtained using con.GetSchema, but with an epic schema we only want to get the data
               // we are inerested in on demand
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
               use com = new SqlCommand(baseQuery,con:?>SqlConnection)
               com.Parameters.AddWithValue("@schema",table.Schema) |> ignore
               com.Parameters.AddWithValue("@table",table.Name) |> ignore
               use reader = com.ExecuteReader()
               let columns =
                  [ while reader.Read() do 
                      let dt = reader.GetSqlString(1).Value
                      match sqlToClr dt, sqlToEnum dt with
                      | Some(clr),Some(sql) ->
                         let col =
                            { Column.Name = reader.GetSqlString(0).Value; 
                              ClrType = clr; 
                              DbType = sql; 
                              IsPrimarKey = if reader.GetSqlString(5).Value = "PRIMARY KEY" then true else false } 
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
                    yield { Name = reader.GetSqlString(0).Value; PrimaryTable=toSchema (reader.GetSqlString(9).Value) (reader.GetSqlString(5).Value); PrimaryKey=reader.GetSqlString(6).Value
                            ForeignTable=toSchema (reader.GetSqlString(8).Value) (reader.GetSqlString(1).Value); ForeignKey=reader.GetSqlString(2).Value } ] 
            reader.Dispose()
            use reader = executeSql con (sprintf "%s WHERE KCU1.TABLE_NAME = '%s'" baseQuery table.Name )
            let parents =
                [ while reader.Read() do 
                    yield { Name = reader.GetSqlString(0).Value; PrimaryTable=toSchema (reader.GetSqlString(9).Value) (reader.GetSqlString(5).Value); PrimaryKey=reader.GetSqlString(6).Value
                            ForeignTable=toSchema (reader.GetSqlString(8).Value) (reader.GetSqlString(1).Value); ForeignKey=reader.GetSqlString(2).Value } ] 
            relationshipLookup.Add(table.FullName,(children,parents))
            (children,parents)    
        member __.GetSprocs(con) = 
            let con = con:?>SqlConnection
            //todo: this whole function needs cleaning up
            let baseQuery = @"SELECT 
                              SPECIFIC_SCHEMA
                              ,SPECIFIC_NAME
                              ,ORDINAL_POSITION
                              ,PARAMETER_MODE
                              ,PARAMETER_NAME
                              ,DATA_TYPE
                              ,CHARACTER_MAXIMUM_LENGTH
                            FROM INFORMATION_SCHEMA.PARAMETERS a
                            JOIN sys.procedures b on a.SPECIFIC_NAME = b.name"
            use reader = executeSql con baseQuery
            let meta =
                [ while reader.Read() do
                       yield 
                           (reader.GetSqlString(0).Value,
                            reader.GetSqlString(1).Value,
                            reader.GetSqlInt32(2).Value,
                            reader.GetSqlString(3).Value,
                            reader.GetSqlString(4).Value,
                            reader.GetSqlString(5).Value,
                            let len = reader.GetSqlInt32(6)
                            if len.IsNull then None else Some len.Value ) ]
                |> Seq.groupBy(fun (schema,name,_,_,_,_,_) -> sprintf "[%s].[%s]" schema name)
                |> Seq.choose(fun (name,values) ->
                   // don't create procs that have unsupported datatypes
                   let values' = 
                      values 
                      |> Seq.choose(fun (_,_,ordinal,mode,name,dt,maxLen)  ->
                         if mode <> "IN" then None else
                         match sqlToClr dt, sqlToEnum dt with
                         |Some(clr), Some(sql) -> Some (ordinal,mode,name,clr,sql,maxLen)
                         | _ -> None)
                   if Seq.length values = Seq.length values' then Some (name,values') else None)
                |> Seq.map(fun (name, values) ->  
                    let parameters = 
                        values |> Seq.map(fun (ordinal,mode,name,clr,sql,maxLen) -> 
                               { Name=name; Ordinal=ordinal
                                 Direction = if mode = "IN" then In else Out
                                 MaxLength = maxLen
                                 ClrType = clr
                                 DbType = sql } )
                        |> Seq.sortBy( fun p -> p.Ordinal)     
                        |> Seq.toList            
                    {FullName = name
                     Params = parameters
                     ReturnColumns = [] })
                |> Seq.toList
            reader.Dispose()
       
            meta
            |> List.choose(fun sproc ->
                use com = new SqlCommand(sproc.FullName,con)
                com.CommandType <- CommandType.StoredProcedure
                try // try / catch here as this stuff is still experimental
                  sproc.Params
                  |> List.iter(fun p ->
                    let p' = SqlParameter()           
                    p'.ParameterName <- p.Name
                    p'.DbType <- p.DbType
                    p'.Value <- 
                         if p.ClrType = typeof<string> then box "1"
                         elif p.ClrType = typeof<DateTime> then box (DateTime(2000,1,1))
                         elif p.ClrType.IsArray then box (Array.zeroCreate 0)
                         // warning: i might have missed cases here and this next call will
                         // blow if the type doesn't have a parameterless ctor
                         else Activator.CreateInstance(p.ClrType)
                    com.Parameters.Add p' |> ignore)
                  use reader = com.ExecuteReader(CommandBehavior.SchemaOnly)
                  let schema = reader.GetSchemaTable()
                  let columns = 
                      if schema = null then [] else
                      schema.Rows
                      |> Seq.cast<DataRow>
                      |> Seq.choose(fun row -> 
                           (clrToEnum (row.["DataType"] :?> Type).FullName ) 
                           |> Option.map( fun sql ->
                                 { Name = row.["ColumnName"] :?> string; ClrType = (row.["DataType"] :?> Type ); 
                                   DbType = sql; IsPrimarKey = false } ))
                      |> Seq.toList
                  if schema = null || columns.Length = schema.Rows.Count then
                     Some { sproc with ReturnColumns = columns }
                  else None
                with 
                | ex -> System.Diagnostics.Debug.WriteLine(sprintf "Failed to rerieve metadata whilst executing sproc %s\r\n : %s" sproc.FullName (ex.ToString()))
                        None 
         )

        member this.GetIndividualsQueryText(table,amount) = sprintf "SELECT TOP %i * FROM %s" amount table.FullName

        member this.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns) = 
            let sb = System.Text.StringBuilder()
            let parameters = ResizeArray<_>()
            let (~~) (t:string) = sb.Append t |> ignore
            
            // SQL query syntax is ordered in the following manner
            // SELECT [alias].[field] as '[alias].[field]' [, .. ]
            // FROM [TABLE_1] as [alias1] join [TABLE_2] as [Alias_2] on ...
            // WHERE (([TABLE_1].Field = cirtiera AND [TABLE_1].Field = cirtiera) OR [TABLE_1].Field = cirtiera ) ...

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
                                if singleEntity then yield sprintf "[%s].[%s] as '%s'" k col col
                                else yield sprintf "[%s].[%s] as '[%s].[%s]'" k col k col
                        else
                            for col in v do 
                                if singleEntity then yield sprintf "[%s].[%s] as '%s'" k col col
                                yield sprintf "[%s].[%s] as '[%s].[%s]'" k col k col|]) // F# makes this so easy :)
        
            // next up is the filter expressions
            // make this nicer later.. just try and get the damn thing to work properly (well, at all) for now :D
            // NOTE: really need to assign the parameters their correct sql types
            let param = ref 0
            let nextParam() =
                incr param
                sprintf "@param%i" !param

            let createParam (value:obj) =
                let paramName = nextParam()
                SqlParameter(paramName,value):> IDataParameter

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
                                    | FSharp.Data.Sql.IsNull -> (sprintf "[%s].[%s] IS NULL") alias col 
                                    | FSharp.Data.Sql.NotNull -> (sprintf "[%s].[%s] IS NOT NULL") alias col 
                                    | FSharp.Data.Sql.In ->                                     
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "[%s].[%s] IN (%s)") alias col text
                                    | FSharp.Data.Sql.NotIn ->                                    
                                        let text = String.Join(",",paras |> Array.map (fun p -> p.ParameterName))
                                        Array.iter parameters.Add paras
                                        (sprintf "[%s].[%s] NOT IN (%s)") alias col text 
                                    | _ -> 
                                        parameters.Add paras.[0]
                                        (sprintf "[%s].[%s]%s %s") alias col 
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
                |> Map.iter(fun fromAlias (destList) ->
                    destList
                    |> List.iter(fun (alias,data) -> 
                        let joinType = if data.OuterJoin then "LEFT OUTER JOIN " else "INNER JOIN "
                        let destTable = getTable alias
                        ~~  (sprintf "%s [%s].[%s] as [%s] on [%s].[%s] = [%s].[%s] " 
                               joinType destTable.Schema destTable.Name alias 
                               (if data.RelDirection = RelationshipDirection.Parents then fromAlias else alias)
                               data.ForeignKey  
                               (if data.RelDirection = RelationshipDirection.Parents then alias else fromAlias) 
                               data.PrimaryKey)))

            let orderByBuilder() =
                sqlQuery.Ordering
                |> List.iteri(fun i (alias,column,desc) -> 
                    if i > 0 then ~~ ", "
                    ~~ (sprintf "[%s].[%s]%s" alias column (if not desc then "DESC" else "")))

            // SELECT
            if sqlQuery.Distinct then ~~(sprintf "SELECT DISTINCT %s "  columns)
            elif sqlQuery.Count then ~~("SELECT COUNT(1) ")
            else ~~(sprintf "SELECT %s "  columns)
            // FROM
            ~~(sprintf "FROM %s as [%s] " baseTable.FullName baseAlias)         
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