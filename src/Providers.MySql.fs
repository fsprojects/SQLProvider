namespace FSharp.Data.Sql.Providers

open System
open System.Collections.Generic
open System.Data

open FSharp.Data.Sql
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Common

type internal MySqlProvider(resolutionPath) as this =
    // note we intentionally do not hang onto a connection object at any time,
    // as the type provider will dicate the connection lifecycles 
    let pkLookup =     Dictionary<string,string>()
    let tableLookup =  Dictionary<string,Table>()
    let columnLookup = Dictionary<string,Column list>()
    let relationshipLookup = Dictionary<string,Relationship list * Relationship list>()

    let mutable clrToEnum : (string -> DbType option)  = fun _ -> failwith "!"
    let mutable sqlToEnum : (string -> DbType option)  = fun _ -> failwith "!"
    let mutable sqlToClr :  (string -> Type option)    = fun _ -> failwith "!"

    let mutable mySqlToDbType : (int -> DbType)  = fun _ -> failwith "!"
    let mutable dbTypeToMySql : (DbType -> int ) = fun _ -> failwith "!"

    // Dynamically load the MySQL assembly so we don't have a dependency on it in the project
    let assembly =  
        Reflection.Assembly.LoadFrom(
            if String.IsNullOrEmpty resolutionPath then "MySql.Data.dll"
            else System.IO.Path.Combine(resolutionPath,"MySql.Data.dll"))
   
    let connectionType =  (assembly.GetTypes() |> Array.find(fun t -> t.Name = "MySqlConnection"))
    let commandType =     (assembly.GetTypes() |> Array.find(fun t -> t.Name = "MySqlCommand"))
    let paramterType =    (assembly.GetTypes() |> Array.find(fun t -> t.Name = "MySqlParameter"))
    let enumType =        (assembly.GetTypes() |> Array.find(fun t -> t.Name = "MySqlDbType"))
    let getSchemaMethod = (connectionType.GetMethod("GetSchema",[|typeof<string>|]))
    let paramEnumCtor   = paramterType.GetConstructor([|typeof<string>;enumType|])
    let paramObjectCtor = paramterType.GetConstructor([|typeof<string>;typeof<obj>|])

    let createTypeMappings (dt:DataTable) =
        let clr = [for r in dt.Rows -> string r.["TypeName"],  unbox<int> r.["ProviderDbType"], string r.["DataType"]]
        
        // MySqlParamter only accepts a MySqlDbType, and if you subsequently set DbType it does not 
        // translate MySqlDbType to the correct value for you. Therefore, we are going to need to store
        // a list of mappings between MySqlDbType and DbType.  This can be accomplished by creating a param
        // with the relevant MySqlDbType, then reading the resulting DbType on the object.               
        let getDbTypeId (mySqlDbType:int) = 
            let p = paramEnumCtor.Invoke([|box ""; box mySqlDbType|]) :?> IDbDataParameter
            p.DbType
                
        let (dbTypeToMySql',mySqlToDbType') =
            clr
            |> List.map(fun (_,mySqlDbType,_) -> 
                let dbT = getDbTypeId mySqlDbType
                (dbT,mySqlDbType),(mySqlDbType,dbT)
                )
            |> fun x -> 
                 Map.ofList (List.map fst x),
                 Map.ofList (List.map snd x)
       
        dbTypeToMySql <- (fun id -> dbTypeToMySql'.[id] )
        mySqlToDbType <- (fun id -> mySqlToDbType'.[id] )

        // create map from sql name to clr type, and type to DbType enum
        let sqlToClr', sqlToEnum', clrToEnum' =
            clr
            |> List.choose( fun (tn,ev,dt) ->
                if String.IsNullOrWhiteSpace dt then None else
                let ty = Type.GetType dt
                let tn = tn.ToLower()
                // lookup the DbType version of ProviderDbType (in this case, MySqlDbType)
                let dbT = mySqlToDbType ev
                Some ((tn,ty),(tn,dbT),(ty.FullName,dbT))
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
        use com = (this:>ISqlProvider).CreateCommand(con,sql)    
        com.ExecuteReader()

    interface ISqlProvider with
        member __.CreateConnection(connectionString) = Activator.CreateInstance(connectionType,[|box connectionString|]) :?> IDbConnection
        member __.CreateCommand(connection,commandText) = Activator.CreateInstance(commandType,[|box commandText;box connection|]) :?> IDbCommand
        member __.CreateCommandParameter(name,value,dbType) = 
            match dbType with
            | Some v -> paramEnumCtor.Invoke([|box name;box(dbTypeToMySql v)|]) :?> IDataParameter
            | None -> paramObjectCtor.Invoke([|box name;box value|]) :?> IDataParameter            
        member __.CreateTypeMappings(con) = 
            let dt = getSchemaMethod.Invoke(con,[|"DataTypes"|]) :?> DataTable
            createTypeMappings dt
        member __.ClrToEnum = clrToEnum
        member __.SqlToEnum = sqlToEnum
        member __.SqlToClr = sqlToClr        
        member __.GetTables(con) =
            use reader = executeSql con "select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE from INFORMATION_SCHEMA.TABLES"
            [ while reader.Read() do 
                let table ={ Schema = reader.GetString(0) ; Name = reader.GetString(1) ; Type=reader.GetString(2).ToLower() } 
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
               com.Parameters.Add((this:>ISqlProvider).CreateCommandParameter("@schema",table.Schema,None)) |> ignore
               com.Parameters.Add((this:>ISqlProvider).CreateCommandParameter("@table",table.Name,None)) |> ignore
               use reader = com.ExecuteReader()
               let columns =
                  [ while reader.Read() do 
                      let dt = reader.GetString(1)
                      match sqlToClr dt, sqlToEnum dt with
                      | Some(clr),Some(sql) ->
                         let col =
                            { Column.Name = reader.GetString(0) 
                              ClrType = clr 
                              DbType = sql
                              IsNullable = let b = reader.GetString(4) in if b = "YES" then true else false
                              IsPrimarKey = if reader.GetString(5) = "PRIMARY KEY" then true else false } 
                         if col.IsPrimarKey && pkLookup.ContainsKey table.FullName = false then pkLookup.Add(table.FullName,col.Name)
                         yield col 
                      | _ -> ()]  
               columnLookup.Add(table.FullName,columns)
               columns
        member __.GetRelationships(con,table) = 
            match relationshipLookup.TryGetValue table.FullName with 
            | true,v -> v
            | _ -> 
            let toSchema schema table = sprintf "[%s].[%s]" schema table
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

            use reader = executeSql con (sprintf "%s WHERE RC.TABLE_NAME = '%s'" baseQuery table.Name )
            let children =
                [ while reader.Read() do 
                    yield { Name = reader.GetString(0); PrimaryTable=toSchema (reader.GetString(2)) (reader.GetString(1)); PrimaryKey=reader.GetString(3)
                            ForeignTable=toSchema (reader.GetString(5)) (reader.GetString(4)); ForeignKey=reader.GetString(6) } ] 
            reader.Dispose()
            use reader = executeSql con (sprintf "%s WHERE RC.REFERENCED_TABLE_NAME = '%s'" baseQuery table.Name )
            let parents =
                [ while reader.Read() do 
                    yield { Name = reader.GetString(0); PrimaryTable=toSchema (reader.GetString(2)) (reader.GetString(1)); PrimaryKey=reader.GetString(3)
                            ForeignTable=toSchema (reader.GetString(5)) (reader.GetString(4)); ForeignKey=reader.GetString(6) } ] 
            relationshipLookup.Add(table.FullName,(children,parents))
            (children,parents)    
        
        // todo
        member __.GetSprocs(con) =             
           []

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
                                yield sprintf "`%s`.`%s` as '`%s`.`%s`'" k col k col|]) // F# makes this so easy :)
        
            // next up is the filter expressions
            // make this nicer later.. just try and get the damn thing to work properly (well, at all) for now :D
            // NOTE: really need to assign the parameters their correct sql types
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
                |> Map.iter(fun fromAlias (destList) ->
                    destList
                    |> List.iter(fun (alias,data) -> 
                        let joinType = if data.OuterJoin then "LEFT OUTER JOIN " else "INNER JOIN "
                        let destTable = getTable alias
                        ~~  (sprintf "%s `%s`.`%s` as `%s` on `%s`.`%s` = `%s`.`%s` " 
                               joinType destTable.Schema destTable.Name alias 
                               (if data.RelDirection = RelationshipDirection.Parents then fromAlias else alias)
                               data.ForeignKey  
                               (if data.RelDirection = RelationshipDirection.Parents then alias else fromAlias) 
                               data.PrimaryKey)))

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

            if sqlQuery.Take.IsSome then 
                ~~(sprintf " LIMIT %i;" sqlQuery.Take.Value)

            let sql = sb.ToString()
            (sql,parameters)