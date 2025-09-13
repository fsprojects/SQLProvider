namespace FSharp.Data.Sql

open System
open System.Data
open System.Reflection
open System.Threading.Tasks
open Microsoft.FSharp.Core.CompilerServices
open Microsoft.FSharp.Quotations
open FSharp.Data.Sql.Transactions
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Runtime
open FSharp.Data.Sql.Common
open FSharp.Data.Sql
open ProviderImplementation.ProvidedTypes

type DesignCacheKey =
            (struct (  string *                    // ConnectionString URL
                    string *                    // ConnectionString Name
                    DatabaseProviderTypes *     // db vendor
                    string *                    // Assembly resolution path for db connectors and custom types
                    int *                       // Individuals Amount
                    NullableColumnType *                      // Use option types?
                    string *                    // Schema owner currently only used for oracle
                    CaseSensitivityChange *     // Should we do ToUpper or ToLower when generating table names?
                    string *                    // Table names list (Oracle and MSSQL Only)
                    string *                    // Context schema path
                    OdbcQuoteCharacter *       // Quote characters (Odbc only)
                    SQLiteLibrary *            // Use System.Data.SQLite or Mono.Data.SQLite or select automatically (SQLite only)
                    string *                   // SSDT Path
                    string))                    //typeName

module DesignTimeCacheSchema =
    let schemaMap = System.Collections.Concurrent.ConcurrentDictionary<DesignCacheKey*string, ProvidedTypeDefinition>()

type internal ParameterValue =
    | UserProvided of string * string * Type
    | Default of Expr

module DesignTimeUtils =

#if COMMON
        let [<Literal>] design1 = "FSharp.Data.SqlProvider.DesignTime"
        let [<Literal>] runtime1 = "FSharp.Data.SqlProvider"
        let [<Literal>] design2 = "SQLProvider.DesignTime"
        let [<Literal>] runtime2 = "SQLProvider.Runtime"
        let [<Literal>] FSHARP_DATA_SQL = "FSharp.Data.Sql"
#endif
#if MSSQL
        let [<Literal>] design1 = "FSharp.Data.SqlProvider.MsSql.DesignTime"
        let [<Literal>] runtime1 = "FSharp.Data.SqlProvider.MsSql"
        let [<Literal>] design2 = "SQLProvider.MsSql.DesignTime"
        let [<Literal>] runtime2 = "SQLProvider.MsSql.Runtime"
        let [<Literal>] FSHARP_DATA_SQL = "FSharp.Data.Sql.MsSql"
#endif
#if POSTGRESQL
        let [<Literal>] design1 = "FSharp.Data.SqlProvider.PostgreSql.DesignTime"
        let [<Literal>] runtime1 = "FSharp.Data.SqlProvider.PostgreSql"
        let [<Literal>] design2 = "SQLProvider.PostgreSql.DesignTime"
        let [<Literal>] runtime2 = "SQLProvider.PostgreSql.Runtime"
        let [<Literal>] FSHARP_DATA_SQL = "FSharp.Data.Sql.PostgreSql"
#endif
#if MYSQL
        let [<Literal>] design1 = "FSharp.Data.SqlProvider.MySql.DesignTime"
        let [<Literal>] runtime1 = "FSharp.Data.SqlProvider.MySql"
        let [<Literal>] design2 = "SQLProvider.MySql.DesignTime"
        let [<Literal>] runtime2 = "SQLProvider.MySql.Runtime"
        let [<Literal>] FSHARP_DATA_SQL = "FSharp.Data.Sql.MySql"
#endif
#if MYSQLCONNECTOR
        let [<Literal>] design1 = "FSharp.Data.SqlProvider.MySqlConnector.DesignTime"
        let [<Literal>] runtime1 = "FSharp.Data.SqlProvider.MySqlConnector"
        let [<Literal>] design2 = "SQLProvider.MySqlConnector.DesignTime"
        let [<Literal>] runtime2 = "SQLProvider.MySqlConnector.Runtime"
        let [<Literal>] FSHARP_DATA_SQL = "FSharp.Data.Sql.MySqlConnector"
#endif
#if SQLITE
        let [<Literal>] design1 = "FSharp.Data.SqlProvider.SQLite.DesignTime"
        let [<Literal>] runtime1 = "FSharp.Data.SqlProvider.SQLite"
        let [<Literal>] design2 = "SQLProvider.SQLite.DesignTime"
        let [<Literal>] runtime2 = "SQLProvider.SQLite.Runtime"
        let [<Literal>] FSHARP_DATA_SQL = "FSharp.Data.Sql.SQLite"
#endif
#if FIREBIRD
        let [<Literal>] design1 = "FSharp.Data.SqlProvider.Firebird.DesignTime"
        let [<Literal>] runtime1 = "FSharp.Data.SqlProvider.Firebird"
        let [<Literal>] design2 = "SQLProvider.Firebird.DesignTime"
        let [<Literal>] runtime2 = "SQLProvider.Firebird.Runtime"
        let [<Literal>] FSHARP_DATA_SQL = "FSharp.Data.Sql.Firebird"
#endif
#if ODBC
        let [<Literal>] design1 = "FSharp.Data.SqlProvider.Odbc.DesignTime"
        let [<Literal>] runtime1 = "FSharp.Data.SqlProvider.Odbc"
        let [<Literal>] design2 = "SQLProvider.Odbc.DesignTime"
        let [<Literal>] runtime2 = "SQLProvider.Odbc.Runtime"
        let [<Literal>] FSHARP_DATA_SQL = "FSharp.Data.Sql.Odbc"
#endif
#if ORACLE
        let [<Literal>] design1 = "FSharp.Data.SqlProvider.Oracle.DesignTime"
        let [<Literal>] runtime1 = "FSharp.Data.SqlProvider.Oracle"
        let [<Literal>] design2 = "SQLProvider.Oracle.DesignTime"
        let [<Literal>] runtime2 = "SQLProvider.Oracle.Runtime"
        let [<Literal>] FSHARP_DATA_SQL = "FSharp.Data.Sql.Oracle"
#endif
#if MSACCESS
        let [<Literal>] design1 = "FSharp.Data.SqlProvider.MsAccess.DesignTime"
        let [<Literal>] runtime1 = "FSharp.Data.SqlProvider.MsAccess"
        let [<Literal>] design2 = "SQLProvider.MsAccess.DesignTime"
        let [<Literal>] runtime2 = "SQLProvider.MsAccess.Runtime"
        let [<Literal>] FSHARP_DATA_SQL = "FSharp.Data.Sql.MsAccess"
#endif
#if DUCKDB
        let [<Literal>] design1 = "FSharp.Data.SqlProvider.DuckDb.DesignTime"
        let [<Literal>] runtime1 = "FSharp.Data.SqlProvider.DuckDb"
        let [<Literal>] design2 = "SQLProvider.DuckDb.DesignTime"
        let [<Literal>] runtime2 = "SQLProvider.DuckDb.Runtime"
        let [<Literal>] FSHARP_DATA_SQL = "FSharp.Data.Sql.DuckDb"
#endif

        let mySaveLock = new Object();
        let mutable saveInProcess = false

        let empty = fun (_:Expr list) -> <@@ () @@>

        let getSprocReturnColumns con (prov:ISqlProvider) sprocname (sprocDefinition: CompileTimeSprocDefinition) param =
            match con with
            | Some con ->
                let returnParams = sprocDefinition.ReturnColumns con param
                prov.GetSchemaCache().SprocsParams.AddOrUpdate(sprocname, returnParams, fun _ inputParams -> inputParams @ returnParams) |> ignore
                returnParams
            | None ->
                let ok, pars = prov.GetSchemaCache().SprocsParams.TryGetValue sprocname
                if ok then
                    pars |> List.filter (fun p ->
                        not (p.Name |> String.IsNullOrWhiteSpace)
                        && (p.Direction = ParameterDirection.Output
                        || p.Direction = ParameterDirection.InputOutput
                        || p.Direction = ParameterDirection.ReturnValue))
                    else []

        let transactionOptions = TransactionOptions.Default

        let createIndividualsType (con:IDbConnection option) (prov:ISqlProvider) (table:Table) (designTimeDc:Lazy<_>) dbVendor individualsAmount tableTypeDef =
            let t = ProvidedTypeDefinition(table.Schema + "." + table.Name + "." + "Individuals", Some typeof<obj>, isErased=true)
            let individualsTypes = ResizeArray<_>()
            individualsTypes.Add t

            t.AddXmlDocDelayed(fun _ -> sprintf "<summary>A sample of %s individuals from the SQL object as supplied in the static parameters</summary>" table.Name)
            t.AddMember(ProvidedConstructor([ProvidedParameter("dataContext", typeof<ISqlDataContext>)], empty))
            t.AddMembersDelayed( fun _ ->
                let columns =
                    match con with
                    | Some con -> prov.GetColumns(con,table)
                    | None -> prov.GetSchemaCache().Columns.TryGetValue(table.FullName) |> function | true,cols -> cols | false, _ -> Map.empty
                match prov.GetPrimaryKey table with
                | Some pkName ->

                    let rec (|FixedType|_|) (o:obj) =
                        match o, o.GetType().IsValueType with
                        // watch out for normal strings
                        | :? string, _ -> Some o
                        // special case for guids as they are not a supported quotable constant in the TP mechanics,
                        // but we can deal with them as strings.
                        | :? Guid, _ -> Some (box (o.ToString()))
                        // Postgres also supports arrays
                        | :? Array as arr, _ when dbVendor = DatabaseProviderTypes.POSTGRESQL -> Some (box arr)
                        // value types in general work
                        | _, true -> Some o
                        // can't support any other types
                        | _, _ -> None

                    let dcDone = designTimeDc.Force()
                    let entities =
                        prov.GetSchemaCache().Individuals.GetOrAdd((table.FullName+"_"+pkName), fun k ->
                            match con with
                            | Some con ->
                                use com = prov.CreateCommand(con,prov.GetIndividualsQueryText(table,individualsAmount))
                                if con.State <> ConnectionState.Open then con.Open()
                                use reader = com.ExecuteReader()
                                let ret = (dcDone :> ISqlDataContext).ReadEntities(table.FullName+"_"+pkName, columns, reader)
                                reader.Close()
                                if (dbVendor <> DatabaseProviderTypes.MSACCESS) then con.Close()
                                let mapped = ret |> Array.choose(fun e ->
                                        match (e :> IColumnHolder).GetColumn pkName with
                                        | FixedType pkValue -> Some (pkValue, e.ColumnValues |> dict)
                                        | _ -> None)
                                mapped
                            | None -> [||]
                        )
                    if Array.isEmpty entities then [] else
                    // for each column in the entity except the primary key, create a new type that will read ``As Column 1`` etc
                    // inside that type the individuals will be listed again but with the text for the relevant column as the name
                    // of the property and the primary key e.g. ``1, Dennis The Squirrel``
                    let buildFieldName = SchemaProjections.buildFieldName

                    let propertyMap =
                        match con with
                        | Some con -> prov.GetColumns(con,table)
                        | None -> prov.GetSchemaCache().Columns.TryGetValue(table.FullName) |> function | true,cols -> cols | false, _ -> Map.empty
                        |> Seq.choose(fun col ->
                        if col.Key = pkName then None else
                        let name = table.Schema + "." + table.Name + "." + col.Key + "Individuals"
                        let ty = ProvidedTypeDefinition(name, Some typeof<obj>, isErased=true)
                        ty.AddMember(ProvidedConstructor([ProvidedParameter("sqlService", typeof<ISqlDataContext>)], empty))
                        individualsTypes.Add ty
                        Some(col.Key,(ty,ProvidedProperty(sprintf "As %s" (buildFieldName col.Key),ty, getterCode = fun args ->
                            let a0 = args.[0]
                            <@@ ((%%a0 : obj) :?> ISqlDataContext)@@> ))))
                        |> Map.ofSeq

                    let prettyPrint (value : obj) =
                        let dirtyName =
                            match value with
                            | null -> "<null>"
                            | :? Array as a -> (sprintf "%A" a)
                            | x -> x.ToString()
                        dirtyName.Replace("\r", "").Replace("\n", "").Replace("\t", "")

                    // on the main object create a property for each entity simply using the primary key
                    let props =
                        entities
                        |> Array.choose(fun (pkValue, columnValues) ->
                            let tableName = table.FullName
                            let getterCode (args : Expr list) =
                                let a0 = args.[0]
                                <@@ ((%%a0 : obj) :?> ISqlDataContext).GetIndividual(tableName, pkValue) @@>

                            // this next bit is just side effect to populate the "As Column" types for the supported columns
                            for colName, colValue in columnValues |> Seq.map(fun kvp -> kvp.Key, kvp.Value) do
                                if colName <> pkName then
                                    let colDefinition, _ = propertyMap.[colName]
                                    colDefinition.AddMemberDelayed(fun() ->
                                        ProvidedProperty( sprintf "%s, %s" (prettyPrint pkValue) (prettyPrint colValue)
                                                        , tableTypeDef
                                                        , getterCode = getterCode
                                                        )
                                    )
                            // return the primary key property
                            Some <| ProvidedProperty(prettyPrint pkValue
                                                    , tableTypeDef
                                                    , getterCode = getterCode
                                                    )
                            )
                        |> Array.append( propertyMap |> Map.toArray |> Array.map (snd >> snd))

                    propertyMap
                    |> Map.toSeq
                    |> Seq.map (snd >> fst)
                    |> Seq.cast<MemberInfo>
                    |> Seq.append (props |> Seq.cast<MemberInfo>)
                    |> Seq.toList

                | None -> [])
            individualsTypes :> seq<_>

        let createColumnProperty (con:IDbConnection option) (prov:ISqlProvider) useOptionTypes key (c:Column) =
            let nullable = if c.IsNullable then useOptionTypes else NullableColumnType.NO_OPTION
            let ty = Utilities.getType c.TypeMapping.ClrType

            let propTy = match nullable with
                            | NullableColumnType.OPTION -> typedefof<option<_>>.MakeGenericType(ty)
                            | NullableColumnType.VALUE_OPTION -> typedefof<ValueOption<_>>.MakeGenericType(ty)
                            | _ -> ty
            let name = c.Name
            let prop =
                ProvidedProperty(
                    SchemaProjections.buildFieldName(name),propTy,
                    getterCode =
                        match nullable with
                        | NullableColumnType.OPTION ->
                           (fun (args:Expr list) ->
                            let meth = typeof<IColumnHolder>.GetMethod("GetColumnOption").MakeGenericMethod([|ty|])
                            Expr.Call(args.[0],meth,[Expr.Value name]))
                        | NullableColumnType.VALUE_OPTION ->
                           (fun (args:Expr list) ->
                            let meth = typeof<IColumnHolder>.GetMethod("GetColumnValueOption").MakeGenericMethod([|ty|])
                            Expr.Call(args.[0],meth,[Expr.Value name]))
                        | _ ->
                           (fun (args:Expr list) ->
                            let meth = typeof<IColumnHolder>.GetMethod("GetColumn").MakeGenericMethod([|ty|])
                            Expr.Call(args.[0],meth,[Expr.Value name]))
                    ,
                    setterCode =
                        match nullable with
                        | NullableColumnType.OPTION ->
                           (fun (args:Expr list) ->
                            let meth = typeof<IColumnHolder>.GetMethod("SetColumnOption").MakeGenericMethod([|ty|])
                            Expr.Call(args.[0],meth,[Expr.Value name;args.[1]]))
                        | NullableColumnType.VALUE_OPTION ->
                           (fun (args:Expr list) ->
                            let meth = typeof<IColumnHolder>.GetMethod("SetColumnValueOption").MakeGenericMethod([|ty|])
                            Expr.Call(args.[0],meth,[Expr.Value name;args.[1]]))
                        | _ ->
                           (fun (args:Expr list) ->
                            let meth = typeof<SqlEntity>.GetMethod("SetColumn").MakeGenericMethod([|ty|])
                            Expr.Call(args.[0],meth,[Expr.Value name;args.[1]])))

            let nfo = c.TypeInfo
            let typeInfo = match nfo with ValueNone -> "" | ValueSome x -> x.ToString()
            match con with
            | Some con ->
                prop.AddXmlDocDelayed(fun () ->
                    let details = prov.GetColumnDescription(con, key, name).Replace("<","&lt;").Replace(">","&gt;")
                    let separator = if (String.IsNullOrWhiteSpace typeInfo) || (String.IsNullOrWhiteSpace details) then "" else "/"
                    sprintf "<summary>%s %s %s</summary>" (String.concat ": " [|name; details|]) separator typeInfo)
            | None ->
                prop.AddXmlDocDelayed(fun () -> sprintf "<summary>Offline mode. %s</summary>" typeInfo)
                ()
            prop

        let generateSprocMethod (container:ProvidedTypeDefinition) (con:IDbConnection option) (prov:ISqlProvider) (sproc:CompileTimeSprocDefinition) =

            let sprocname = SchemaProjections.buildSprocName(sproc.Name.DbName)
                            |> SchemaProjections.avoidNameClashBy (container.GetMember >> Array.isEmpty >> not)

            let rt = ProvidedTypeDefinition(sprocname, Some typeof<obj>, isErased=true)
            let resultType = ProvidedTypeDefinition("Result", Some typeof<obj>, isErased=true)
            resultType.AddMember(ProvidedConstructor([ProvidedParameter("sqlDataContext", typeof<ISqlDataContext>)], empty))
            rt.AddMember resultType
            container.AddMember(rt)

            resultType.AddMembersDelayed(fun () ->
                    let sprocParameters =

                        match con with
                        | None ->
                            match prov.GetSchemaCache().SprocsParams.TryGetValue sprocname with
                            | true, x -> x
                            | false, _ -> []
                        | Some con ->
                            (lazy
                                Sql.ensureOpen con
                                let ps = sproc.Params con
                                prov.GetSchemaCache().SprocsParams.AddOrUpdate(sprocname, ps, fun _ _ -> ps) |> ignore
                                ps).Value

                    let parameters =
                        sprocParameters
                        |> List.filter (fun p -> p.Direction = ParameterDirection.Input || p.Direction = ParameterDirection.InputOutput)
                        |> List.map(fun p -> ProvidedParameter(p.Name,Utilities.getType p.TypeMapping.ClrType))
                    let retCols = getSprocReturnColumns con prov sprocname sproc sprocParameters |> List.toArray
                    let runtimeSproc = {Name = sproc.Name; Params = sprocParameters} : RunTimeSprocDefinition
                    let returnType =
                        match retCols.Length with
                        | 0 -> typeof<Unit>
                        | _ ->
                                let rt = ProvidedTypeDefinition("SprocResult",Some typeof<SqlEntity>, isErased=true)
                                rt.AddMember(ProvidedConstructor([], empty))
                                retCols
                                |> Array.iter(fun col ->
                                    let name = col.Name
                                    let ty = Utilities.getType col.TypeMapping.ClrType
                                    let ty =
                                        if isNull ty then
                                            Utilities.getType (col.TypeMapping.ClrType+",FSharp.Data.SqlProvider.Common")
                                        else ty
                                    let prop =
                                        ProvidedProperty(
                                            name, ty,
                                            getterCode = (fun (args:Expr list) ->
                                                let meth = typeof<IColumnHolder>.GetMethod("GetColumn").MakeGenericMethod([|ty|])
                                                Expr.Call(args.[0],meth,[Expr.Value name])),
                                            setterCode = (fun (args:Expr list) ->
                                                let meth = typeof<SqlEntity>.GetMethod("SetColumn").MakeGenericMethod([|typeof<obj>|])
                                                Expr.Call(args.[0],meth,[Expr.Value name;Expr.Coerce(args.[1], typeof<obj>)])))
                                    rt.AddMember prop)
                                resultType.AddMember(rt)
                                rt :> Type
                    let retColsExpr =
                        QuotationHelpers.arrayExpr retCols |> snd
                    let isUnit, asyncRet =
                        if Type.(=)(returnType, typeof<unit>) then
                            true, typeof<Task>
                        else
                            false, typedefof<Task<_>>.MakeGenericType([| returnType |])
                    [ProvidedMethod("Invoke", parameters, returnType, invokeCode = QuotationHelpers.quoteRecord runtimeSproc (fun args var ->
                            let a0 = args.[0]
                            let tail = args.Tail
                            <@@ (((%%a0 : obj):?>ISqlDataContext)).CallSproc(%%var, %%retColsExpr,  %%Expr.NewArray(typeof<obj>,List.map(fun e -> Expr.Coerce(e,typeof<obj>)) tail)) @@>));
                        ProvidedMethod("InvokeAsync", parameters, asyncRet, invokeCode =
                            if isUnit then
                                QuotationHelpers.quoteRecord runtimeSproc (fun args var ->
                                    let a0 = args.[0]
                                    let tail = args.Tail
                                    <@@ task {
                                            let! r =
                                                (((%%a0 : obj):?>ISqlDataContext)).CallSprocAsync(%%var, %%retColsExpr,  %%Expr.NewArray(typeof<obj>,List.map(fun e -> Expr.Coerce(e,typeof<obj>)) tail))
                                            return ()
                                        } :> Task @@>
                                )
                            else
                                QuotationHelpers.quoteRecord runtimeSproc (fun args var ->
                                    let a0 = args.[0]
                                    let tail = args.Tail
                                    <@@ (((%%a0 : obj):?>ISqlDataContext)).CallSprocAsync(%%var, %%retColsExpr,  %%Expr.NewArray(typeof<obj>,List.map(fun e -> Expr.Coerce(e,typeof<obj>)) tail))  @@>
                                )
                            )]
            )

            let niceUniqueSprocName =
                SchemaProjections.buildSprocName(sproc.Name.ProcName)
                |> SchemaProjections.avoidNameClashBy (container.GetProperty >> (<>) null)

            let p = ProvidedProperty(niceUniqueSprocName, resultType, getterCode = (fun args ->
                let a0 = args.[0]
                <@@ ((%%a0 : obj) :?>ISqlDataContext) @@>) )
            let dbName = sproc.Name.DbName
            p.AddXmlDocDelayed(fun _ -> sprintf "<summary>%s</summary>" dbName)
            p


        let rec walkSproc con (prov:ISqlProvider) (path:string list) (parent:ProvidedTypeDefinition option) (createdTypes:Map<string list,ProvidedTypeDefinition>) (sproc:Sproc) =
            match sproc with
            | Root(typeName, next) ->
                let path = (path @ [typeName])
                match createdTypes.TryFind path with
                | Some(typ) ->
                    walkSproc con prov path (Some typ) createdTypes next
                | None ->
                    let typ = ProvidedTypeDefinition(typeName, Some typeof<obj>, isErased=true)
                    typ.AddMember(ProvidedConstructor([ProvidedParameter("sqlDataContext", typeof<ISqlDataContext>)], empty))
                    walkSproc con prov path (Some typ) (createdTypes.Add(path, typ)) next
            | Package(typeName, packageDefn) ->
                match parent with
                | Some(parent) ->
                    let path = (path @ [typeName])
                    let typ = ProvidedTypeDefinition(typeName, Some typeof<obj>, isErased=true)
                    parent.AddMember(typ)
                    parent.AddMember(ProvidedProperty(SchemaProjections.nicePascalName typeName, typ, getterCode = fun args ->
                        let a0 = args.[0]
                        <@@ ((%%a0 : obj) :?> ISqlDataContext) @@>))
                    typ.AddMember(ProvidedConstructor([ProvidedParameter("sqlDataContext", typeof<ISqlDataContext>)], empty))
                    match con with
                    | Some co ->
                        typ.AddMembersDelayed(fun () ->
                            (lazy
                                Sql.ensureOpen co
                                let p = (packageDefn.Sprocs co)
                                prov.GetSchemaCache().Packages.AddRange p
                                p |> List.map (generateSprocMethod typ con prov)).Value)
                    | None ->
                        typ.AddMembersDelayed(fun () ->
                            prov.GetSchemaCache().Packages |> Seq.toList |> List.map (generateSprocMethod typ con prov))
                    createdTypes.Add(path, typ)
                | _ -> failwithf "Could not generate package path type undefined root or previous type"
            | Sproc(sproc) ->
                    match parent with
                    | Some(parent) ->
                        match con with
                        | Some co ->
                            parent.AddMemberDelayed(fun () ->
                                (lazy
                                    Sql.ensureOpen co
                                    generateSprocMethod parent con prov sproc
                                ).Value)
                            createdTypes
                        | None ->
                            parent.AddMemberDelayed(fun () -> generateSprocMethod parent con prov sproc); createdTypes
                    | _ -> failwithf "Could not generate sproc undefined root or previous type"
            | Empty -> createdTypes

        let rec generateTypeTree con (prov:ISqlProvider) (createdTypes:Map<string list, ProvidedTypeDefinition>) (sprocs:Sproc list) =
            match sprocs with
            | [] ->
                Map.filter (fun (k:string list) _ -> match k with [_] -> true | _ -> false) createdTypes
                |> Map.toSeq
                |> Seq.map snd
            | sproc::rest -> generateTypeTree con prov (walkSproc con prov [] None createdTypes sproc) rest

        let getOrAddSchema (args:DesignCacheKey) (name:string) =
            DesignTimeCacheSchema.schemaMap.GetOrAdd((args,name), fun (a,nme) ->
                let pt = ProvidedTypeDefinition(nme + "Schema", Some typeof<obj>, isErased=true)
                pt)

        let createDesignTimeCommands (prov:ISqlProvider) contextSchemaPath recreate (invalidate: _ -> Unit)=
            let designTimeCommandsContainer = ProvidedTypeDefinition("DesignTimeCommands", Some typeof<obj>, isErased=true)
            let designTime = ProvidedProperty("Design Time Commands", designTimeCommandsContainer, getterCode = empty)
            designTime.AddXmlDocDelayed(fun () -> "Developer's design time commands to TypeProvider.")


            let saveResponse = ProvidedTypeDefinition("SaveContextResponse", Some typeof<obj>, isErased=true)
            saveResponse.AddMember(ProvidedConstructor([], empty))
            saveResponse.AddMembersDelayed(fun () ->
                if not saveInProcess then
                    let result =
                        if not(String.IsNullOrEmpty contextSchemaPath) then
                            try
                                lock mySaveLock (fun() ->
                                    saveInProcess <- true
                                    prov.GetSchemaCache().Save contextSchemaPath
                                    saveInProcess <- false
                                    "Saved " + contextSchemaPath + " at " + DateTime.Now.ToString("hh:mm:ss")
                                )
                            with
                            | e -> "Save failed: " + e.Message
                        else "ContextSchemaPath is not defined"
                    [ ProvidedProperty(result,typeof<unit>, getterCode = empty) :> MemberInfo ]
                else []
            )

            let m = ProvidedProperty("SaveContextSchema", (saveResponse :> Type), getterCode = empty)
            let mOld = ProvidedMethod("SaveContextSchema", [], (saveResponse :> Type), invokeCode = empty)
            m.AddXmlDocComputed(fun () ->
                if String.IsNullOrEmpty contextSchemaPath then "ContextSchemaPath static parameter has to be defined to use this function."
                else "Schema location: " + contextSchemaPath + ". Write dot after SaveContextSchema to save the schema at design time."
                )
            let expirationMessage = "Expired, moved under: .``Design Time Commands``"
            mOld.AddXmlDocComputed(fun () -> expirationMessage)
            mOld.AddObsoleteAttribute expirationMessage

            // ClearDatabaseSchemaCache only in online-mode
            if String.IsNullOrEmpty contextSchemaPath then
                let invalidateActionResponse = ProvidedTypeDefinition("InvalidateResponse", Some typeof<obj>, isErased=true)
                invalidateActionResponse.AddMember(ProvidedConstructor([], empty))
                invalidateActionResponse.AddMembersDelayed(fun () ->
                    if not saveInProcess then
                        let result =
                            lock mySaveLock (fun() ->
                                saveInProcess <- true
                                let schemacache = prov.GetSchemaCache()
                                schemacache.PrimaryKeys.Clear()
                                schemacache.Tables.Clear()
                                schemacache.Columns.Clear()
                                schemacache.Relationships.Clear()
                                schemacache.Sprocs.Clear()
                                schemacache.SprocsParams.Clear()
                                schemacache.Packages.Clear()
                                schemacache.Individuals.Clear()
                                DesignTimeCacheSchema.schemaMap.Clear()
                                invalidate()
                                let pf = recreate()
                                saveInProcess <- false
                                "Database schema cache cleared.")
                        [ProvidedProperty(result,typeof<unit>, getterCode = empty) :> MemberInfo]
                    else []
                )
                let m2 = ProvidedProperty("ClearDatabaseSchemaCache", (invalidateActionResponse :> Type), getterCode = empty)
                m2.AddXmlDocComputed(fun () ->
                    "This method can be used to refresh and detect recent database schema changes. " +
                    "Write dot after ClearDatabaseSchemaCache to invalidate and clear the schema cache. May take a while."
                    )
                designTimeCommandsContainer.AddMember m2
                designTimeCommandsContainer.AddMember m
                designTimeCommandsContainer, saveResponse, mOld, designTime, Some invalidateActionResponse
            else
                designTimeCommandsContainer.AddMember m
                designTimeCommandsContainer, saveResponse, mOld, designTime, None

        let rec createTypes (rootType:ProvidedTypeDefinition) (serviceType:ProvidedTypeDefinition) (readServiceType:ProvidedTypeDefinition) (config:TypeProviderConfig) (sqlRuntimeInfo:_) invalidate registerDispose (args) =
            let struct(connectionString, conStringName,dbVendor,resolutionPath,individualsAmount,useOptionTypes,owner,caseSensitivity, tableNames, contextSchemaPath, odbcquote, sqliteLibrary, ssdtPath, rootTypeName) = args
            let resolutionPath =
                if String.IsNullOrWhiteSpace resolutionPath
                then config.ResolutionFolder
                else resolutionPath

            let caseInsensitivityCheck =
                match caseSensitivity with
                | CaseSensitivityChange.TOLOWER -> (fun (x:string) -> x.ToLower())
                | CaseSensitivityChange.TOUPPER -> (fun (x:string) -> x.ToUpperInvariant())
                | _ -> id

            let conString = ConfigHelpers.tryGetConnectionString false config.ResolutionFolder conStringName connectionString

            let rootType, prov, con =
                    let referencedAssemblies = Array.append [|config.RuntimeAssembly|] config.ReferencedAssemblies
                    let prov : ISqlProvider = SqlDataContext.ProviderFactory dbVendor resolutionPath referencedAssemblies config.RuntimeAssembly owner tableNames contextSchemaPath odbcquote sqliteLibrary ssdtPath
                    let con =
                        match dbVendor with
                        | DatabaseProviderTypes.MSSQLSERVER_SSDT ->
                            if ssdtPath = "" then failwith "No SsdtPath was specified."
                            elif not (ssdtPath.EndsWith(".dacpac")) then failwith "SsdtPath must point to a .dacpac file."
                            elif not (System.IO.File.Exists ssdtPath) then failwith ("File not exists: " + ssdtPath)
                            else Some Stubs.connection
                        | _ ->
                            match conString, conStringName with
                            | "", "" -> failwith "No connection string or connection string name was specified."
                            | "", _ -> failwithf "Could not find a connection string with name '%s'." conStringName
                            | _ ->
                                match prov.GetSchemaCache().IsOffline with
                                | false ->
                                    let con = prov.CreateConnection conString
                                    registerDispose (con, dbVendor)
                                    try
                                        con.Open()
                                    with
                                    | exn ->
                                        let baseError = exn.GetBaseException()
                                        failwithf $"Error opening compile-time connection. Connection string: {conString}. Error: {exn.GetType()}, {exn.Message}, inner {baseError.GetType()} {baseError.Message}"
                                    prov.CreateTypeMappings con
                                    Some con
                                | true ->
                                    None

                    rootType, prov, con

            let tables =
                lazy
                    match con with
                    | Some con -> prov.GetTables(con,caseSensitivity)
                    | None -> prov.GetSchemaCache().Tables |> Seq.map (fun kv -> kv.Value) |> Seq.toArray

            let tableColumns =
                lazy
                    dict
                        [for t in tables.Force() do
                            yield( t.FullName,
                                lazy
                                    match con with
                                    | Some con ->
                                        let cols = prov.GetColumns(con,t)
                                        let rel = prov.GetRelationships(con,t)
                                        (cols,rel)
                                    | None ->
                                        let cols =
                                            match prov.GetSchemaCache().Columns.TryGetValue(t.FullName) with
                                            | true,cols -> cols
                                            | false,_ -> Map.empty
                                        let rel =
                                            match prov.GetSchemaCache().Relationships.TryGetValue(t.FullName) with
                                            | true,rel -> rel
                                            | false,_ -> ([||],[||])
                                        (cols,rel))]

            let sprocData =
                lazy
                    match con with
                    | Some con -> prov.GetSprocs con
                    | None -> prov.GetSchemaCache().Sprocs |> Seq.toList

            let getTableData name = tableColumns.Force().[name].Force()
            let designTimeDc = lazy SqlDataContext(rootTypeName, conString, dbVendor, resolutionPath, config.ReferencedAssemblies, config.RuntimeAssembly, owner, caseSensitivity, tableNames, contextSchemaPath, odbcquote, sqliteLibrary, transactionOptions, None, SelectOperations.DotNetSide, ssdtPath, true)
            // first create all the types so we are able to recursively reference them in each other's definitions
            let baseTypes =
                lazy
                    dict [ let tablesforced = tables.Force()
                           if Array.isEmpty tablesforced then
                                let hint =
                                    match con with
                                    | Some con ->
                                        match caseSensitivity with
                                        | CaseSensitivityChange.ORIGINAL | CaseSensitivityChange.TOLOWER
                                                when prov.GetTables(con,CaseSensitivityChange.TOUPPER).Length > 0 ->
                                            ". Try adding parameter SqlDataProvider<CaseSensitivityChange=Common.CaseSensitivityChange.TOUPPER, ...> \r\nConnection: " + connectionString
                                        | CaseSensitivityChange.ORIGINAL | CaseSensitivityChange.TOUPPER
                                                when prov.GetTables(con,CaseSensitivityChange.TOLOWER).Length > 0 ->
                                            ". Try adding parameter SqlDataProvider<CaseSensitivityChange=Common.CaseSensitivityChange.TOLOWER, ...> \r\nConnection: " + connectionString
                                        | _ when owner = "" -> ". Try adding parameter SqlDataProvider<Owner=...> where Owner value is database name or schema. \r\nConnection: " + connectionString
                                        | _ -> " for schema or database " + owner + ". Connection: " + connectionString
                                    | None -> ""
                                let possibleError = "Tables not found" + hint
                                let errInfo =
                                    ProvidedProperty("PossibleError", typeof<String>, getterCode = fun _ -> <@@ possibleError @@>)
                                errInfo.AddXmlDocDelayed(fun () ->
                                    invalidate()
                                    "You have possible configuration error. \r\n " + possibleError)
                                serviceType.AddMember errInfo
                           else
                           for table in tablesforced do
                            let t = ProvidedTypeDefinition(table.FullName + "Entity", Some typeof<SqlEntity>, isErased=true)
                            let fullname = table.FullName
                            t.AddMemberDelayed(fun () -> ProvidedConstructor([ProvidedParameter("dataContext",typeof<ISqlDataContext>)],
                                                            fun args ->
                                                                let a0 = args.[0]
                                                                try
                                                                    <@@ ((%%a0 : obj) :?> ISqlDataContext).CreateEntity(fullname) @@>
                                                                with
                                                                | :? ArgumentException ->
                                                                    <@@ (%%a0 : ISqlDataContext).CreateEntity(fullname) @@>
                                                                ))
                            let desc = (sprintf "An instance of the %s %s belonging to schema %s" table.Type table.Name table.Schema)
                            t.AddXmlDoc desc
                            yield table.FullName,(t,sprintf "The %s %s belonging to schema %s" table.Type table.Name table.Schema,"", table.Schema) ]

            let baseCollectionTypes =
                lazy
                    dict [ for table in tables.Force() do
                            let name = table.FullName
                            match baseTypes.Force().TryGetValue name with
                            | true, (et,_,_,_) ->
                                let ct = ProvidedTypeDefinition(name, Some typeof<obj>,isErased=true)
                                ct.AddInterfaceImplementationsDelayed( fun () -> [ProvidedTypeBuilder.MakeGenericType(typedefof<System.Linq.IQueryable<_>>,[et :> Type]); typeof<ISqlDataContext>])
                                let tableTypeDef,_,_,_ = baseTypes.Force().[table.FullName]
                                let it = createIndividualsType con prov table designTimeDc dbVendor individualsAmount tableTypeDef
                                yield name,(ct,it)
                            | false, _ -> ()]

            // add the attributes and relationships
            for KeyValue(key,(t,_,_,_)) in baseTypes.Force() do
                t.AddMembersDelayed(fun () ->
                    let (columns,(children,parents)) = getTableData key
                    let attProps =
                        let createCols = createColumnProperty con prov useOptionTypes key
                        List.map createCols (columns |> Seq.map (fun kvp -> kvp.Value) |> Seq.toList)
                    let relProps =
                        let getRelationshipName = Utilities.uniqueName()
                        let bts = baseTypes.Force()
                        let ty = typedefof<System.Linq.IQueryable<_>>
                        [ for r in children do
                           match bts.TryGetValue r.ForeignTable with
                           | true, (tt,_,_,_) ->
                                let ty = ty.MakeGenericType tt
                                let constraintName = r.Name
                                let niceName = getRelationshipName (sprintf "%s by %s" r.ForeignTable r.PrimaryKey)
                                let pt = r.PrimaryTable
                                let pk = r.PrimaryKey
                                let ft = r.ForeignTable
                                let fk = r.ForeignKey
                                let prop = ProvidedProperty(niceName,ty, getterCode = fun args ->
                                    let a0 = args.[0]
                                    <@@ (%%a0 : SqlEntity).DataContext.CreateRelated((%%a0 : SqlEntity),constraintName,pt,pk,ft,fk,RelationshipDirection.Children) @@> )
                                prop.AddXmlDoc(sprintf "Related %s entities from the foreign side of the relationship, where the primary key is %s and the foreign key is %s. Constraint: %s" r.ForeignTable r.PrimaryKey r.ForeignKey constraintName)
                                yield prop
                            | false, _ -> ()
                                ] @
                        [ for r in parents do
                           match bts.TryGetValue r.PrimaryTable with
                           | true, (tt,_,_,_) ->
                                let ty = ty.MakeGenericType tt
                                let constraintName = r.Name
                                let niceName = getRelationshipName (sprintf "%s by %s" r.PrimaryTable r.PrimaryKey)
                                let pt = r.PrimaryTable
                                let pk = r.PrimaryKey
                                let ft = r.ForeignTable
                                let fk = r.ForeignKey
                                let prop = ProvidedProperty(niceName,ty, getterCode = fun args ->
                                    let a0 = args.[0]
                                    <@@ (%%a0 : SqlEntity).DataContext.CreateRelated((%%a0 : SqlEntity),constraintName,pt, pk,ft, fk,RelationshipDirection.Parents) @@> )
                                prop.AddXmlDoc(sprintf "Related %s entities from the primary side of the relationship, where the primary key is %s and the foreign key is %s. Constraint: %s" r.PrimaryTable r.PrimaryKey r.ForeignKey constraintName)
                                yield prop
                           | false, _ -> ()
                                ]
                    attProps @ relProps)

            let tableTypes = baseTypes.Force()
            let containers =
                let sprocs =
                    match con with
                    | None -> prov.GetSchemaCache().Sprocs |> Seq.toList
                    | Some _ ->
                        let sprocList = sprocData.Force()
                        prov.GetSchemaCache().Sprocs.AddRange sprocList
                        sprocList
                generateTypeTree con prov Map.empty sprocs

            let addServiceTypeMembers (isReadonly:bool) =
                [
                  if not isReadonly then
                      yield! containers |> Seq.cast<MemberInfo>

                  let tableTypes =
                      if not isReadonly then tableTypes
                      else [] |> dict // Readonly shares the same schema and table types.


                  let templateContainer = ProvidedTypeDefinition("TemplateAsRecord", Some typeof<obj>, isErased=true)
                  templateContainer.AddXmlDocDelayed(fun () -> "As this is erasing TypeProvider, you can use the generated types. However, if you need manual access to corresponding type, e.g. to use it in reflection, this will generate you a template of the runtime type. Copy and paste this to use however you will (e.g. with MapTo).")


                  for (KeyValue(key,(entityType,desc,_,schema))) in tableTypes do
                      // collection type, individuals type
                      let (ct,it) = baseCollectionTypes.Force().[key]
                      let schemaType = getOrAddSchema args schema

                      let templateTable = ProvidedTypeDefinition(ct.Name+"Template", Some typeof<obj>, isErased=true)
                      templateTable.AddMemberDelayed(fun () ->

                            let columns, _ = getTableData key

                            let optType =
                                match useOptionTypes with
                                | NullableColumnType.OPTION -> " option"
                                | NullableColumnType.VALUE_OPTION -> " voption"
                                | NullableColumnType.NO_OPTION
                                | _ -> ""
                            let template=
                                let items =
                                    columns
                                    |> Map.toArray
                                    |> Array.map(fun (s,v) -> (SchemaProjections.nicePascalName v.Name) + " : " + (Utilities.getType v.TypeMapping.ClrType).Name + (if v.IsNullable then optType else ""))
                                "type " + (SchemaProjections.nicePascalName key) + " = { " + (String.concat "; " items) + " }"
                            let p = ProvidedProperty(template, typeof<obj>, isStatic = true, getterCode = empty)
                            p.AddXmlDoc("Remove quotes and copy paste this to your code.")
                            p :> MemberInfo
                        )
                      templateContainer.AddMember templateTable

                      ct.AddMembersDelayed( fun () ->
                          // creation methods.
                          // we are forced to load the columns here, but this is ok as the user has already
                          // pressed . on an IQueryable type so they are obviously interested in using this entity..
                          let columns, _ = getTableData key

                          let requiredColumns =
                              columns
                              |> Map.toArray
                              |> Array.map (fun (s,c) -> c)
                              |> Array.filter (fun c -> (not c.IsNullable) && (not c.IsAutonumber) && (not c.IsComputed))

                          let backwardCompatibilityOnly =
                              requiredColumns
                              |> Array.filter (fun c-> not c.IsPrimaryKey)
                              |> Array.map(fun c -> ProvidedParameter(c.Name,Utilities.getType c.TypeMapping.ClrType))
                              |> Array.sortBy(fun p -> p.Name)
                              |> Array.toList

                          let normalParameters =
                              requiredColumns
                              |> Array.map(fun c -> ProvidedParameter(c.Name,Utilities.getType c.TypeMapping.ClrType))
                              |> Array.sortBy(fun p -> p.Name)
                              |> Array.toList

                          if isReadonly then

                              seq {
                                  if not (ct.DeclaredProperties |> Seq.exists(fun m -> m.Name = "Individuals")) then
                                      let individuals = ProvidedProperty("Individuals",Seq.head it, getterCode = fun args ->
                                          let a0 = args.[0]
                                          <@@ ((%%a0 : obj ):?> IWithDataContext ).DataContext @@> )
                                      individuals.AddXmlDoc("<summary>Get individual items from the table. Requires single primary key.</summary>")
                                      yield individuals :> MemberInfo
                               } |> Seq.toList

                          else

                          // Create: unit -> SqlEntity
                          let create1 = ProvidedMethod("Create", [], entityType, invokeCode = fun args ->
                              let a0 = args.[0]
                              <@@
                                  let e = ((%%a0 : obj ):?> IWithDataContext).DataContext.CreateEntity(key)
                                  e._State <- Created
                                  ((%%a0 : obj ):?> IWithDataContext ).DataContext.SubmitChangedEntity e
                                  e
                              @@> )

                          // Create: ('a * 'b * 'c * ...) -> SqlEntity
                          let create2 =
                              if List.isEmpty normalParameters then Unchecked.defaultof<ProvidedMethod> else
                              ProvidedMethod("Create", normalParameters, entityType, invokeCode = fun args ->

                                let dc = args.Head
                                let args = args.Tail
                                let columns =
                                    Expr.NewArray(
                                            typeof<string*obj>,
                                            args
                                            |> List.mapi(fun i v -> Expr.NewTuple [ Expr.Value normalParameters.[i].Name
                                                                                    Expr.Coerce(v, typeof<obj>) ] ))
                                <@@
                                    let e = ((%%dc : obj ):?> IWithDataContext).DataContext.CreateEntity(key)
                                    e._State <- Created
                                    e.SetData(%%columns : (string *obj) array)
                                    ((%%dc : obj ):?> IWithDataContext ).DataContext.SubmitChangedEntity e
                                    e
                                @@>)

                          // Create: ('a * 'b * 'c * ...) -> SqlEntity
                          let create2old =
                              if List.isEmpty backwardCompatibilityOnly || normalParameters.Length = backwardCompatibilityOnly.Length then Unchecked.defaultof<ProvidedMethod> else
                              ProvidedMethod("Create", backwardCompatibilityOnly, entityType, invokeCode = fun args ->

                                let dc = args.Head
                                let args = args.Tail
                                let columns =
                                    Expr.NewArray(
                                            typeof<string*obj>,
                                            args
                                            |> List.mapi(fun i v -> Expr.NewTuple [ Expr.Value backwardCompatibilityOnly.[i].Name
                                                                                    Expr.Coerce(v, typeof<obj>) ] ))
                                <@@
                                    let e = ((%%dc : obj ):?> IWithDataContext).DataContext.CreateEntity(key)
                                    e._State <- Created
                                    e.SetData(%%columns : (string *obj) array)
                                    ((%%dc : obj ):?> IWithDataContext ).DataContext.SubmitChangedEntity e
                                    e
                                @@>)

                          // Create: (data : seq<string*obj>) -> SqlEntity
                          let create3 = ProvidedMethod("Create", [ProvidedParameter("data",typeof< (string*obj) seq >)] , entityType, invokeCode = fun args ->
                                let dc = args.[0]
                                let data = args.[1]
                                <@@
                                    let e = ((%%dc : obj ):?> IWithDataContext).DataContext.CreateEntity(key)
                                    e._State <- Created
                                    e.SetData(%%data : (string * obj) seq)
                                    ((%%dc : obj ):?> IWithDataContext ).DataContext.SubmitChangedEntity e
                                    e
                                @@>)
                          let desc3 =
                              let cols = requiredColumns |> Seq.map(fun c -> c.Name)
                              "Item array of database columns: \r\n" + (String.concat ","  cols)
                          create3.AddXmlDoc (sprintf "<summary>%s</summary>" desc3)

                          // ``Create(...)``: ('a * 'b * 'c * ...) -> SqlEntity
                          let create4 =
                              if List.isEmpty normalParameters then Unchecked.defaultof<ProvidedMethod> else
                              let template=
                                  let cols = normalParameters |> Seq.map(fun c -> c.Name )
                                  "Create(" + (String.concat ", " cols) + ")"
                              ProvidedMethod(template, normalParameters, entityType, invokeCode = fun args ->
                                let dc = args.Head
                                let args = args.Tail
                                let columns =
                                    Expr.NewArray(
                                            typeof<string*obj>,
                                            args
                                            |> List.mapi(fun i v -> Expr.NewTuple [ Expr.Value normalParameters.[i].Name
                                                                                    Expr.Coerce(v, typeof<obj>) ] ))
                                <@@
                                    let e = ((%%dc : obj ):?> IWithDataContext).DataContext.CreateEntity(key)
                                    e._State <- Created
                                    e.SetData(%%columns : (string *obj) array)
                                    ((%%dc : obj ):?> IWithDataContext ).DataContext.SubmitChangedEntity e
                                    e
                                @@>)

                          let minimalParameters =
                              requiredColumns
                              |> Array.filter (fun c-> (not c.HasDefault))
                              |> Array.map(fun c -> ProvidedParameter(c.Name,Utilities.getType c.TypeMapping.ClrType))
                              |> Array.sortBy(fun p -> p.Name)
                              |> Array.toList

                          // ``Create(...)``: ('a * 'b * 'c * ...) -> SqlEntity
                          let create4old =
                              if List.isEmpty backwardCompatibilityOnly || backwardCompatibilityOnly.Length = normalParameters.Length ||
                                  backwardCompatibilityOnly.Length = minimalParameters.Length then Unchecked.defaultof<ProvidedMethod> else
                              let template=
                                  let cols = backwardCompatibilityOnly |> Seq.map(fun c -> c.Name )
                                  "Create(" + (String.concat ", " cols) + ")"
                              ProvidedMethod(template, backwardCompatibilityOnly, entityType, invokeCode = fun args ->
                                let dc = args.Head
                                let args = args.Tail
                                let columns =
                                    Expr.NewArray(
                                            typeof<string*obj>,
                                            args
                                            |> List.mapi(fun i v -> Expr.NewTuple [ Expr.Value backwardCompatibilityOnly.[i].Name
                                                                                    Expr.Coerce(v, typeof<obj>) ] ))
                                <@@
                                    let e = ((%%dc : obj ):?> IWithDataContext).DataContext.CreateEntity(key)
                                    e._State <- Created
                                    e.SetData(%%columns : (string *obj) array)
                                    ((%%dc : obj ):?> IWithDataContext ).DataContext.SubmitChangedEntity e
                                    e
                                @@>)

                          // ``Create(...)``: ('a * 'b * 'c * ...) -> SqlEntity
                          let create5 =
                              if List.isEmpty minimalParameters || normalParameters.Length = minimalParameters.Length then Unchecked.defaultof<ProvidedMethod> else
                              let template=
                                  let cols = minimalParameters |> Seq.map(fun c -> c.Name )
                                  "Create(" + (String.concat ", " cols) + ")"
                              ProvidedMethod(template, minimalParameters, entityType, invokeCode = fun args ->
                                let dc = args.Head
                                let args = args.Tail
                                let columns =
                                    Expr.NewArray(
                                            typeof<string*obj>,
                                            args
                                            |> List.mapi(fun i v -> Expr.NewTuple [ Expr.Value minimalParameters.[i].Name
                                                                                    Expr.Coerce(v, typeof<obj>) ] ))
                                <@@
                                    let e = ((%%dc : obj ):?> IWithDataContext).DataContext.CreateEntity(key)
                                    e._State <- Created
                                    e.SetData(%%columns : (string *obj) array)
                                    ((%%dc : obj ):?> IWithDataContext ).DataContext.SubmitChangedEntity e
                                    e
                                @@>)

                          seq {
                              if not (ct.DeclaredProperties |> Seq.exists(fun m -> m.Name = "Individuals")) then
                                  let individuals = ProvidedProperty("Individuals",Seq.head it, getterCode = fun args ->
                                      let a0 = args.[0]
                                      <@@ ((%%a0 : obj ):?> IWithDataContext ).DataContext @@> )
                                  individuals.AddXmlDoc("<summary>Get individual items from the table. Requires single primary key.</summary>")
                                  yield individuals :> MemberInfo
                              if normalParameters.Length > 0 then yield create2 :> MemberInfo
                              if backwardCompatibilityOnly.Length > 0 && normalParameters.Length <> backwardCompatibilityOnly.Length then
                                 create2old.AddXmlDoc("This will be obsolete soon. Migrate away from this!")
                                 yield create2old :> MemberInfo
                              yield create3 :> MemberInfo
                              yield create1 :> MemberInfo
                              if normalParameters.Length > 0 then
                                 create4.AddXmlDoc("Create version that breaks if your columns change. Only non-nullable parameters.")
                                 yield create4 :> MemberInfo
                              if minimalParameters.Length > 0 && normalParameters.Length <> minimalParameters.Length then
                                 create5.AddXmlDoc("Create version that breaks if your columns change. No default value parameters.")
                                 yield create5 :> MemberInfo
                              if backwardCompatibilityOnly.Length > 0 && backwardCompatibilityOnly.Length <> normalParameters.Length &&
                                 backwardCompatibilityOnly.Length <> minimalParameters.Length then
                                     create4old.AddXmlDoc("This will be obsolete soon. Migrate away from this!")
                                     yield create4old :> MemberInfo

                           } |> Seq.toList
                      )

                      let buildTableName = SchemaProjections.buildTableName >> caseInsensitivityCheck
                      let prop = ProvidedProperty(buildTableName(ct.Name),ct, getterCode = fun args ->
                          let a0 = args.[0]
                          <@@ ((%%a0 : obj) :?> ISqlDataContext).CreateEntities(key) @@> )
                      let tname = ct.Name
                      match con with
                      | Some con ->
                          prop.AddXmlDocDelayed (fun () ->
                              let details = prov.GetTableDescription(con, tname).Replace("<","&lt;").Replace(">","&gt;")
                              let separator = if (String.IsNullOrWhiteSpace desc) || (String.IsNullOrWhiteSpace details) then "" else "/"
                              sprintf "<summary>%s %s %s</summary>" details separator desc)
                      | None ->
                          prop.AddXmlDocDelayed (fun () -> "<summary>Offline mode.</summary>")
                          ()
                      schemaType.AddMember ct
                      if not (schemaType.DeclaredMembers |> Seq.exists(fun m -> m.Name = prop.Name)) then
                          schemaType.AddMember prop

                      yield entityType :> MemberInfo
                      //yield ct         :> MemberInfo
                      //yield prop       :> MemberInfo
                      yield! Seq.cast<MemberInfo> it

                  if not isReadonly then
                     yield! containers |> Seq.map(fun p -> ProvidedProperty(p.Name.Replace("Container",""), p, getterCode = fun args ->
                          let a0 = args.[0]
                          <@@ ((%%a0 : obj) :?> ISqlDataContext) @@>)) |> Seq.cast<MemberInfo>
                     let submit = ProvidedMethod("SubmitUpdates",[],typeof<unit>, invokeCode = fun args ->
                         let a0 = args.[0]
                         <@@ ((%%a0 : obj) :?> ISqlDataContext).SubmitPendingChanges() @@>)
                     submit.AddXmlDoc("<summary>Save changes to data-source. May throws errors: To deal with non-saved items use GetUpdates() and ClearUpdates().</summary>")
                     yield submit :> MemberInfo
                     let submitAsync = ProvidedMethod("SubmitUpdatesAsync",[],typeof<System.Threading.Tasks.Task>, invokeCode = fun args ->
                         let a0 = args.[0]
                         <@@ ((%%a0 : obj) :?> ISqlDataContext).SubmitPendingChangesAsync() :> Task @@>)
                     submitAsync.AddXmlDoc("<summary>Save changes to data-source. May throws errors: Use Async.Catch and to deal with non-saved items use GetUpdates() and ClearUpdates().</summary>")
                     yield submitAsync :> MemberInfo
                     yield ProvidedMethod("GetUpdates",[],typeof<SqlEntity list>, invokeCode = fun args ->
                         let a0 = args.[0]
                         <@@ ((%%a0 : obj) :?> ISqlDataContext).GetPendingEntities() @@>)  :> MemberInfo
                     yield ProvidedMethod("ClearUpdates",[],typeof<SqlEntity list>, invokeCode = fun args ->
                         let a0 = args.[0]
                         <@@ ((%%a0 : obj) :?> ISqlDataContext).ClearPendingChanges() @@>)  :> MemberInfo
                  yield ProvidedMethod("CreateConnection",[],typeof<IDbConnection>, invokeCode = fun args ->
                      let a0 = args.[0]
                      <@@ ((%%a0 : obj) :?> ISqlDataContext).CreateConnection() @@>)  :> MemberInfo

                  if not isReadonly then
                      let recreate = fun () -> createTypes rootType serviceType readServiceType config sqlRuntimeInfo invalidate registerDispose (connectionString, conStringName,dbVendor,resolutionPath,individualsAmount,useOptionTypes,owner,caseSensitivity, tableNames, contextSchemaPath, odbcquote, sqliteLibrary, ssdtPath, rootTypeName)
                      let designTimeCommandsContainer, saveResponse, mOld, designTime, invalidateActionResponse =
                            createDesignTimeCommands prov contextSchemaPath recreate invalidate

                      designTimeCommandsContainer.AddMember saveResponse
                      designTimeCommandsContainer.AddMember templateContainer
                      yield mOld :> MemberInfo

                      let clearCacheType = ProvidedTypeDefinition("ClearConnectionCache", Some typeof<obj>, isErased=true)
                      clearCacheType.AddMember(ProvidedConstructor([], empty))
                      clearCacheType.AddMembersDelayed(fun () ->

                          try
                              match con with
                              | Some con when not (isNull con) && con.State <> ConnectionState.Closed -> con.Close()
                              | _ -> ()
                          with
                          | :? ObjectDisposedException -> ()

                          invalidate()
                          DesignTimeCacheSchema.schemaMap.Clear()

                          GC.Collect()

                          [ ProvidedProperty("Done",typeof<unit>, getterCode = empty) :> MemberInfo ]
                      )
                      clearCacheType.AddXmlDocComputed(fun () -> "Close possible design-time database connection and clear connection cache")
                      let clearC = ProvidedProperty("ClearConnection", (clearCacheType :> Type), getterCode = empty)
                      clearC.AddXmlDocComputed(fun () -> "You can try to use this to refresh your database connection if you changed your database schema.")

                      designTimeCommandsContainer.AddMember clearC
                      designTimeCommandsContainer.AddMember clearCacheType

                      match invalidateActionResponse with
                      | Some r -> designTimeCommandsContainer.AddMember r
                      | None -> ()

                      serviceType.AddMember designTimeCommandsContainer
                      yield designTime :> MemberInfo

                 ] @ [
                    for KeyValue((cachedargs,name),pt) in DesignTimeCacheSchema.schemaMap do
                        if args = cachedargs then
                            yield pt :> MemberInfo
                            yield ProvidedProperty(SchemaProjections.buildTableName(name),pt, getterCode = fun args ->
                                let a0 = args.[0]
                                <@@ ((%%a0 : obj) :?> ISqlDataContext) @@> ) :> MemberInfo
                 ]

            serviceType.AddMembers(addServiceTypeMembers false)
            serviceType.AddXmlDoc("Use dataContext to explore database schema and querying data. It will carry database-connection and possible modifications within transaction, that you can commit via SubmitUpdates.")
            rootType.AddMembers [ serviceType ]

            readServiceType.AddMembersDelayed( fun () -> addServiceTypeMembers true)
            readServiceType.AddXmlDoc("readDataContext to be used in schema exploration and querying. Like dataContext but not so easy to do accidental mutations of context state.")
            rootType.AddMembersDelayed(fun () -> [ readServiceType ])
            serviceType.AddMemberDelayed(fun () ->
                        let p = ProvidedMethod("AsReadOnly", [], readServiceType, invokeCode = fun args ->
                            let a0 = args.[0]
                            <@@ ((%%a0 : obj) :?> ISqlDataContext) @@> )
                        p.AddXmlDoc ("Context can be casted as readonly to use it when function takes a readonly parameter. Type corresponds to return of GetReadOnlyDataContext()")
                        p :> MemberInfo)

            match con with
            | Some con -> if (dbVendor <> DatabaseProviderTypes.MSACCESS) && con.State <> ConnectionState.Closed then con.Close()
            | None -> ()

            ()

        let createConstructors (config:TypeProviderConfig) (rootType:ProvidedTypeDefinition, serviceType, readServiceType, args) =
            let struct(connectionString, conStringName,dbVendor,resolutionPath,individualsAmount,useOptionTypes,owner,caseSensitivity, tableNames, contextSchemaPath, odbcquote, sqliteLibrary, ssdtPath, rootTypeName) = args

            let referencedAssemblyExpr = QuotationHelpers.arrayExpr config.ReferencedAssemblies |> snd
            let resolutionFolder = config.ResolutionFolder

            // using a magic number for the lack of a command timeout; otherwise we'd need to convert an untyped Expr to an untyped option Expr
            // i'm pretty sure no SQL driver actually wants this as a value. still, can this be handled better?
            let NO_COMMAND_TIMEOUT = Int32.MinValue

            // these are the definitions for the parameters that may appear in the .GetDataContext() overload
            let customConnStr =
              "connectionString",
              "The database runtime connection string",
              typeof<string>

            let customResPath =
              "resolutionPath",
              "The location to look for dynamically loaded assemblies containing database vendor specific connections and custom types.  Types used in design-time: If no better clue, prefer .NET Standard 2.0 versions. Semicolon to separate multiple.",
              typeof<string>

            let customTransOpts =
              "transactionOptions",
              "TransactionOptions for the transaction created on SubmitChanges.",
              typeof<TransactionOptions>

            let customCmdTimeout =
              "commandTimeout",
              "SQL command timeout. Maximum time for single SQL-command in seconds.",
              typeof<int>

            let customSelectOps =
              "selectOperations",
              "Execute select-clause operations in SQL database rather than .NET-side.",
              typeof<SelectOperations>

            // these are the default values to be used if the .GetDataContext() overload doesn't include the parameter
            let defaultConnStr =
                <@@ match ConfigHelpers.tryGetConnectionString true resolutionFolder conStringName connectionString with
                    | "" -> failwithf "No connection string specified or could not find a connection string with name %s" conStringName
                    | cs -> cs
                @@>
            let defaultResPath = <@@ resolutionPath @@>
            let defaultCmdTimeout = <@@ NO_COMMAND_TIMEOUT @@>
            let defaultTransOpts = <@@ TransactionOptions.Default @@>
            let defaultSelectOps = <@@ SelectOperations.DotNetSide @@>

            let optionPairs = [|
              customConnStr, defaultConnStr
              customResPath, defaultResPath
              customTransOpts, defaultTransOpts
              customCmdTimeout, defaultCmdTimeout
              customSelectOps, defaultSelectOps
            |]

            // generates the .GetDataContext() overloads
            // each parameter can either be present (passed as argument at runtime) or missing (uses the default value)
            let overloads = [|
              [| |]
              [| customConnStr |]
              [| customConnStr; customResPath |]
              [| customConnStr; customTransOpts |]
              [| customConnStr; customResPath; customTransOpts |]
              [| customConnStr; customCmdTimeout |]
              [| customConnStr; customResPath; customCmdTimeout |]
              [| customConnStr; customTransOpts; customCmdTimeout |]
              [| customConnStr; customResPath; customTransOpts; customCmdTimeout |]
              [| customTransOpts |]
              [| customCmdTimeout |]
              [| customTransOpts; customCmdTimeout |]
              [| customSelectOps |]
              [| customConnStr; customSelectOps |]
              [| customConnStr; customTransOpts; customSelectOps |]
              [| customConnStr; customCmdTimeout; customSelectOps |]
              [| customConnStr; customResPath; customTransOpts; customCmdTimeout; customSelectOps |]
            |]

            rootType.AddMembersDelayed (fun () ->
                [

                    for overload in overloads do

                        let actualParams = [|
                            for (customParam, defaultParam) in optionPairs do
                                match overload |> Array.exists ((=) customParam) with
                                | true -> yield UserProvided customParam
                                | false -> yield Default defaultParam
                        |]

                        // The code that gets actually executed
                        let inline invoker (isReadOnly:bool) (args: Expr list) =

                              let actualArgs =
                                    [|
                                        let mutable argPosition = 0
                                        for actualParam in actualParams do
                                            match actualParam with
                                            // if the parameter appears, we read it from the argument list and advance
                                            | UserProvided _ -> yield args.[argPosition]; argPosition <- argPosition + 1
                                            // otherwise, we use the default value
                                            | Default p -> yield p
                                    |]

                              <@@
                                  let cmdTimeout =
                                      let argTimeout = %%actualArgs.[3]
                                      if argTimeout = NO_COMMAND_TIMEOUT then None else Some argTimeout

                                  // **important**: contextSchemaPath is empty because we do not want
                                  // to load the schema cache from (the developer's) local filesystem in production
                                  SqlDataContext(typeName = rootTypeName, connectionString = %%actualArgs.[0], providerType = dbVendor,
                                                  resolutionPath = %%actualArgs.[1], referencedAssemblies = %%referencedAssemblyExpr,
                                                  runtimeAssembly = resolutionFolder, owner = owner, caseSensitivity = caseSensitivity,
                                                  tableNames = tableNames, contextSchemaPath = "", odbcquote = odbcquote,
                                                  sqliteLibrary = sqliteLibrary, transactionOptions = %%actualArgs.[2],
                                                  commandTimeout = cmdTimeout, sqlOperationsInSelect = %%actualArgs.[4], ssdtPath = ssdtPath, isReadOnly=isReadOnly)
                                  :> ISqlDataContext
                              @@>

                        // builds the definitions
                        let paramList =
                            [ for actualParam in actualParams do
                                  match actualParam with
                                  | UserProvided(pname, pcomment, ptype) -> yield pname, pcomment, ptype
                                  | _ -> ()
                            ]

                        let providerParams =
                            [ for (pname, _, ptype) in paramList -> ProvidedParameter(pname, ptype)]

                        let xmlComments =
                            [|  yield "<summary>Returns an instance of the SQL Provider using the static parameters</summary>"
                                for (pname, xmlInfo, _) in paramList -> "<param name='" + pname + "'>" + xmlInfo + "</param>"
                            |]

                        let method =
                            ProvidedMethod( methodName = "GetDataContext"
                                          , parameters = providerParams
                                          , returnType = serviceType
                                          , isStatic = true
                                          , invokeCode = invoker false
                                          )

                        method.AddXmlDoc (String.Concat xmlComments)

                        yield method

                        let xmlComments2 =
                            [|  yield "<summary>Returns an instance of the SQL Provider using the static parameters, without direct access to modify data.</summary>"
                                for (pname, xmlInfo, _) in paramList -> "<param name='" + pname + "'>" + xmlInfo + "</param>"
                            |]

                        let rmethod =
                            ProvidedMethod( methodName = "GetReadOnlyDataContext"
                                          , parameters = providerParams
                                          , returnType = readServiceType
                                          , isStatic = true
                                          , invokeCode = invoker true
                                          )

                        rmethod.AddXmlDoc (String.Concat xmlComments2)

                        yield rmethod

                ])
            ()

open DesignTimeUtils

module DesignReflection =
    let execAssembly = lazy System.Reflection.Assembly.GetExecutingAssembly()

type SqlRuntimeInfo (config : TypeProviderConfig) =
    let runtimeAssembly =
        DesignReflection.execAssembly.Force()
        //let r = Reflection.tryLoadAssemblyFrom "" [||] [config.RuntimeAssembly]
        //match r with
        //| Choice1Of2(assembly) -> assembly
        //| Choice2Of2(paths, errors) -> Assembly.GetExecutingAssembly()
    member __.RuntimeAssembly = runtimeAssembly

module DesignTimeCache =
    let cache = System.Collections.Concurrent.ConcurrentDictionary<DesignCacheKey,Lazy<ProvidedTypeDefinition> * DateTime>()

/// The idea of this is trying to avoid case where compile-time has loaded non-runtime assembly. (Happens in .NET 8.0, not in .NET Framework.)
/// So let's load compile-time (and design-time) manually the required runtime assembly.
module internal FixReferenceAssemblies =
#if MSSQL
    AppContext.SetSwitch("Switch.Microsoft.Data.SqlClient.UseManagedNetworkingOnWindows", true); // No Windows SNI in design-time
#endif

    let pathsToSeek() =
        let ifNotNull (x:Assembly) =
            if isNull x then ""
            elif String.IsNullOrWhiteSpace x.Location then ""
            else x.Location |> System.IO.Path.GetDirectoryName

        [__SOURCE_DIRECTORY__;
#if !INTERACITVE
            DesignReflection.execAssembly.Force() |> ifNotNull;
#endif
            Environment.CurrentDirectory;
            System.Reflection.Assembly.GetEntryAssembly() |> ifNotNull;]

    let manualLoadNet8Runtime =
        lazy
            let isWindows = System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows)
            let isMac = System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.OSX)

            let libraries =
                [|
        #if MSSQL
                    System.IO.Path.Combine [| "runtimes"; (if isWindows then "win" else "unix"); "lib"; "net8.0"; "System.Data.SqlClient.dll" |]
                    System.IO.Path.Combine [| "runtimes"; (if isWindows then "win" else "unix"); "lib"; "net8.0";"Microsoft.Data.SqlClient.dll" |]
        #endif
        #if MSACCESS
                    System.IO.Path.Combine [| "runtimes"; "win"; "lib"; "net8.0"; "System.Diagnostics.EventLog.Messages.dll" |]
                    System.IO.Path.Combine [| "runtimes"; "win"; "lib"; "net8.0"; "System.Diagnostics.EventLog.dll" |]
                    System.IO.Path.Combine [| "runtimes"; "win"; "lib"; "net8.0"; "System.Diagnostics.PerformanceCounter.dll" |]
                    System.IO.Path.Combine [| "runtimes"; "win"; "lib"; "net6.0"; "System.Data.OleDb.dll" |]
        #endif
        #if ODBC
                    System.IO.Path.Combine [| "runtimes"; (
                            if isWindows then "win"
                            elif isMac then "osx"
                            elif System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Linux) then "linux"
                            elif System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Create "FreeBSD") then "freebsd"
                            else ""
                        ); "lib"; "net6.0"; "System.Data.Odbc.dll" |]
        #endif
        #if ORACLE
                    System.IO.Path.Combine [| "runtimes"; (
                            if isWindows then "win"
                            elif isMac then "osx"
                            elif System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Linux) then "linux"
                            else ""
                        ); "lib"; "net8.0"; "System.DirectoryServices.Protocols.dll" |]
                    if isWindows then
                        System.IO.Path.Combine [| "runtimes"; "win"; "lib"; "net8.0"; "System.Diagnostics.EventLog.Messages.dll" |]
                        System.IO.Path.Combine [| "runtimes"; "win"; "lib"; "net8.0"; "System.Diagnostics.EventLog.dll" |]
                        System.IO.Path.Combine [| "runtimes"; "win"; "lib"; "net8.0"; "System.Diagnostics.PerformanceCounter.dll" |]
                        System.IO.Path.Combine [| "runtimes"; "win"; "lib"; "net8.0"; "System.Security.Cryptography.Pkcs.dll" |]
                    if System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Create "Browser") then
                        System.IO.Path.Combine [| "runtimes"; "browser"; "lib"; "net8.0"; "System.Text.Encodings.Web.dll" |]
        #endif
        #if POSTGRES
                    if System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Create "Browser") then
                        System.IO.Path.Combine [| "runtimes"; "browser"; "lib"; "net8.0"; "System.Text.Encodings.Web.dll" |]
        #endif
                |]

        #if DUCKDB
            let isArm = System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture.ToString().StartsWith "Arm"
            let isLinux = System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Linux)
            pathsToSeek() |> List.iter(fun basePath ->
                let nativeLibrary = System.IO.Path.Combine [| basePath; "runtimes"; (
                        if isWindows then
                            if isArm then "win-arm64"
                            else "win-x64"
                        elif isMac then
                            "osx"
                        elif isLinux then
                            if isArm then "linux-arm64"
                            else "linux-x64"
                        else ""
                    ); "native" |]
                if System.IO.Directory.Exists nativeLibrary then
                    Environment.SetEnvironmentVariable("Path", Environment.GetEnvironmentVariable("Path") + ";" + nativeLibrary) // Path for native duckdb.dll
                ()
            )

        #endif
        #if SQLITE

            let isLinux = System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Linux)
            pathsToSeek() |> List.iter(fun basePath ->
                let nativeLibrary = System.IO.Path.Combine [| basePath; "runtimes"; (
                        if isWindows then
                            match System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture with
                            | System.Runtime.InteropServices.Architecture.X64 -> "win-x64"
                            | System.Runtime.InteropServices.Architecture.Arm64 -> "win-arm64"
                            | System.Runtime.InteropServices.Architecture.X86 -> "win-x86"
                            | System.Runtime.InteropServices.Architecture.Arm -> "win-arm"
                            | _ -> ""
                        elif isMac then
                            if System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture.ToString().StartsWith "Arm" then "osx-arm64"
                            else "osx-x64"
                        elif isLinux then
                            "linux-" + System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture.ToString().ToLowerInvariant()
                        else ""
                    ); "native" |]

                if System.IO.Directory.Exists nativeLibrary then
                    Environment.SetEnvironmentVariable("Path", Environment.GetEnvironmentVariable("Path") + ";" + nativeLibrary) // Path for native libraries (net8.0)

                let anotherLocation =
                    System.IO.Path.Combine [| basePath; (if System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture = System.Runtime.InteropServices.Architecture.X64 then "x64" else "x86") |]

                if System.IO.Directory.Exists anotherLocation then
                    Environment.SetEnvironmentVariable("Path", Environment.GetEnvironmentVariable("Path") + ";" + anotherLocation) // net462

                ()
            )
        #endif


            if libraries |> Array.isEmpty then true
            else

            let tryLoad (asmPath:string) =
                // Only Net8.0 compile-time need fixing. Path doesn't exist in other targetFrameworks.
                if not (System.IO.Directory.Exists asmPath) then ()
                else
                    let checkAndLoad (file:string) =
                        let fileToSeek = asmPath + System.IO.Path.DirectorySeparatorChar.ToString() + file
                        if System.IO.File.Exists (fileToSeek) then
                            try
                                Assembly.LoadFrom fileToSeek |> ignore
                            with
                            | e ->
                                ()
                        ()
                    libraries |> Array.iter checkAndLoad
                    ()
            pathsToSeek() |> List.iter tryLoad
            true

[<TypeProvider>]
type public SqlTypeProvider(config: TypeProviderConfig) as this =
    inherit TypeProviderForNamespaces(config, assemblyReplacementMap=[design1, runtime1;
                                                                      design2, runtime2], addDefaultProbingLocation=true)
    let asm = DesignReflection.execAssembly.Force()

    // check we contain a copy of runtime files, and are not referencing the runtime DLL
    do assert (typeof<SqlDataContext>.Assembly.GetName().Name = asm.GetName().Name)
#if !COMMON
    let _ = FixReferenceAssemblies.manualLoadNet8Runtime.Force()
#endif
    let sqlRuntimeInfo = SqlRuntimeInfo(config)
    let mySaveLock = new Object();
    let mutable saveInProcess = false

    let empty = fun (_:Expr list) -> <@@ () @@>

    let registerDispose = fun (con, dbVendor) ->
        this.Disposing.Add(fun _ ->
            if con <> Unchecked.defaultof<IDbConnection> && dbVendor <> DatabaseProviderTypes.MSACCESS
            then con.Dispose())

    let invalidate = fun _ ->
            this.Invalidate()
            DesignTimeCache.cache.Clear()

    let paramSqlType = ProvidedTypeDefinition(sqlRuntimeInfo.RuntimeAssembly, FSHARP_DATA_SQL, "SqlDataProvider", Some(typeof<obj>), isErased=true)

    let conString = ProvidedStaticParameter("ConnectionString",typeof<string>, "")
    let connStringName = ProvidedStaticParameter("ConnectionStringName", typeof<string>, "")
    let optionTypes = ProvidedStaticParameter("UseOptionTypes",typeof<NullableColumnType>,NullableColumnType.NO_OPTION)
    let dbVendor = ProvidedStaticParameter("DatabaseVendor",typeof<DatabaseProviderTypes>,DatabaseProviderTypes.MSSQLSERVER)
    let individualsAmount = ProvidedStaticParameter("IndividualsAmount",typeof<int>,50)
    let owner = ProvidedStaticParameter("Owner", typeof<string>, "")
    let resolutionPath = ProvidedStaticParameter("ResolutionPath",typeof<string>, "")
    let caseSensitivity = ProvidedStaticParameter("CaseSensitivityChange",typeof<CaseSensitivityChange>,CaseSensitivityChange.ORIGINAL)
    let tableNames = ProvidedStaticParameter("TableNames", typeof<string>, "")
    let contextSchemaPath = ProvidedStaticParameter("ContextSchemaPath", typeof<string>, "")
    let odbcquote = ProvidedStaticParameter("OdbcQuote", typeof<OdbcQuoteCharacter>, OdbcQuoteCharacter.DEFAULT_QUOTE)
    let sqliteLibrary = ProvidedStaticParameter("SQLiteLibrary",typeof<SQLiteLibrary>,SQLiteLibrary.AutoSelect)
    let ssdtPath = ProvidedStaticParameter("SsdtPath", typeof<string>, "")
    let helpText = "<summary>Typed representation of a database</summary>
                    <param name='ConnectionString'>The connection string for the SQL database</param>
                    <param name='ConnectionStringName'>The connection string name to select from a configuration file</param>
                    <param name='DatabaseVendor'> The target database vendor</param>
                    <param name='IndividualsAmount'>The amount of sample entities to project into the type system for each SQL entity type. Default 50. Note GDPR/PII regulations if using individuals with ContextSchemaPath.</param>
                    <param name='UseOptionTypes'>If set, F# option types will be used in place of nullable database columns.  If not, you will always receive the default value of the column's type even if it is null in the database.</param>
                    <param name='ResolutionPath'>The location to look for dynamically loaded assemblies containing database vendor specific connections and custom types. Types used in design-time: If no better clue, prefer .NET Standard 2.0 versions. Semicolon to separate multiple.</param>
                    <param name='Owner'>Oracle: The owner of the schema for this provider to resolve. PostgreSQL: A list of schemas to resolve, separated by spaces, newlines, commas, or semicolons.</param>
                    <param name='CaseSensitivityChange'>Should we do ToUpper or ToLower when generating table names?</param>
                    <param name='TableNames'>Comma separated table names list to limit a number of tables in big instances. The names can have '%' sign to handle it as in the 'LIKE' query (Oracle and MSSQL Only)</param>
                    <param name='ContextSchemaPath'>The location of the context schema previously saved with SaveContextSchema. When not empty, will be used to populate the database schema instead of retrieving it from then database.</param>
                    <param name='OdbcQuote'>Odbc quote characters: Quote characters for the table and column names: `alias`, [alias]</param>
                    <param name='SQLiteLibrary'>Use System.Data.SQLite or Mono.Data.SQLite or select automatically (SQLite only)</param>
                    <param name='SsdtPath'>A path to an SSDT .dacpac file.'</param>
                    "

    do paramSqlType.DefineStaticParameters([dbVendor;conString;connStringName;resolutionPath;individualsAmount;optionTypes;owner;caseSensitivity; tableNames; contextSchemaPath; odbcquote; sqliteLibrary; ssdtPath], fun typeName args ->

        let arguments =
          struct (
            args.[1] :?> string,                    // ConnectionString URL
            args.[2] :?> string,                    // ConnectionString Name
            args.[0] :?> DatabaseProviderTypes,     // db vendor
            args.[3] :?> string,                    // Assembly resolution path for db connectors and custom types
            args.[4] :?> int,                       // Individuals Amount
            args.[5] :?> NullableColumnType,                      // Use option types?
            args.[6] :?> string,                    // Schema owner currently only used for oracle
            args.[7] :?> CaseSensitivityChange,     // Should we do ToUpper or ToLower when generating table names?
            args.[8] :?> string,                    // Table names list (Oracle and MSSQL Only)
            args.[9] :?> string,                    // Context schema path
            args.[10] :?> OdbcQuoteCharacter,       // Quote characters (Odbc only)
            args.[11] :?> SQLiteLibrary,            // Use System.Data.SQLite or Mono.Data.SQLite or select automatically (SQLite only)
            args.[12] :?> string,                   // SSDT Path
            typeName)

        let addCache args =
            lazy
                let struct(connectionString, conStringName,dbVendor,resolutionPath,individualsAmount,useOptionTypes,owner,caseSensitivity, tableNames, contextSchemaPath, odbcquote, sqliteLibrary, ssdtPath, rootTypeName) = args

                let rootType = ProvidedTypeDefinition(sqlRuntimeInfo.RuntimeAssembly,FSHARP_DATA_SQL,rootTypeName,Some typeof<obj>, isErased=true)
                let serviceType = ProvidedTypeDefinition( "dataContext", Some typeof<obj>, isErased=true)
                let readServiceType = ProvidedTypeDefinition( "readDataContext", Some typeof<obj>, isErased=true)

                createTypes rootType serviceType readServiceType config sqlRuntimeInfo invalidate registerDispose args
                createConstructors config (rootType, serviceType, readServiceType, args)

                // This is not a perfect cache-invalidation solution, it can remove a valid item from
                // cache after the time-out, causing one extra hit, but this is only a design-time cache
                // and it will work well enough to deal with Visual Studio's multi-threading problems
                let expiration = TimeSpan.FromMinutes 3
                let rec invalidationFunction key =
                    async {
                        do! Async.Sleep (int expiration.TotalMilliseconds)

                        match DesignTimeCache.cache.TryGetValue key with
                        | true, (_, timestamp) ->
                            if DateTime.UtcNow - timestamp >= expiration then
                                DesignTimeCache.cache.TryRemove key |> ignore
                            else
                                do! invalidationFunction key
                        | _ -> ()

                    }
                invalidationFunction args |> Async.Start
                rootType
            , DateTime.UtcNow
        try (DesignTimeCache.cache.GetOrAdd(arguments, addCache) |> fst).Value
        with
        | e ->
            let _ = DesignTimeCache.cache.TryRemove(arguments)
            let _ =
                lock mySaveLock (fun() ->
                    let keysToClear =
                        DesignTimeCacheSchema.schemaMap.Keys
                        |> Seq.toList
                        |> List.filter(fun (a,k) -> a = arguments)
                    keysToClear |> Seq.iter(fun ak ->
                        let _ = DesignTimeCacheSchema.schemaMap.TryRemove ak
                        ()
                    )
                )
            reraise()
    )

    do paramSqlType.AddXmlDoc helpText

    // add them to the namespace
    do this.AddNamespace(FSHARP_DATA_SQL, [paramSqlType])
