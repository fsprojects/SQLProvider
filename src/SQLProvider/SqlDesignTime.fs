namespace FSharp.Data.Sql

open System
open System.Data
open System.Reflection
open Microsoft.FSharp.Core.CompilerServices
open Microsoft.FSharp.Quotations
open FSharp.Data.Sql.Transactions
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Runtime
open FSharp.Data.Sql.Common
open FSharp.Data.Sql
open ProviderImplementation.ProvidedTypes

type internal SqlRuntimeInfo (config : TypeProviderConfig) =
    let runtimeAssembly = 
        Assembly.GetExecutingAssembly()
        //let r = Reflection.tryLoadAssemblyFrom "" [||] [config.RuntimeAssembly]
        //match r with
        //| Choice1Of2(assembly) -> assembly
        //| Choice2Of2(paths, errors) -> Assembly.GetExecutingAssembly()
    member __.RuntimeAssembly = runtimeAssembly 

module internal DesignTimeCache = 
    let cache = System.Collections.Concurrent.ConcurrentDictionary<_,ProvidedTypeDefinition>()

type internal ParameterValue =
  | UserProvided of string * string * Type
  | Default of Expr

[<TypeProvider>]
type SqlTypeProvider(config: TypeProviderConfig) as this =     
    inherit TypeProviderForNamespaces(config)
    let sqlRuntimeInfo = SqlRuntimeInfo(config)
    let mySaveLock = new Object();
    
    let [<Literal>] FSHARP_DATA_SQL = "FSharp.Data.Sql"
    let empty = fun (_:Expr list) -> <@@ () @@>
    
    let createTypes(connectionString, conStringName,dbVendor,resolutionPath,individualsAmount,useOptionTypes,owner,caseSensitivity, tableNames, contextSchemaPath, odbcquote, sqliteLibrary, rootTypeName) = 
        let resolutionPath = 
            if String.IsNullOrWhiteSpace resolutionPath
            then config.ResolutionFolder
            else resolutionPath

        let caseInsensitivityCheck = 
            match caseSensitivity with
            | CaseSensitivityChange.TOLOWER -> (fun (x:string) -> x.ToLower())
            | CaseSensitivityChange.TOUPPER -> (fun (x:string) -> x.ToUpper())
            | _ -> (fun x -> x)

        let conString = 
            match ConfigHelpers.tryGetConnectionString false config.ResolutionFolder conStringName connectionString with
            | "" -> failwithf "No connection string specified or could not find a connection string with name %s" conStringName
            | cs -> cs
                    
        let rootType, prov, con = 
            let rootType = ProvidedTypeDefinition(sqlRuntimeInfo.RuntimeAssembly,FSHARP_DATA_SQL,rootTypeName,Some typeof<obj>, isErased=true)
            let prov = ProviderBuilder.createProvider dbVendor resolutionPath config.ReferencedAssemblies config.RuntimeAssembly owner tableNames contextSchemaPath odbcquote sqliteLibrary
            match prov.GetSchemaCache().IsOffline with
            | false ->
                let con = prov.CreateConnection conString
                this.Disposing.Add(fun _ -> 
                    if con <> Unchecked.defaultof<IDbConnection> && dbVendor <> DatabaseProviderTypes.MSACCESS then
                        con.Dispose())
                con.Open()
                prov.CreateTypeMappings con
                rootType, prov, Some con
            | true ->
            rootType, prov, None
        
        let tables = 
            lazy
                match con with
                | Some con -> prov.GetTables(con,caseSensitivity)
                | None -> prov.GetSchemaCache().Tables |> Seq.map (fun kv -> kv.Value) |> Seq.toList

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
                                        | false,_ -> ([],[])
                                    (cols,rel))]

        let sprocData = 
            lazy
                match con with
                | Some con -> prov.GetSprocs con
                | None -> prov.GetSchemaCache().Sprocs |> Seq.toList

        let getSprocReturnColumns sprocname (sprocDefinition: CompileTimeSprocDefinition) param =
            match con with
            | Some con -> 
                let returnParams = sprocDefinition.ReturnColumns con param
                prov.GetSchemaCache().SprocsParams.AddOrUpdate(sprocname, returnParams, fun _ inputParams -> inputParams @ returnParams) |> ignore
                returnParams
            | None -> 
                let ok, pars = prov.GetSchemaCache().SprocsParams.TryGetValue sprocname
                if ok then
                    pars |> List.filter (fun p -> p.Direction = ParameterDirection.Output)
                else []

        let getTableData name = tableColumns.Force().[name].Force()
        let serviceType = ProvidedTypeDefinition( "dataContext", None, isErased=true)
        let transactionOptions = TransactionOptions.Default
        let designTimeDc = SqlDataContext(rootTypeName, conString, dbVendor, resolutionPath, config.ReferencedAssemblies, config.RuntimeAssembly, owner, caseSensitivity, tableNames, contextSchemaPath, odbcquote, sqliteLibrary, transactionOptions, None, SelectOperations.DotNetSide)
        // first create all the types so we are able to recursively reference them in each other's definitions
        let baseTypes =
            lazy
                dict [ let tablesforced = tables.Force()
                       if tablesforced.Length = 0 then
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
                                this.Invalidate()
                                "You have possible configuration error. \r\n " + possibleError)
                            serviceType.AddMember errInfo
                       else                
                       for table in tablesforced do
                        let t = ProvidedTypeDefinition(table.FullName + "Entity", Some typeof<SqlEntity>, isErased=true)
                        t.AddMemberDelayed(fun () -> ProvidedConstructor([ProvidedParameter("dataContext",typeof<ISqlDataContext>)],
                                                        fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).CreateEntity(table.FullName) @@>))
                        let desc = (sprintf "An instance of the %s %s belonging to schema %s" table.Type table.Name table.Schema)
                        t.AddXmlDoc desc
                        yield table.FullName,(t,sprintf "The %s %s belonging to schema %s" table.Type table.Name table.Schema,"", table.Schema) ]

        let createIndividualsType (table:Table) =
            let tableTypeDef,_,_,_ = baseTypes.Force().[table.FullName]
            let t = ProvidedTypeDefinition(table.Schema + "." + table.Name + "." + "Individuals", None, isErased=true)
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
                   let entities =
                        match con with
                        | Some con ->
                            use com = prov.CreateCommand(con,prov.GetIndividualsQueryText(table,individualsAmount))
                            if con.State <> ConnectionState.Open then con.Open()
                            use reader = com.ExecuteReader()
                            let ret = (designTimeDc :> ISqlDataContext).ReadEntities(table.FullName, columns, reader)
                            if (dbVendor <> DatabaseProviderTypes.MSACCESS) then con.Close()
                            if ret.Length > 0 then 
                                prov.GetSchemaCache().Individuals.AddRange ret
                            ret
                        | None -> prov.GetSchemaCache().Individuals |> Seq.toArray
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
                        let ty = ProvidedTypeDefinition(name, None, isErased=true)
                        ty.AddMember(ProvidedConstructor([ProvidedParameter("sqlService", typeof<ISqlDataContext>)], empty))
                        individualsTypes.Add ty
                        Some(col.Key,(ty,ProvidedProperty(sprintf "As %s" (buildFieldName col.Key),ty, getterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext)@@> ))))
                      |> Map.ofSeq
                 
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
                      |> Array.choose(fun e -> 
                         match e.GetColumn pkName with
                         | FixedType pkValue -> 
                            
                            let tableName = table.FullName
                            let getterCode (args : Expr list) = <@@ ((%%args.[0] : obj) :?> ISqlDataContext).GetIndividual(tableName, pkValue) @@> 

                            // this next bit is just side effect to populate the "As Column" types for the supported columns
                            for colName, colValue in e.ColumnValues do                                         
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
                         | _ -> None)
                      |> Array.append( propertyMap |> Map.toArray |> Array.map (snd >> snd))

                   propertyMap 
                   |> Map.toSeq
                   |> Seq.map (snd >> fst) 
                   |> Seq.cast<MemberInfo>
                   |> Seq.append (props |> Seq.cast<MemberInfo>)
                   |> Seq.toList

               | None -> [])
            individualsTypes :> seq<_> 
            
        let baseCollectionTypes =
            lazy
                dict [ for table in tables.Force() do  
                        let name = table.FullName
                        let (et,_,_,_) = baseTypes.Force().[name]
                        let ct = ProvidedTypeDefinition(table.FullName, None ,isErased=true)                        
                        ct.AddInterfaceImplementationsDelayed( fun () -> [ProvidedTypeBuilder.MakeGenericType(typedefof<System.Linq.IQueryable<_>>,[et :> Type]); typeof<ISqlDataContext>])
                        let it = createIndividualsType table 
                        yield table.FullName,(ct,it) ]
        
        // add the attributes and relationships
        for KeyValue(key,(t,_,_,_)) in baseTypes.Force() do 
            t.AddMembersDelayed(fun () -> 
                let (columns,(children,parents)) = getTableData key
                let attProps = 
                    let createColumnProperty (c:Column) =
                        let nullable = useOptionTypes && c.IsNullable
                        let ty = Type.GetType c.TypeMapping.ClrType
                        let propTy = if nullable then typedefof<option<_>>.MakeGenericType(ty) else ty
                        let name = c.Name
                        let prop = 
                            ProvidedProperty(
                                SchemaProjections.buildFieldName(name),propTy, 
                                getterCode = (fun (args:Expr list) ->
                                    let meth = if nullable then typeof<SqlEntity>.GetMethod("GetColumnOption").MakeGenericMethod([|ty|])
                                               else  typeof<SqlEntity>.GetMethod("GetColumn").MakeGenericMethod([|ty|])
                                    Expr.Call(args.[0],meth,[Expr.Value name])
                                ),
                                setterCode = (fun (args:Expr list) ->
                                    if nullable then 
                                        let meth = typeof<SqlEntity>.GetMethod("SetColumnOption").MakeGenericMethod([|ty|])
                                        Expr.Call(args.[0],meth,[Expr.Value name;args.[1]])
                                    else      
                                        let meth = typeof<SqlEntity>.GetMethod("SetColumn").MakeGenericMethod([|ty|])
                                        Expr.Call(args.[0],meth,[Expr.Value name;args.[1]]))
                                 )
                        let nfo = c.TypeInfo
                        let typeInfo = match nfo with None -> "" | Some x -> x.ToString() 
                        match con with
                        | Some con ->
                            prop.AddXmlDocDelayed(fun () -> 
                                let details = prov.GetColumnDescription(con, key, name).Replace("<","&lt;").Replace(">","&gt;")
                                let separator = if (String.IsNullOrWhiteSpace typeInfo) || (String.IsNullOrWhiteSpace details) then "" else "/"
                                sprintf "<summary>%s %s %s</summary>" (String.Join(": ", [|name; details|])) separator typeInfo)
                        | None -> 
                            prop.AddXmlDocDelayed(fun () -> sprintf "<summary>Offline mode. %s</summary>" typeInfo)
                            ()
                        prop
                    List.map createColumnProperty (columns |> Seq.map (fun kvp -> kvp.Value) |> Seq.toList)
                let relProps = 
                    let getRelationshipName = Utilities.uniqueName() 
                    let bts = baseTypes.Force()       
                    [ for r in children do       
                       if bts.ContainsKey(r.ForeignTable) then
                        let (tt,_,_,_) = bts.[r.ForeignTable]
                        let ty = typedefof<System.Linq.IQueryable<_>>
                        let ty = ty.MakeGenericType tt
                        let constraintName = r.Name
                        let niceName = getRelationshipName (sprintf "%s by %s" r.ForeignTable r.PrimaryKey) 
                        let prop = ProvidedProperty(niceName,ty, getterCode = fun args -> 
                            let pt = r.PrimaryTable
                            let pk = r.PrimaryKey
                            let ft = r.ForeignTable
                            let fk = r.ForeignKey
                            <@@ (%%args.[0] : SqlEntity).DataContext.CreateRelated((%%args.[0] : SqlEntity),constraintName,pt,pk,ft,fk,RelationshipDirection.Children) @@> )
                        prop.AddXmlDoc(sprintf "Related %s entities from the foreign side of the relationship, where the primary key is %s and the foreign key is %s. Constriant: %s" r.ForeignTable r.PrimaryKey r.ForeignKey constraintName)
                        yield prop ] @
                    [ for r in parents do
                       if bts.ContainsKey(r.PrimaryTable) then
                        let (tt,_,_,_) = (bts.[r.PrimaryTable])
                        let ty = typedefof<System.Linq.IQueryable<_>>
                        let ty = ty.MakeGenericType tt
                        let constraintName = r.Name
                        let niceName = getRelationshipName (sprintf "%s by %s" r.PrimaryTable r.PrimaryKey)
                        let prop = ProvidedProperty(niceName,ty, getterCode = fun args -> 
                            let pt = r.PrimaryTable
                            let pk = r.PrimaryKey
                            let ft = r.ForeignTable
                            let fk = r.ForeignKey
                            <@@ (%%args.[0] : SqlEntity).DataContext.CreateRelated((%%args.[0] : SqlEntity),constraintName,pt, pk,ft, fk,RelationshipDirection.Parents) @@> )
                        prop.AddXmlDoc(sprintf "Related %s entities from the primary side of the relationship, where the primary key is %s and the foreign key is %s. Constraint: %s" r.PrimaryTable r.PrimaryKey r.ForeignKey constraintName)
                        yield prop ]
                attProps @ relProps)
        
        let generateSprocMethod (container:ProvidedTypeDefinition) (con:IDbConnection option) (sproc:CompileTimeSprocDefinition) =    
            
            let sprocname = SchemaProjections.buildSprocName(sproc.Name.DbName) 
                            |> SchemaProjections.avoidNameClashBy (container.GetMember >> Array.isEmpty >> not)

            let rt = ProvidedTypeDefinition(sprocname,None, isErased=true)
            let resultType = ProvidedTypeDefinition("Result", None, isErased=true)
            resultType.AddMember(ProvidedConstructor([ProvidedParameter("sqlDataContext", typeof<ISqlDataContext>)], empty))
            rt.AddMember resultType
            container.AddMember(rt)
            
            resultType.AddMembersDelayed(fun () ->
                    let sprocParameters = 
                        let cache = prov.GetSchemaCache()
                        match con with
                        | None -> 
                            match cache.SprocsParams.TryGetValue sprocname with
                            | true, x -> x
                            | false, _ -> []
                        | Some con ->
                            Sql.ensureOpen con
                            let ps = sproc.Params con  
                            cache.SprocsParams.AddOrUpdate(sprocname, ps, fun _ _ -> ps) |> ignore
                            ps

                    let parameters =
                        sprocParameters
                        |> List.filter (fun p -> p.Direction = ParameterDirection.Input || p.Direction = ParameterDirection.InputOutput)
                        |> List.map(fun p -> ProvidedParameter(p.Name,Type.GetType p.TypeMapping.ClrType))
                    let retCols = getSprocReturnColumns sprocname sproc sprocParameters |> List.toArray
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
                                  let ty = Type.GetType col.TypeMapping.ClrType
                                  let prop = 
                                      ProvidedProperty(
                                          name,ty,
                                          getterCode = (fun (args:Expr list) ->
                                              let meth = typeof<SqlEntity>.GetMethod("GetColumn").MakeGenericMethod([|ty|])
                                              Expr.Call(args.[0],meth,[Expr.Value name])),
                                          setterCode = (fun (args:Expr list) ->
                                              let meth = typeof<SqlEntity>.GetMethod("SetColumn").MakeGenericMethod([|typeof<obj>|])
                                              Expr.Call(args.[0],meth,[Expr.Value name;Expr.Coerce(args.[1], typeof<obj>)])))
                                  rt.AddMember prop)
                              resultType.AddMember(rt)
                              rt :> Type
                    let retColsExpr =
                        QuotationHelpers.arrayExpr retCols |> snd
                    let asyncRet = typedefof<Async<_>>.MakeGenericType([| returnType |])
                    [ProvidedMethod("Invoke", parameters, returnType, invokeCode = QuotationHelpers.quoteRecord runtimeSproc (fun args var -> 
                        <@@ (((%%args.[0] : obj):?>ISqlDataContext)).CallSproc(%%var, %%retColsExpr,  %%Expr.NewArray(typeof<obj>,List.map(fun e -> Expr.Coerce(e,typeof<obj>)) args.Tail)) @@>));
                     ProvidedMethod("InvokeAsync", parameters, asyncRet, invokeCode = QuotationHelpers.quoteRecord runtimeSproc (fun args var -> 
                        if returnType = typeof<unit> then 
                            <@@ async {
                                    let! r = (((%%args.[0] : obj):?>ISqlDataContext)).CallSprocAsync(%%var, %%retColsExpr,  %%Expr.NewArray(typeof<obj>,List.map(fun e -> Expr.Coerce(e,typeof<obj>)) args.Tail))
                                    return ()
                                } @@>
                        else
                           <@@ (((%%args.[0] : obj):?>ISqlDataContext)).CallSprocAsync(%%var, %%retColsExpr,  %%Expr.NewArray(typeof<obj>,List.map(fun e -> Expr.Coerce(e,typeof<obj>)) args.Tail))  @@>
                        ))]
            )
                              
            let niceUniqueSprocName = 
                SchemaProjections.buildSprocName(sproc.Name.ProcName)
                |> SchemaProjections.avoidNameClashBy (container.GetProperty >> (<>) null)

            let p = ProvidedProperty(niceUniqueSprocName, resultType, getterCode = (fun args -> <@@ ((%%args.[0] : obj) :?>ISqlDataContext) @@>) ) 
            let dbName = sproc.Name.DbName
            p.AddXmlDocDelayed(fun _ -> sprintf "<summary>%s</summary>" dbName)
            p
            
        
        let rec walkSproc con (path:string list) (parent:ProvidedTypeDefinition option) (createdTypes:Map<string list,ProvidedTypeDefinition>) (sproc:Sproc) =
            match sproc with
            | Root(typeName, next) -> 
                let path = (path @ [typeName])
                match createdTypes.TryFind path with
                | Some(typ) -> 
                    walkSproc con path (Some typ) createdTypes next 
                | None ->
                    let typ = ProvidedTypeDefinition(typeName, None, isErased=true)
                    typ.AddMember(ProvidedConstructor([ProvidedParameter("sqlDataContext", typeof<ISqlDataContext>)], empty))
                    walkSproc con path (Some typ) (createdTypes.Add(path, typ)) next 
            | Package(typeName, packageDefn) ->       
                match parent with
                | Some(parent) ->
                    let path = (path @ [typeName])
                    let typ = ProvidedTypeDefinition(typeName, None, isErased=true)
                    parent.AddMember(typ)
                    parent.AddMember(ProvidedProperty(SchemaProjections.nicePascalName typeName, typ, getterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext) @@>))
                    typ.AddMember(ProvidedConstructor([ProvidedParameter("sqlDataContext", typeof<ISqlDataContext>)], empty))
                    match con with
                    | Some co ->
                        typ.AddMembersDelayed(fun () -> 
                            Sql.ensureOpen co
                            let p = (packageDefn.Sprocs co) 
                            prov.GetSchemaCache().Packages.AddRange p
                            p |> List.map (generateSprocMethod typ con)) 
                    | None -> 
                        typ.AddMembersDelayed(fun () ->  
                        prov.GetSchemaCache().Packages |> Seq.toList |> List.map (generateSprocMethod typ con)) 
                    createdTypes.Add(path, typ)
                | _ -> failwithf "Could not generate package path type undefined root or previous type"    
            | Sproc(sproc) ->
                    match parent with
                    | Some(parent) ->
                        match con with
                        | Some co ->
                            parent.AddMemberDelayed(fun () -> Sql.ensureOpen co;  generateSprocMethod parent con sproc); createdTypes
                        | None -> 
                            parent.AddMemberDelayed(fun () -> generateSprocMethod parent con sproc); createdTypes
                    | _ -> failwithf "Could not generate sproc undefined root or previous type"
            | Empty -> createdTypes

        let rec generateTypeTree con (createdTypes:Map<string list, ProvidedTypeDefinition>) (sprocs:Sproc list) = 
            match sprocs with
            | [] -> 
                Map.filter (fun (k:string list) _ -> match k with [_] -> true | _ -> false) createdTypes
                |> Map.toSeq
                |> Seq.map snd
            | sproc::rest -> generateTypeTree con (walkSproc con [] None createdTypes sproc) rest

        serviceType.AddMembersDelayed( fun () ->
            let schemaMap = new System.Collections.Generic.Dictionary<string, ProvidedTypeDefinition>()
            let getOrAddSchema name = 
                match schemaMap.TryGetValue name with
                | true, pt -> pt
                | false, _  -> 
                    let pt = ProvidedTypeDefinition(name + "Schema", Some typeof<obj>, isErased=true)
                    schemaMap.Add(name, pt)
                    pt
            [ 
              let containers = 
                    let sprocs = 
                        match con with
                        | None -> prov.GetSchemaCache().Sprocs |> Seq.toList
                        | Some _ ->
                            let sprocList = sprocData.Force()
                            prov.GetSchemaCache().Sprocs.AddRange sprocList
                            sprocList
                    generateTypeTree con Map.empty sprocs
              yield! containers |> Seq.cast<MemberInfo>

              for (KeyValue(key,(entityType,desc,_,schema))) in baseTypes.Force() do
                // collection type, individuals type
                let (ct,it) = baseCollectionTypes.Force().[key]
                let schemaType = getOrAddSchema schema
                
                ct.AddMembersDelayed( fun () -> 
                    // creation methods.
                    // we are forced to load the columns here, but this is ok as the user has already 
                    // pressed . on an IQueryable type so they are obviously interested in using this entity..                    
                    let columns, _ = getTableData key

                    let requiredColumns =
                        columns
                        |> Seq.toArray
                        |> Array.map (fun kvp -> kvp.Value)
                        |> Array.filter (fun c-> (not c.IsNullable) && (not c.IsAutonumber))

                    let backwardCompatibilityOnly =
                        requiredColumns
                        |> Array.filter (fun c-> not c.IsPrimaryKey)
                        |> Array.map(fun c -> ProvidedParameter(c.Name,Type.GetType c.TypeMapping.ClrType))
                        |> Array.sortBy(fun p -> p.Name)
                        |> Array.toList

                    let normalParameters = 
                        requiredColumns 
                        |> Array.map(fun c -> ProvidedParameter(c.Name,Type.GetType c.TypeMapping.ClrType))
                        |> Array.sortBy(fun p -> p.Name)
                        |> Array.toList

                    // Create: unit -> SqlEntity 
                    let create1 = ProvidedMethod("Create", [], entityType, invokeCode = fun args ->                         
                        <@@ 
                            let e = ((%%args.[0] : obj ):?> IWithDataContext).DataContext.CreateEntity(key)
                            e._State <- Created
                            ((%%args.[0] : obj ):?> IWithDataContext ).DataContext.SubmitChangedEntity e
                            e 
                        @@> )
                    
                    // Create: ('a * 'b * 'c * ...) -> SqlEntity 
                    let create2 = 
                        if normalParameters.Length = 0 then Unchecked.defaultof<ProvidedMethod> else
                        ProvidedMethod("Create", normalParameters, entityType, invokeCode = fun args -> 
                          
                          let dc = args.Head
                          let args = args.Tail
                          let columns =
                              Expr.NewArray(
                                      typeof<string*obj>,
                                      args
                                      |> Seq.toList
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
                        if backwardCompatibilityOnly.Length = 0 || normalParameters.Length = backwardCompatibilityOnly.Length then Unchecked.defaultof<ProvidedMethod> else
                        ProvidedMethod("Create", backwardCompatibilityOnly, entityType, invokeCode = fun args -> 
                          
                          let dc = args.Head
                          let args = args.Tail
                          let columns =
                              Expr.NewArray(
                                      typeof<string*obj>,
                                      args
                                      |> Seq.toList
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
                        "Item array of database columns: \r\n" + String.Join(",", cols)
                    create3.AddXmlDoc (sprintf "<summary>%s</summary>" desc3)

                    // ``Create(...)``: ('a * 'b * 'c * ...) -> SqlEntity 
                    let create4 = 
                        if normalParameters.Length = 0 then Unchecked.defaultof<ProvidedMethod> else
                        let template=
                            let cols = normalParameters |> Seq.map(fun c -> c.Name )
                            "Create(" + String.Join(", ", cols) + ")"
                        ProvidedMethod(template, normalParameters, entityType, invokeCode = fun args -> 
                          let dc = args.Head
                          let args = args.Tail
                          let columns =
                              Expr.NewArray(
                                      typeof<string*obj>,
                                      args
                                      |> Seq.toList
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
                        |> Array.map(fun c -> ProvidedParameter(c.Name,Type.GetType c.TypeMapping.ClrType))
                        |> Array.sortBy(fun p -> p.Name)
                        |> Array.toList

                    // ``Create(...)``: ('a * 'b * 'c * ...) -> SqlEntity 
                    let create4old = 
                        if backwardCompatibilityOnly.Length = 0 || backwardCompatibilityOnly.Length = normalParameters.Length ||
                            backwardCompatibilityOnly.Length = minimalParameters.Length then Unchecked.defaultof<ProvidedMethod> else
                        let template=
                            let cols = backwardCompatibilityOnly |> Seq.map(fun c -> c.Name )
                            "Create(" + String.Join(", ", cols) + ")"
                        ProvidedMethod(template, backwardCompatibilityOnly, entityType, invokeCode = fun args -> 
                          let dc = args.Head
                          let args = args.Tail
                          let columns =
                              Expr.NewArray(
                                      typeof<string*obj>,
                                      args
                                      |> Seq.toList
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
                        if minimalParameters.Length = 0 || normalParameters.Length = minimalParameters.Length then Unchecked.defaultof<ProvidedMethod> else
                        let template=
                            let cols = minimalParameters |> Seq.map(fun c -> c.Name )
                            "Create(" + String.Join(", ", cols) + ")"
                        ProvidedMethod(template, minimalParameters, entityType, invokeCode = fun args -> 
                          let dc = args.Head
                          let args = args.Tail
                          let columns =
                              Expr.NewArray(
                                      typeof<string*obj>,
                                      args
                                      |> Seq.toList
                                      |> List.mapi(fun i v -> Expr.NewTuple [ Expr.Value minimalParameters.[i].Name 
                                                                              Expr.Coerce(v, typeof<obj>) ] ))
                          <@@
                              let e = ((%%dc : obj ):?> IWithDataContext).DataContext.CreateEntity(key)
                              e._State <- Created                            
                              e.SetData(%%columns : (string *obj) array)
                              ((%%dc : obj ):?> IWithDataContext ).DataContext.SubmitChangedEntity e
                              e 
                          @@>)


                    // This genertes a template.

                    seq {
                     let individuals = ProvidedProperty("Individuals",Seq.head it, getterCode = fun args -> <@@ ((%%args.[0] : obj ):?> IWithDataContext ).DataContext @@> )
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
                let prop = ProvidedProperty(buildTableName(ct.Name),ct, getterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).CreateEntities(key) @@> )
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
                schemaType.AddMember prop

                yield entityType :> MemberInfo
                //yield ct         :> MemberInfo                
                //yield prop       :> MemberInfo
                yield! Seq.cast<MemberInfo> it

              yield! containers |> Seq.map(fun p -> ProvidedProperty(p.Name.Replace("Container",""), p, getterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext) @@>)) |> Seq.cast<MemberInfo>
              let submit = ProvidedMethod("SubmitUpdates",[],typeof<unit>, invokeCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).SubmitPendingChanges() @@>)
              submit.AddXmlDoc("<summary>Save changes to data-source. May throws errors: To deal with non-saved items use GetUpdates() and ClearUpdates().</summary>") 
              yield submit :> MemberInfo
              let submitAsync = ProvidedMethod("SubmitUpdatesAsync",[],typeof<Async<unit>>, invokeCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).SubmitPendingChangesAsync() @@>)
              submitAsync.AddXmlDoc("<summary>Save changes to data-source. May throws errors: Use Async.Catch and to deal with non-saved items use GetUpdates() and ClearUpdates().</summary>") 
              yield submitAsync :> MemberInfo
              yield ProvidedMethod("GetUpdates",[],typeof<SqlEntity list>, invokeCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).GetPendingEntities() @@>)  :> MemberInfo
              yield ProvidedMethod("ClearUpdates",[],typeof<SqlEntity list>, invokeCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).ClearPendingChanges() @@>)  :> MemberInfo
              yield ProvidedMethod("CreateConnection",[],typeof<IDbConnection>, invokeCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).CreateConnection() @@>)  :> MemberInfo
              
              let saveResponse = ProvidedTypeDefinition("SaveContextResponse",None, isErased=true)
              saveResponse.AddMember(ProvidedConstructor([], empty))
              saveResponse.AddMemberDelayed(fun () -> 
                  let result = 
                      if not(String.IsNullOrEmpty contextSchemaPath) then
                          try
                              lock mySaveLock (fun() ->
                                  prov.GetSchemaCache().Save contextSchemaPath
                                  "Saved " + contextSchemaPath + " at " + DateTime.Now.ToString("hh:mm:ss")
                              )
                          with
                          | e -> "Save failed: " + e.Message
                      else "ContextSchemaPath is not defined"
                  ProvidedMethod(result,[],typeof<unit>, invokeCode = empty) :> MemberInfo
              )
              let m = ProvidedMethod("SaveContextSchema", [], (saveResponse :> Type), invokeCode = empty)
              m.AddXmlDocComputed(fun () -> 
                  if String.IsNullOrEmpty contextSchemaPath then "ContextSchemaPath static parameter has to be defined to use this function."
                  else "Schema location: " + contextSchemaPath + ". Write dot after SaveContextSchema() to save the schema at design time."
                  )
              serviceType.AddMember saveResponse
              yield m :> MemberInfo

             ] @ [
                for KeyValue(name,pt) in schemaMap do
                    yield pt :> MemberInfo
                    yield ProvidedProperty(SchemaProjections.buildTableName(name),pt, getterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext) @@> ) :> MemberInfo
             ])
        
        rootType.AddMembers [ serviceType ]
        
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
          "The location to look for dynamically loaded assemblies containing database vendor specific connections and custom types", 
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
        let defaultResPath = <@@ resolutionFolder @@>                
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
                let invoker (args: Expr list) =

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
                                    commandTimeout = cmdTimeout, sqlOperationsInSelect = %%actualArgs.[4])
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
                                , invokeCode = invoker
                                )                        
                         
                method.AddXmlDoc (String.concat "" xmlComments)
                         
                yield method                   
            ])

        match con with
        | Some con -> if (dbVendor <> DatabaseProviderTypes.MSACCESS) then con.Close()
        | None -> ()
        rootType
    
    let paramSqlType = ProvidedTypeDefinition(sqlRuntimeInfo.RuntimeAssembly, FSHARP_DATA_SQL, "SqlDataProvider", Some(typeof<obj>), isErased=true)
    
    let conString = ProvidedStaticParameter("ConnectionString",typeof<string>, "")
    let connStringName = ProvidedStaticParameter("ConnectionStringName", typeof<string>, "")    
    let optionTypes = ProvidedStaticParameter("UseOptionTypes",typeof<bool>,false)
    let dbVendor = ProvidedStaticParameter("DatabaseVendor",typeof<DatabaseProviderTypes>,DatabaseProviderTypes.MSSQLSERVER)
    let individualsAmount = ProvidedStaticParameter("IndividualsAmount",typeof<int>,1000)
    let owner = ProvidedStaticParameter("Owner", typeof<string>, "")    
    let resolutionPath = ProvidedStaticParameter("ResolutionPath",typeof<string>, "")    
    let caseSensitivity = ProvidedStaticParameter("CaseSensitivityChange",typeof<CaseSensitivityChange>,CaseSensitivityChange.ORIGINAL)
    let tableNames = ProvidedStaticParameter("TableNames", typeof<string>, "")
    let contextSchemaPath = ProvidedStaticParameter("ContextSchemaPath", typeof<string>, "")
    let odbcquote = ProvidedStaticParameter("OdbcQuote", typeof<OdbcQuoteCharacter>, OdbcQuoteCharacter.DEFAULT_QUOTE)
    let sqliteLibrary = ProvidedStaticParameter("SQLiteLibrary",typeof<SQLiteLibrary>,SQLiteLibrary.AutoSelect)
    let helpText = "<summary>Typed representation of a database</summary>
                    <param name='ConnectionString'>The connection string for the SQL database</param>
                    <param name='ConnectionStringName'>The connection string name to select from a configuration file</param>
                    <param name='DatabaseVendor'> The target database vendor</param>
                    <param name='IndividualsAmount'>The amount of sample entities to project into the type system for each SQL entity type. Default 1000.</param>
                    <param name='UseOptionTypes'>If true, F# option types will be used in place of nullable database columns.  If false, you will always receive the default value of the column's type even if it is null in the database.</param>
                    <param name='ResolutionPath'>The location to look for dynamically loaded assemblies containing database vendor specific connections and custom types.</param>
                    <param name='Owner'>Oracle: The owner of the schema for this provider to resolve. PostgreSQL: A list of schemas to resolve, separated by spaces, newlines, commas, or semicolons.</param>
                    <param name='CaseSensitivityChange'>Should we do ToUpper or ToLower when generating table names?</param>
                    <param name='TableNames'>Comma separated table names list to limit a number of tables in big instances. The names can have '%' sign to handle it as in the 'LIKE' query (Oracle and MSSQL Only)</param>
                    <param name='ContextSchemaPath'>The location of the context schema previously saved with SaveContextSchema. When not empty, will be used to populate the database schema instead of retrieving it from then database.</param>
                    <param name='OdbcQuote'>Odbc quote characters: Quote characters for the table and column names: `alias`, [alias]</param>
                    <param name='SQLiteLibrary'>Use System.Data.SQLite or Mono.Data.SQLite or select automatically (SQLite only)</param>
                    "
        
    do paramSqlType.DefineStaticParameters([dbVendor;conString;connStringName;resolutionPath;individualsAmount;optionTypes;owner;caseSensitivity; tableNames; contextSchemaPath; odbcquote; sqliteLibrary], fun typeName args -> 
        
        let arguments =
            args.[1] :?> string,                  // ConnectionString URL
            args.[2] :?> string,                  // ConnectionString Name
            args.[0] :?> DatabaseProviderTypes,   // db vendor
            args.[3] :?> string,                  // Assembly resolution path for db connectors and custom types
            args.[4] :?> int,                     // Individuals Amount
            args.[5] :?> bool,                    // Use option types?
            args.[6] :?> string,                  // Schema owner currently only used for oracle
            args.[7] :?> CaseSensitivityChange,   // Should we do ToUpper or ToLower when generating table names?
            args.[8] :?> string,                  // Table names list (Oracle and MSSQL Only)
            args.[9] :?> string,                  // Context schema path
            args.[10] :?> OdbcQuoteCharacter,      // Quote characters (Odbc only)
            args.[11] :?> SQLiteLibrary,          // Use System.Data.SQLite or Mono.Data.SQLite or select automatically (SQLite only)
            typeName

        DesignTimeCache.cache.GetOrAdd(arguments, fun args ->
            let types = createTypes args
            
            // This is not a perfect cache-invalidation solution, it can remove a valid item from
            // cache after the time-out, causing one extra hit, but this is only a design-time cache 
            // and it will work well enough to deal with Visual Studio's multi-threading problems
            async {
                do! Async.Sleep 30000
                DesignTimeCache.cache.TryRemove args |> ignore
            } |> Async.Start
            types
            )
        )

    do paramSqlType.AddXmlDoc helpText               
    
    // add them to the namespace    
    do this.AddNamespace(FSHARP_DATA_SQL, [paramSqlType])
                            
[<assembly:TypeProviderAssembly>] 
do()


