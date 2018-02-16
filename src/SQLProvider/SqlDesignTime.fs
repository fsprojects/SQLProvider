namespace FSharp.Data.Sql

open System
open System.Data
open System.Reflection
open Microsoft.FSharp.Core.CompilerServices
open Microsoft.FSharp.Quotations
open ProviderImplementation.ProvidedTypes
open FSharp.Data.Sql.Transactions
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Runtime
open FSharp.Data.Sql.Common
open ProviderImplementation
open ProviderImplementation.ProvidedTypes
open FSharp.Data.Sql

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

[<TypeProvider>]
type SqlTypeProvider(config: TypeProviderConfig) as this =     
    inherit TypeProviderForNamespaces(config)
    let sqlRuntimeInfo = SqlRuntimeInfo(config)
    
    let [<Literal>] FSHARP_DATA_SQL = "FSharp.Data.Sql"
    let empty = fun (_:Expr list) -> <@@ () @@>
    
    let createTypes(connnectionString, conStringName,dbVendor,resolutionPath,individualsAmount,useOptionTypes,owner,caseSensitivity, tableNames, odbcquote, sqliteLibrary, rootTypeName) = 
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
            match ConfigHelpers.tryGetConnectionString false config.ResolutionFolder conStringName connnectionString with
            | "" -> failwithf "No connection string specified or could not find a connection string with name %s" conStringName
            | cs -> cs
                    
        let rootType, prov, con = 
            let rootType = ProvidedTypeDefinition(sqlRuntimeInfo.RuntimeAssembly,FSHARP_DATA_SQL,rootTypeName,Some typeof<obj>, isErased=true)
            let prov = ProviderBuilder.createProvider dbVendor resolutionPath config.ReferencedAssemblies config.RuntimeAssembly owner tableNames odbcquote sqliteLibrary
            let con = prov.CreateConnection conString
            this.Disposing.Add(fun _ -> 
                if con <> Unchecked.defaultof<IDbConnection> && dbVendor <> DatabaseProviderTypes.MSACCESS then
                    con.Dispose())
            con.Open()
            prov.CreateTypeMappings con
            rootType, prov, con
        
        let tables = lazy prov.GetTables(con,caseSensitivity)
        let tableColumns =
            lazy
                dict
                  [for t in tables.Force() do
                    yield( t.FullName, 
                        lazy
                            let cols = prov.GetColumns(con,t)
                            let rel = prov.GetRelationships(con,t)
                            (cols,rel))]
        let sprocData = lazy prov.GetSprocs con

        let getSprocReturnColumns (sprocDefinition: CompileTimeSprocDefinition) param =
            (sprocDefinition.ReturnColumns con param)

        let getTableData name = tableColumns.Force().[name].Force()
        let serviceType = ProvidedTypeDefinition( "dataContext", None, isErased=true)
        let transactionOptions = TransactionOptions.Default
        let designTimeDc = SqlDataContext(rootTypeName, conString, dbVendor, resolutionPath, config.ReferencedAssemblies, config.RuntimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, transactionOptions, None, SelectOperations.DotNetSide)
        // first create all the types so we are able to recursively reference them in each other's definitions
        let baseTypes =
            lazy
                dict [ let tablesforced = tables.Force()
                       if tablesforced.Length = 0 then
                            let hint =
                                match caseSensitivity with
                                | CaseSensitivityChange.ORIGINAL | CaseSensitivityChange.TOLOWER
                                        when prov.GetTables(con,CaseSensitivityChange.TOUPPER).Length > 0 ->
                                    ". Try adding parameter SqlDataProvider<CaseSensitivityChange=Common.CaseSensitivityChange.TOUPPER, ...> \r\nConnection: " + connnectionString
                                | CaseSensitivityChange.ORIGINAL | CaseSensitivityChange.TOUPPER 
                                        when prov.GetTables(con,CaseSensitivityChange.TOLOWER).Length > 0 ->
                                    ". Try adding parameter SqlDataProvider<CaseSensitivityChange=Common.CaseSensitivityChange.TOLOWER, ...> \r\nConnection: " + connnectionString
                                | _ when owner = "" -> ". Try adding parameter SqlDataProvider<Owner=...> where Owner value is database name or schema. \r\nConnection: " + connnectionString
                                | _ -> " for schema or database " + owner + ". Connection: " + connnectionString
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
               let columns = prov.GetColumns(con,table)
               match prov.GetPrimaryKey table with
               | Some pkName ->
                   let entities =
                        use com = prov.CreateCommand(con,prov.GetIndividualsQueryText(table,individualsAmount))
                        if con.State <> ConnectionState.Open then con.Open()
                        use reader = com.ExecuteReader()
                        let ret = (designTimeDc :> ISqlDataContext).ReadEntities(table.FullName, columns, reader)
                        if (dbVendor <> DatabaseProviderTypes.MSACCESS) then con.Close()
                        ret
                   if Array.isEmpty entities then [] else
                   // for each column in the entity except the primary key, create a new type that will read ``As Column 1`` etc
                   // inside that type the individuals will be listed again but with the text for the relevant column as the name 
                   // of the property and the primary key e.g. ``1, Dennis The Squirrel``
                   let buildFieldName = SchemaProjections.buildFieldName
                   let propertyMap =
                      prov.GetColumns(con,table)
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
                        prop.AddXmlDocDelayed(fun () -> 
                            let typeInfo = match nfo with None -> "" | Some x -> x.ToString() 
                            let details = prov.GetColumnDescription(con, key, name).Replace("<","&lt;").Replace(">","&gt;")
                            let separator = if (String.IsNullOrWhiteSpace typeInfo) || (String.IsNullOrWhiteSpace details) then "" else "/"
                            sprintf "<summary>%s %s %s</summary>" (String.Join(": ", [|name; details|])) separator typeInfo)
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
        
        let generateSprocMethod (container:ProvidedTypeDefinition) (con:IDbConnection) (sproc:CompileTimeSprocDefinition) =    
            
            let sprocname = SchemaProjections.buildSprocName(sproc.Name.DbName) 
                            |> SchemaProjections.avoidNameClashBy (container.GetMember >> Array.isEmpty >> not)

            let rt = ProvidedTypeDefinition(sprocname,None, isErased=true)
            let resultType = ProvidedTypeDefinition("Result", None, isErased=true)
            resultType.AddMember(ProvidedConstructor([ProvidedParameter("sqlDataContext", typeof<ISqlDataContext>)], empty))
            rt.AddMember resultType
            container.AddMember(rt)
            
            resultType.AddMembersDelayed(fun () ->
                    Sql.ensureOpen con
                    let sprocParameters = sproc.Params con        
                    let parameters =
                        sprocParameters
                        |> List.filter (fun p -> p.Direction = ParameterDirection.Input || p.Direction = ParameterDirection.InputOutput)
                        |> List.map(fun p -> ProvidedParameter(p.Name,Type.GetType p.TypeMapping.ClrType))
                    let retCols = getSprocReturnColumns sproc sprocParameters |> List.toArray
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
                        <@@ (((%%args.[0] : obj):?>ISqlDataContext)).CallSprocAsync(%%var, %%retColsExpr,  %%Expr.NewArray(typeof<obj>,List.map(fun e -> Expr.Coerce(e,typeof<obj>)) args.Tail)) @@>))]
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
                    typ.AddMembersDelayed(fun () -> Sql.ensureOpen con; (packageDefn.Sprocs con) |> List.map (generateSprocMethod typ con)) 
                    createdTypes.Add(path, typ)
                | _ -> failwithf "Could not generate package path type undefined root or previous type"    
            | Sproc(sproc) ->
                    match parent with
                    | Some(parent) ->
                        parent.AddMemberDelayed(fun () -> Sql.ensureOpen con;  generateSprocMethod parent con sproc); createdTypes
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
              let containers = generateTypeTree con Map.empty (sprocData.Force())
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
                        |> Seq.map (fun kvp -> kvp.Value)
                        |> Seq.filter (fun c-> (not c.IsNullable) && (not c.IsPrimaryKey))

                    let normalParameters = 
                        requiredColumns 
                        |> Seq.map(fun c -> ProvidedParameter(c.Name,Type.GetType c.TypeMapping.ClrType))
                        |> Seq.sortBy(fun p -> p.Name)
                        |> Seq.toList
                    
                    // Create: unit -> SqlEntity 
                    let create1 = ProvidedMethod("Create", [], entityType, invokeCode = fun args ->                         
                        <@@ 
                            let e = ((%%args.[0] : obj ):?> IWithDataContext).DataContext.CreateEntity(key)
                            e._State <- Created
                            ((%%args.[0] : obj ):?> IWithDataContext ).DataContext.SubmitChangedEntity e
                            e 
                        @@> )
                    
                    // Create: ('a * 'b * 'c * ...) -> SqlEntity 
                    let create2 = ProvidedMethod("Create", normalParameters, entityType, invokeCode = fun args -> 
                          
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

                    let template=
                        let cols = normalParameters |> Seq.map(fun c -> c.Name )
                        "Create(" + String.Join(", ", cols) + ")"
                    let create4 = ProvidedMethod(template, normalParameters, entityType, invokeCode = fun args -> 
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
                    create4.AddXmlDoc("Create version that breaks if your columns change.")
                    
                    // This genertes a template.

                    seq {
                     let individuals = ProvidedProperty("Individuals",Seq.head it, getterCode = fun args -> <@@ ((%%args.[0] : obj ):?> IWithDataContext ).DataContext @@> )
                     individuals.AddXmlDoc("<summary>Get individual items from the table. Requires single primary key.</summary>")
                     yield individuals :> MemberInfo
                     if normalParameters.Length > 0 then yield create2 :> MemberInfo
                     yield create3 :> MemberInfo
                     yield create1 :> MemberInfo
                     yield create4 :> MemberInfo } |> Seq.toList
                )

                let buildTableName = SchemaProjections.buildTableName >> caseInsensitivityCheck
                let prop = ProvidedProperty(buildTableName(ct.Name),ct, getterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).CreateEntities(key) @@> )
                let tname = ct.Name
                prop.AddXmlDocDelayed (fun () -> 
                    let details = prov.GetTableDescription(con, tname).Replace("<","&lt;").Replace(">","&gt;")
                    let separator = if (String.IsNullOrWhiteSpace desc) || (String.IsNullOrWhiteSpace details) then "" else "/"
                    sprintf "<summary>%s %s %s</summary>" details separator desc)
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
             ] @ [
                for KeyValue(name,pt) in schemaMap do
                    yield pt :> MemberInfo
                    yield ProvidedProperty(SchemaProjections.buildTableName(name),pt, getterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext) @@> ) :> MemberInfo
             ])
        
        let referencedAssemblyExpr = QuotationHelpers.arrayExpr config.ReferencedAssemblies |> snd
        let defaultTransactionOptionsExpr = <@@ TransactionOptions.Default @@>
        let defaultSelectOperations = <@@ SelectOperations.DotNetSide @@>
        rootType.AddMembers [ serviceType ]

        rootType.AddMembersDelayed (fun () -> 
            [ 
              let constr =     "connectionString","The database runtime connection string",typeof<string>
              let respath =    "resolutionPath", "The location to look for dynamically loaded assemblies containing database vendor specific connections and custom types",typeof<string>
              let transopt =   "transactionOptions", "TransactionOptions for the transaction created on SubmitChanges.", typeof<TransactionOptions>
              let cmdTimeout = "commandTimeout", "SQL command timeout. Maximum time for single SQL-command in seconds.", typeof<int>
              let selectOperations = "selectOperations", "Execute select-clause operations in SQL database rahter than .NET-side.", typeof<SelectOperations>

              let crossTargetParameterCombinations = [
                    [], (fun (_:Expr list) ->
                                let runtimePath = config.ResolutionFolder
                                let runtimeAssembly = config.ResolutionFolder
                                let runtimeConStr = 
                                    <@@ match ConfigHelpers.tryGetConnectionString true runtimePath conStringName connnectionString with
                                        | "" -> failwithf "No connection string specified or could not find a connection string with name %s" conStringName
                                        | cs -> cs @@>
                                <@@ SqlDataContext(rootTypeName, %%runtimeConStr, dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%defaultTransactionOptionsExpr, None, %%defaultSelectOperations) :> ISqlDataContext @@>);
                    [constr], (fun (args:Expr list) ->
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%defaultTransactionOptionsExpr, None, %%defaultSelectOperations) :> ISqlDataContext @@> );
                    [constr;respath], (fun args -> 
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, %%args.[1], %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%defaultTransactionOptionsExpr, None, %%defaultSelectOperations) :> ISqlDataContext  @@>);
                    [constr; transopt], (fun args ->
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%args.[1], None, %%defaultSelectOperations) :> ISqlDataContext @@> );
                    [constr; respath; transopt], (fun args -> 
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, %%args.[1], %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%args.[2], None, %%defaultSelectOperations) :> ISqlDataContext  @@>)
                    [constr;cmdTimeout], (fun args ->
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%defaultTransactionOptionsExpr, (Some %%args.[1]), %%defaultSelectOperations) :> ISqlDataContext @@> );
                    [constr;respath;cmdTimeout], (fun args -> 
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, %%args.[1], %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%defaultTransactionOptionsExpr, (Some %%args.[2]), %%defaultSelectOperations) :> ISqlDataContext  @@>);
                    [constr; transopt;cmdTimeout], (fun args ->
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%args.[1], (Some %%args.[2]), %%defaultSelectOperations) :> ISqlDataContext @@> );
                    [constr; respath; transopt;cmdTimeout], (fun args -> 
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, %%args.[1], %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%args.[2], (Some %%args.[3]), %%defaultSelectOperations) :> ISqlDataContext  @@>)
                    [transopt], (fun args ->
                                let runtimePath = config.ResolutionFolder
                                let runtimeAssembly = config.ResolutionFolder
                                let runtimeConStr = 
                                    <@@ match ConfigHelpers.tryGetConnectionString true runtimePath conStringName connnectionString with
                                        | "" -> failwithf "No connection string specified or could not find a connection string with name %s" conStringName
                                        | cs -> cs @@>
                                <@@ SqlDataContext(rootTypeName, %%runtimeConStr, dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%args.[0], None, %%defaultSelectOperations) :> ISqlDataContext @@>);
                    [cmdTimeout], (fun args ->
                                let runtimePath = config.ResolutionFolder
                                let runtimeAssembly = config.ResolutionFolder
                                let runtimeConStr = 
                                    <@@ match ConfigHelpers.tryGetConnectionString true runtimePath conStringName connnectionString with
                                        | "" -> failwithf "No connection string specified or could not find a connection string with name %s" conStringName
                                        | cs -> cs @@>
                                <@@ SqlDataContext(rootTypeName, %%runtimeConStr, dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%defaultTransactionOptionsExpr, (Some %%args.[0]), %%defaultSelectOperations) :> ISqlDataContext @@>);
                    [transopt;cmdTimeout], (fun args ->
                                let runtimePath = config.ResolutionFolder
                                let runtimeAssembly = config.ResolutionFolder
                                let runtimeConStr = 
                                    <@@ match ConfigHelpers.tryGetConnectionString true runtimePath conStringName connnectionString with
                                        | "" -> failwithf "No connection string specified or could not find a connection string with name %s" conStringName
                                        | cs -> cs @@>
                                <@@ SqlDataContext(rootTypeName, %%runtimeConStr, dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%args.[0], (Some %%args.[1]), %%defaultSelectOperations) :> ISqlDataContext @@>);
                    [selectOperations], (fun (args:Expr list) ->
                                let runtimePath = config.ResolutionFolder
                                let runtimeAssembly = config.ResolutionFolder
                                let runtimeConStr = 
                                    <@@ match ConfigHelpers.tryGetConnectionString true runtimePath conStringName connnectionString with
                                        | "" -> failwithf "No connection string specified or could not find a connection string with name %s" conStringName
                                        | cs -> cs @@>
                                <@@ SqlDataContext(rootTypeName, %%runtimeConStr, dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%defaultTransactionOptionsExpr, None, %%args.[0]) :> ISqlDataContext @@>);
                    [constr;selectOperations], (fun (args:Expr list) ->
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%defaultTransactionOptionsExpr, None, %%args.[1]) :> ISqlDataContext @@> );
                    [constr; transopt;selectOperations], (fun args ->
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%args.[1], None, %%args.[2]) :> ISqlDataContext @@> );
                    [constr;cmdTimeout;selectOperations], (fun args ->
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%defaultTransactionOptionsExpr, (Some %%args.[1]), %%args.[3]) :> ISqlDataContext @@> );
                    [constr; respath; transopt;cmdTimeout;selectOperations], (fun args -> 
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, %%args.[1], %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%args.[2], (Some %%args.[3]), %%args.[4]) :> ISqlDataContext  @@>)




                ]
              yield! 
                  crossTargetParameterCombinations |> Seq.map(fun (parmArr, invoker) ->
                      let providerParams = parmArr |> List.map(fun (pname, _, ptype) -> ProvidedParameter(pname, ptype))
                      let meth = 
                        ProvidedMethod("GetDataContext", providerParams, serviceType, isStatic = true, invokeCode = invoker)
                      let xmlComment = 
                            let all = parmArr |> List.map(fun (pname, xmlInfo, _) -> "<param name='" + pname + "'>" + xmlInfo + "</param>") |> List.toArray
                            String.Join("", all)
                      meth.AddXmlDoc ("<summary>Returns an instance of the SQL Provider using the static parameters</summary>" + xmlComment)
                      meth
                  )
            ])
        if (dbVendor <> DatabaseProviderTypes.MSACCESS) then con.Close()
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
    let odbcquote = ProvidedStaticParameter("OdbcQuote", typeof<OdbcQuoteCharacter>, OdbcQuoteCharacter.DEFAULT_QUOTE)
    let sqliteLibrary = ProvidedStaticParameter("SQLiteLibrary",typeof<SQLiteLibrary>,SQLiteLibrary.AutoSelect)
    let helpText = "<summary>Typed representation of a database</summary>
                    <param name='ConnectionString'>The connection string for the SQL database</param>
                    <param name='ConnectionStringName'>The connection string name to select from a configuration file</param>
                    <param name='DatabaseVendor'> The target database vendor</param>
                    <param name='IndividualsAmount'>The amount of sample entities to project into the type system for each SQL entity type. Default 1000.</param>
                    <param name='UseOptionTypes'>If true, F# option types will be used in place of nullable database columns.  If false, you will always receive the default value of the column's type even if it is null in the database.</param>
                    <param name='ResolutionPath'>The location to look for dynamically loaded assemblies containing database vendor specific connections and custom types.</param>
                    <param name='Owner'>The owner of the schema for this provider to resolve (Oracle Only)</param>
                    <param name='CaseSensitivityChange'>Should we do ToUpper or ToLower when generating table names?</param>
                    <param name='TableNames'>Comma separated table names list to limit a number of tables in big instances. The names can have '%' sign to handle it as in the 'LIKE' query (Oracle and MSSQL Only)</param>
                    <param name='OdbcQuote'>Odbc quote characters: Quote characters for the table and column names: `alias`, [alias]</param>
                    <param name='SQLiteLibrary'>Use System.Data.SQLite or Mono.Data.SQLite or select automatically (SQLite only)</param>
                    "
        
    do paramSqlType.DefineStaticParameters([dbVendor;conString;connStringName;resolutionPath;individualsAmount;optionTypes;owner;caseSensitivity; tableNames; odbcquote; sqliteLibrary], fun typeName args -> 
        
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
            args.[9] :?> OdbcQuoteCharacter,      // Quote characters (Odbc only)
            args.[10] :?> SQLiteLibrary,          // Use System.Data.SQLite or Mono.Data.SQLite or select automatically (SQLite only)
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


