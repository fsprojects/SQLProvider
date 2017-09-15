﻿namespace FSharp.Data.Sql

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
    let runtimeAssembly = Assembly.LoadFrom(config.RuntimeAssembly)
    member __.RuntimeAssembly = runtimeAssembly 

module internal DesignTimeCache = 
    let cache = System.Collections.Concurrent.ConcurrentDictionary<_,ProvidedTypeDefinition>()

[<TypeProvider>]
type SqlTypeProvider(config: TypeProviderConfig) as this =     
    inherit TypeProviderForNamespaces()
    let sqlRuntimeInfo = SqlRuntimeInfo(config)
    let ctxt = ProvidedTypesContext.Create(config)
    let ns = "FSharp.Data.Sql"
     
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
            let rootType = ctxt.ProvidedTypeDefinition(sqlRuntimeInfo.RuntimeAssembly,ns,rootTypeName,baseType=Some typeof<obj>, HideObjectMethods=true)
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
        let serviceType = ctxt.ProvidedTypeDefinition( "dataContext", None, HideObjectMethods = true)
        let transactionOptions = TransactionOptions.Default
        let designTimeDc = SqlDataContext(rootTypeName, conString, dbVendor, resolutionPath, config.ReferencedAssemblies, config.RuntimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, transactionOptions, None)
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
                                ctxt.ProvidedProperty("PossibleError", typeof<String>, fun _ -> <@@ possibleError @@>)
                            errInfo.AddXmlDocDelayed(fun () -> 
                                this.Invalidate()
                                "You have possible configuration error. \r\n " + possibleError)
                            serviceType.AddMember errInfo
                       else                
                       for table in tablesforced do
                        let t = ctxt.ProvidedTypeDefinition(table.FullName + "Entity", Some typeof<SqlEntity>, HideObjectMethods = true)
                        t.AddMemberDelayed(fun () -> ctxt.ProvidedConstructor([ctxt.ProvidedParameter("dataContext",typeof<ISqlDataContext>)],
                                                        fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).CreateEntity(table.FullName) @@>))
                        let desc = (sprintf "An instance of the %s %s belonging to schema %s" table.Type table.Name table.Schema)
                        t.AddXmlDoc desc
                        yield table.FullName,(t,sprintf "The %s %s belonging to schema %s" table.Type table.Name table.Schema,"", table.Schema) ]

        let createIndividualsType (table:Table) =
            let (et,_,_,_) = baseTypes.Force().[table.FullName]
            let t = ctxt.ProvidedTypeDefinition(table.Schema + "." + table.Name + "." + "Individuals", None, HideObjectMethods = true)
            let individualsTypes = ResizeArray<_>()
            individualsTypes.Add t
            
            t.AddXmlDocDelayed(fun _ -> sprintf "A sample of %s individuals from the SQL object as supplied in the static parameters" table.Name)
            t.AddMember(ctxt.ProvidedConstructor([ctxt.ProvidedParameter("dataContext", typeof<ISqlDataContext>)]))
            t.AddMembersDelayed( fun _ ->
               let columns = prov.GetColumns(con,table)
               match prov.GetPrimaryKey table with
               | Some pk ->
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
                        if col.Key = pk then None else
                        let name = table.Schema + "." + table.Name + "." + col.Key + "Individuals"
                        let ty = ctxt.ProvidedTypeDefinition(name, None, HideObjectMethods = true )
                        ty.AddMember(ProvidedConstructor([ctxt.ProvidedParameter("sqlService", typeof<ISqlDataContext>)]))
                        individualsTypes.Add ty
                        Some(col.Key,(ty,ctxt.ProvidedProperty(sprintf "As %s" (buildFieldName col.Key),ty, fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext)@@> ))))
                      |> Map.ofSeq
                 
                   // special case for guids as they are not a supported quotable constant in the TP mechanics,
                   // but we can deal with them as strings.
                   let (|FixedType|_|) (o:obj) = 
                      match o, o.GetType().IsValueType with
                      // watch out for normal strings
                      | :? string, _ -> Some o
                      | :? Guid, _ -> Some (box (o.ToString()))                      
                      | _, true -> Some o
                      // can't support any other types
                      | _, _ -> None

                   // on the main object create a property for each entity simply using the primary key 
                   let props =
                      entities
                      |> Array.choose(fun e -> 
                         match e.GetColumn pk with
                         | FixedType pkValue -> 
                            let name = table.FullName
                            // this next bit is just side effect to populate the "As Column" types for the supported columns
                            e.ColumnValues
                            |> Seq.iter(fun (k,v) -> 
                               if k = pk then () else      
                               (fst propertyMap.[k]).AddMemberDelayed(
                                  fun()->ctxt.ProvidedProperty(sprintf "%s, %s" (pkValue.ToString()) (if v = null then "<null>" else v.ToString()) ,et,
                                            fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).GetIndividual(name,pkValue) @@> )))
                            // return the primary key property
                            Some <| ctxt.ProvidedProperty(pkValue.ToString(),et, fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).GetIndividual(name,pkValue) @@> )
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
                        let ct = ctxt.ProvidedTypeDefinition(table.FullName, None ,HideObjectMethods=false)                        
                        ct.AddInterfaceImplementationsDelayed( fun () -> [ctxt.MakeGenericType(typedefof<System.Linq.IQueryable<_>>,[et :> Type]); typeof<ISqlDataContext>])
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
                            ctxt.ProvidedProperty(
                                SchemaProjections.buildFieldName(name),propTy,
                                (fun (args:Expr list) ->
                                    let meth = if nullable then typeof<SqlEntity>.GetMethod("GetColumnOption").MakeGenericMethod([|ty|])
                                               else  typeof<SqlEntity>.GetMethod("GetColumn").MakeGenericMethod([|ty|])
                                    Expr.Call(args.[0],meth,[Expr.Value name])
                                ),
                                (fun (args:Expr list) ->
                                    if nullable then 
                                        let meth = typeof<SqlEntity>.GetMethod("SetColumnOption").MakeGenericMethod([|ty|])
                                        Expr.Call(args.[0],meth,[Expr.Value name;args.[1]])
                                    else      
                                        let meth = typeof<SqlEntity>.GetMethod("SetColumn").MakeGenericMethod([|ty|])
                                        Expr.Call(args.[0],meth,[Expr.Value name;args.[1]]))
                                 )
                        prop.AddXmlDocDelayed(fun () -> 
                            let typeInfo = match c.TypeInfo with None -> "" | Some x -> x.ToString() 
                            let details = prov.GetColumnDescription(con, key, c.Name).Replace("<","&lt;").Replace(">","&gt;")
                            let separator = if (String.IsNullOrWhiteSpace typeInfo) || (String.IsNullOrWhiteSpace details) then "" else "/"
                            sprintf "<summary>%s %s %s</summary>" details separator typeInfo)
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
                        let prop = ctxt.ProvidedProperty(niceName,ty, fun args -> 
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
                        let prop = ctxt.ProvidedProperty(niceName,ty, fun args -> 
                            let pt = r.PrimaryTable
                            let pk = r.PrimaryKey
                            let ft = r.ForeignTable
                            let fk = r.ForeignKey
                            <@@ (%%args.[0] : SqlEntity).DataContext.CreateRelated((%%args.[0] : SqlEntity),constraintName,pt, pk,ft, fk,RelationshipDirection.Parents) @@> )
                        prop.AddXmlDoc(sprintf "Related %s entities from the primary side of the relationship, where the primary key is %s and the foreign key is %s. Constraint: %s" r.PrimaryTable r.PrimaryKey r.ForeignKey constraintName)
                        yield prop ]
                attProps @ relProps)
        
        let generateSprocMethod (container:ProvidedTypeDefinition) (con:IDbConnection) (sproc:CompileTimeSprocDefinition) =             
            let rt = ctxt.ProvidedTypeDefinition(SchemaProjections.buildSprocName(sproc.Name.DbName),None, HideObjectMethods = true)
            let resultType = ctxt.ProvidedTypeDefinition("Result",None, HideObjectMethods = true)
            resultType.AddMember(ctxt.ProvidedConstructor([ctxt.ProvidedParameter("sqlDataContext", typeof<ISqlDataContext>)]))
            rt.AddMember resultType
            container.AddMember(rt)
            
            resultType.AddMembersDelayed(fun () ->
                    Sql.ensureOpen con
                    let sprocParameters = sproc.Params con        
                    let parameters =
                        sprocParameters
                        |> List.filter (fun p -> p.Direction = ParameterDirection.Input || p.Direction = ParameterDirection.InputOutput)
                        |> List.map(fun p -> ctxt.ProvidedParameter(p.Name,Type.GetType p.TypeMapping.ClrType))
                    let retCols = getSprocReturnColumns sproc sprocParameters |> List.toArray
                    let runtimeSproc = {Name = sproc.Name; Params = sprocParameters} : RunTimeSprocDefinition
                    let returnType = 
                        match retCols.Length with
                        | 0 -> typeof<Unit>
                        | _ -> 
                              let rt = ctxt.ProvidedTypeDefinition("SprocResult",Some typeof<SqlEntity>, HideObjectMethods = true)
                              rt.AddMember(ctxt.ProvidedConstructor([]))
                                      
                              retCols
                              |> Array.iter(fun col ->
                                  let name = col.Name
                                  let ty = Type.GetType col.TypeMapping.ClrType
                                  let prop = 
                                      ctxt.ProvidedProperty(
                                          name,ty,
                                          (fun (args:Expr list) ->
                                              let meth = typeof<SqlEntity>.GetMethod("GetColumn").MakeGenericMethod([|ty|])
                                              Expr.Call(args.[0],meth,[Expr.Value name])),
                                          (fun (args:Expr list) ->
                                              let meth = typeof<SqlEntity>.GetMethod("SetColumn").MakeGenericMethod([|typeof<obj>|])
                                              Expr.Call(args.[0],meth,[Expr.Value name;Expr.Coerce(args.[1], typeof<obj>)])))
                                  rt.AddMember prop)
                              resultType.AddMember(rt)
                              rt :> Type
                    let retColsExpr =
                        QuotationHelpers.arrayExpr retCols |> snd
                    let asyncRet = typedefof<Async<_>>.MakeGenericType([| returnType |])
                    [ctxt.ProvidedMethod("Invoke", parameters, returnType, QuotationHelpers.quoteRecord runtimeSproc (fun args var -> 
                        <@@ (((%%args.[0] : obj):?>ISqlDataContext)).CallSproc(%%var, %%retColsExpr,  %%Expr.NewArray(typeof<obj>,List.map(fun e -> Expr.Coerce(e,typeof<obj>)) args.Tail)) @@>));
                     ctxt.ProvidedMethod("InvokeAsync", parameters, asyncRet, QuotationHelpers.quoteRecord runtimeSproc (fun args var -> 
                        <@@ (((%%args.[0] : obj):?>ISqlDataContext)).CallSprocAsync(%%var, %%retColsExpr,  %%Expr.NewArray(typeof<obj>,List.map(fun e -> Expr.Coerce(e,typeof<obj>)) args.Tail)) @@>))]
            )

            ctxt.ProvidedProperty(SchemaProjections.buildSprocName(sproc.Name.ProcName), resultType, (fun args -> <@@ ((%%args.[0] : obj) :?>ISqlDataContext) @@>) ) 
            
        
        let rec walkSproc con (path:string list) (parent:ProvidedTypeDefinition option) (createdTypes:Map<string list,ProvidedTypeDefinition>) (sproc:Sproc) =
            match sproc with
            | Root(typeName, next) -> 
                let path = (path @ [typeName])
                match createdTypes.TryFind path with
                | Some(typ) -> 
                    walkSproc con path (Some typ) createdTypes next 
                | None ->
                    let typ = ctxt.ProvidedTypeDefinition(typeName, None, HideObjectMethods = true)
                    typ.AddMember(ctxt.ProvidedConstructor([ctxt.ProvidedParameter("sqlDataContext", typeof<ISqlDataContext>)]))
                    walkSproc con path (Some typ) (createdTypes.Add(path, typ)) next 
            | Package(typeName, packageDefn) ->       
                match parent with
                | Some(parent) ->
                    let path = (path @ [typeName])
                    let typ = ctxt.ProvidedTypeDefinition(typeName, None, HideObjectMethods = true)
                    parent.AddMember(typ)
                    parent.AddMember(ctxt.ProvidedProperty(SchemaProjections.nicePascalName typeName, typ, fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext) @@>))
                    typ.AddMember(ctxt.ProvidedConstructor([ctxt.ProvidedParameter("sqlDataContext", typeof<ISqlDataContext>)]))
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
                    let pt = ctxt.ProvidedTypeDefinition(name + "Schema", Some typeof<obj>)
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
                    let columns,_ = getTableData key 
                    let (_,columns) =
                        columns |> Seq.map (fun kvp -> kvp.Value)
                                |> Seq.toList
                                |> List.partition(fun c->c.IsNullable || c.IsPrimaryKey)
                    let normalParameters = 
                        columns 
                        |> List.map(fun c -> ctxt.ProvidedParameter(c.Name,Type.GetType c.TypeMapping.ClrType))
                        |> List.sortBy(fun p -> p.Name)
                    
                    // Create: unit -> SqlEntity 
                    let create1 = ctxt.ProvidedMethod("Create", [], entityType, fun args ->                         
                        <@@ 
                            let e = ((%%args.[0] : obj ):?> IWithDataContext).DataContext.CreateEntity(key)
                            e._State <- Created
                            ((%%args.[0] : obj ):?> IWithDataContext ).DataContext.SubmitChangedEntity e
                            e 
                        @@> )
                    
                    // Create: ('a * 'b * 'c * ...) -> SqlEntity 
                    let create2 = ctxt.ProvidedMethod("Create", normalParameters, entityType, fun args -> 
                          
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
                    let create3 = ctxt.ProvidedMethod("Create", [ctxt.ProvidedParameter("data",typeof< (string*obj) seq >)] , entityType, fun args -> 
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
                        let cols = columns |> Seq.map(fun c -> c.Name)
                        "Item array of database columns: \r\n" + String.Join(",", cols)
                    create3.AddXmlDoc (sprintf "<summary>%s</summary>" desc3)


                    // ``Create(...)``: ('a * 'b * 'c * ...) -> SqlEntity 

                    let template=
                        let cols = normalParameters |> Seq.map(fun c -> c.Name )
                        "Create(" + String.Join(", ", cols) + ")"
                    let create4 = ctxt.ProvidedMethod(template, normalParameters, entityType, fun args -> 
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
                     let individuals = ctxt.ProvidedProperty("Individuals",Seq.head it, fun args -> <@@ ((%%args.[0] : obj ):?> IWithDataContext ).DataContext @@> )
                     individuals.AddXmlDoc("<summary>Get individual items from the table. Requires single primary key.</summary>")
                     yield individuals :> MemberInfo
                     if normalParameters.Length > 0 then yield create2 :> MemberInfo
                     yield create3 :> MemberInfo
                     yield create1 :> MemberInfo
                     yield create4 :> MemberInfo } |> Seq.toList
                )

                let buildTableName = SchemaProjections.buildTableName >> caseInsensitivityCheck
                let prop = ctxt.ProvidedProperty(buildTableName(ct.Name),ct, fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).CreateEntities(key) @@> )

                prop.AddXmlDocDelayed (fun () -> 
                    let details = prov.GetTableDescription(con, ct.Name).Replace("<","&lt;").Replace(">","&gt;")
                    let separator = if (String.IsNullOrWhiteSpace desc) || (String.IsNullOrWhiteSpace details) then "" else "/"
                    sprintf "<summary>%s %s %s</summary>" details separator desc)
                schemaType.AddMember ct
                schemaType.AddMember prop

                yield entityType :> MemberInfo
                //yield ct         :> MemberInfo                
                //yield prop       :> MemberInfo
                yield! Seq.cast<MemberInfo> it

              yield! containers |> Seq.map(fun p ->  ctxt.ProvidedProperty(p.Name.Replace("Container",""), p, fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext) @@>)) |> Seq.cast<MemberInfo>
              let submit = ctxt.ProvidedMethod("SubmitUpdates",[],typeof<unit>,     fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).SubmitPendingChanges() @@>)
              submit.AddXmlDoc("<summary>Save changes to data-source. May throws errors: To deal with non-saved items use GetUpdates() and ClearUpdates().</summary>") 
              yield submit :> MemberInfo
              let submitAsync = ctxt.ProvidedMethod("SubmitUpdatesAsync",[],typeof<Async<unit>>,     fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).SubmitPendingChangesAsync() @@>)
              submitAsync.AddXmlDoc("<summary>Save changes to data-source. May throws errors: Use Async.Catch and to deal with non-saved items use GetUpdates() and ClearUpdates().</summary>") 
              yield submitAsync :> MemberInfo
              yield ctxt.ProvidedMethod("GetUpdates",[],typeof<SqlEntity list>, fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).GetPendingEntities() @@>)  :> MemberInfo
              yield ctxt.ProvidedMethod("ClearUpdates",[],typeof<SqlEntity list>, fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).ClearPendingChanges() @@>)  :> MemberInfo
              yield ctxt.ProvidedMethod("CreateConnection",[],typeof<IDbConnection>, fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).CreateConnection() @@>)  :> MemberInfo
             ] @ [
                for KeyValue(name,pt) in schemaMap do
                    yield pt :> MemberInfo
                    yield ctxt.ProvidedProperty(SchemaProjections.buildTableName(name),pt, fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext) @@> ) :> MemberInfo
             ])
        
        let referencedAssemblyExpr = QuotationHelpers.arrayExpr config.ReferencedAssemblies |> snd
        let defaultTransactionOptionsExpr = <@@ TransactionOptions.Default @@>
        rootType.AddMembers [ serviceType ]
        rootType.AddMembersDelayed (fun () -> 
            [ 
              let constr =     "connectionString","The database runtime connection string",typeof<string>
              let respath =    "resolutionPath", "The location to look for dynamically loaded assemblies containing database vendor specific connections and custom types",typeof<string>
              let transopt =   "transactionOptions", "TransactionOptions for the transaction created on SubmitChanges.", typeof<TransactionOptions>
              let cmdTimeout = "commandTimeout", "SQL command timeout. Maximum time for single SQL-command in seconds.", typeof<int>

              // This could be refactored to some kind of Option type thing to get rid of different invoke-args-functions?
              let parameterCombinations = [
                    [], (fun (_:Expr list) ->
                                let runtimePath = config.ResolutionFolder
                                let runtimeAssembly = config.ResolutionFolder
                                let runtimeConStr = 
                                    <@@ match ConfigHelpers.tryGetConnectionString true runtimePath conStringName connnectionString with
                                        | "" -> failwithf "No connection string specified or could not find a connection string with name %s" conStringName
                                        | cs -> cs @@>
                                <@@ SqlDataContext(rootTypeName, %%runtimeConStr, dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%defaultTransactionOptionsExpr, None) :> ISqlDataContext @@>);
                    [constr], (fun args ->
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%defaultTransactionOptionsExpr, None) :> ISqlDataContext @@> );
                    [constr;respath], (fun args -> 
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, %%args.[1], %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%defaultTransactionOptionsExpr, None) :> ISqlDataContext  @@>);
                    [transopt], (fun args ->
                                let runtimePath = config.ResolutionFolder
                                let runtimeAssembly = config.ResolutionFolder
                                let runtimeConStr = 
                                    <@@ match ConfigHelpers.tryGetConnectionString true runtimePath conStringName connnectionString with
                                        | "" -> failwithf "No connection string specified or could not find a connection string with name %s" conStringName
                                        | cs -> cs @@>
                                <@@ SqlDataContext(rootTypeName, %%runtimeConStr, dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%args.[0], None) :> ISqlDataContext @@>);
                    [constr; transopt], (fun args ->
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%args.[1], None) :> ISqlDataContext @@> );
                    [constr; respath; transopt], (fun args -> 
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, %%args.[1], %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%args.[2], None) :> ISqlDataContext  @@>)
                    [cmdTimeout], (fun args ->
                                let runtimePath = config.ResolutionFolder
                                let runtimeAssembly = config.ResolutionFolder
                                let runtimeConStr = 
                                    <@@ match ConfigHelpers.tryGetConnectionString true runtimePath conStringName connnectionString with
                                        | "" -> failwithf "No connection string specified or could not find a connection string with name %s" conStringName
                                        | cs -> cs @@>
                                <@@ SqlDataContext(rootTypeName, %%runtimeConStr, dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%defaultTransactionOptionsExpr, (Some %%args.[0])) :> ISqlDataContext @@>);
                    [constr;cmdTimeout], (fun args ->
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%defaultTransactionOptionsExpr, (Some %%args.[1])) :> ISqlDataContext @@> );
                    [constr;respath;cmdTimeout], (fun args -> 
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, %%args.[1], %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%defaultTransactionOptionsExpr, (Some %%args.[2])) :> ISqlDataContext  @@>);
                    [transopt;cmdTimeout], (fun args ->
                                let runtimePath = config.ResolutionFolder
                                let runtimeAssembly = config.ResolutionFolder
                                let runtimeConStr = 
                                    <@@ match ConfigHelpers.tryGetConnectionString true runtimePath conStringName connnectionString with
                                        | "" -> failwithf "No connection string specified or could not find a connection string with name %s" conStringName
                                        | cs -> cs @@>
                                <@@ SqlDataContext(rootTypeName, %%runtimeConStr, dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%args.[0], (Some %%args.[1])) :> ISqlDataContext @@>);
                    [constr; transopt;cmdTimeout], (fun args ->
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%args.[1], (Some %%args.[2])) :> ISqlDataContext @@> );
                    [constr; respath; transopt;cmdTimeout], (fun args -> 
                                let runtimeAssembly = config.ResolutionFolder
                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, %%args.[1], %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames, odbcquote, sqliteLibrary, %%args.[2], (Some %%args.[3])) :> ISqlDataContext  @@>)
                ]

              yield! 
                  parameterCombinations |> Seq.map(fun (parmArr, invoker) ->
                      let providerParams = parmArr |> List.map(fun (pname, _, ptype) -> ctxt.ProvidedParameter(pname, ptype))
                      let meth = 
                        ProvidedMethod ("GetDataContext", providerParams,
                                        serviceType, IsStaticMethod=true,
                                        InvokeCode = invoker)
                      let xmlComment = 
                            let all = parmArr |> List.map(fun (pname, xmlInfo, _) -> "<param name='" + pname + "'>" + xmlInfo + "</param>") |> List.toArray
                            String.Join("", all)
                      meth.AddXmlDoc ("<summary>Returns an instance of the SQL Provider using the static parameters</summary>" + xmlComment)
                      meth
                  )

            ])
        if (dbVendor <> DatabaseProviderTypes.MSACCESS) then con.Close()
        rootType
    
    let paramSqlType = ctxt.ProvidedTypeDefinition(sqlRuntimeInfo.RuntimeAssembly, ns, "SqlDataProvider", Some(typeof<obj>), HideObjectMethods = true)
    
    let conString = ctxt.ProvidedStaticParameter("ConnectionString",typeof<string>, "")
    let connStringName = ctxt.ProvidedStaticParameter("ConnectionStringName", typeof<string>, "")    
    let optionTypes = ctxt.ProvidedStaticParameter("UseOptionTypes",typeof<bool>,false)
    let dbVendor = ctxt.ProvidedStaticParameter("DatabaseVendor",typeof<DatabaseProviderTypes>,DatabaseProviderTypes.MSSQLSERVER)
    let individualsAmount = ctxt.ProvidedStaticParameter("IndividualsAmount",typeof<int>,1000)
    let owner = ctxt.ProvidedStaticParameter("Owner", typeof<string>, "")    
    let resolutionPath = ctxt.ProvidedStaticParameter("ResolutionPath",typeof<string>, "")    
    let caseSensitivity = ctxt.ProvidedStaticParameter("CaseSensitivityChange",typeof<CaseSensitivityChange>,CaseSensitivityChange.ORIGINAL)
    let tableNames = ctxt.ProvidedStaticParameter("TableNames", typeof<string>, "")
    let odbcquote = ctxt.ProvidedStaticParameter("OdbcQuote", typeof<OdbcQuoteCharacter>, OdbcQuoteCharacter.DEFAULT_QUOTE)
    let sqliteLibrary = ctxt.ProvidedStaticParameter("SQLiteLibrary",typeof<SQLiteLibrary>,SQLiteLibrary.AutoSelect)
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
    do this.AddNamespace(ns, [paramSqlType])
                            
[<assembly:TypeProviderAssembly>] 
do()


