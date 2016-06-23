namespace FSharp.Data.Sql

open FSharp.Data.Sql.Runtime
open FSharp.Data.Sql.Common

open System
open System.Data
open System.Reflection
open Microsoft.FSharp.Core.CompilerServices
open Microsoft.FSharp.Quotations
open Samples.FSharp.ProvidedTypes
open FSharp.Data.Sql.Schema

type internal SqlRuntimeInfo (config : TypeProviderConfig) =
    let runtimeAssembly = Assembly.LoadFrom(config.RuntimeAssembly)
    member __.RuntimeAssembly = runtimeAssembly 

[<TypeProvider>]
type SqlTypeProvider(config: TypeProviderConfig) as this =     
    inherit TypeProviderForNamespaces()
    let sqlRuntimeInfo = SqlRuntimeInfo(config)
    let ns = "FSharp.Data.Sql"
     
    let createTypes(connnectionString, conStringName,dbVendor,resolutionPath,individualsAmount,useOptionTypes,owner,caseSensitivity, tableNames, rootTypeName) = 
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
            
        let rootType = ProvidedTypeDefinition(sqlRuntimeInfo.RuntimeAssembly,ns,rootTypeName,baseType=Some typeof<obj>, HideObjectMethods=true)

        let prov = ProviderBuilder.createProvider dbVendor resolutionPath config.ReferencedAssemblies config.RuntimeAssembly owner tableNames
        let con = prov.CreateConnection conString
        con.Open()
        prov.CreateTypeMappings con
        
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
        let serviceType = ProvidedTypeDefinition( "dataContext", None, HideObjectMethods = true)
        let designTimeDc = SqlDataContext(rootTypeName,conString,dbVendor,resolutionPath,config.ReferencedAssemblies, config.RuntimeAssembly, owner, caseSensitivity, tableNames)
        // first create all the types so we are able to recursively reference them in each other's definitions
        let baseTypes =
            lazy
                dict [ let tablesforced = tables.Force()
                       if tablesforced.Length = 0 then
                            let hint =
                                match caseSensitivity with
                                | CaseSensitivityChange.ORIGINAL | CaseSensitivityChange.TOLOWER
                                        when prov.GetTables(con,CaseSensitivityChange.TOUPPER).Length > 0 ->
                                    ". Try adding parameter SqlDataProvider<CaseSensitivityChange=Common.CaseSensitivityChange.TOUPPER, ...>"
                                | CaseSensitivityChange.ORIGINAL | CaseSensitivityChange.TOUPPER 
                                        when prov.GetTables(con,CaseSensitivityChange.TOLOWER).Length > 0 ->
                                    ". Try adding parameter SqlDataProvider<CaseSensitivityChange=Common.CaseSensitivityChange.TOLOWER, ...>"
                                | _ when owner = "" -> ". Try adding parameter SqlDataProvider<Owner=...> where Owner value is database name or schema."
                                | _ -> " for schema or database " + owner + ". Connection: " + connnectionString
                            let possibleError = "Tables not found" + hint
                            let t = ProvidedProperty("PossibleError", typeof<String>, IsStatic=true, GetterCode = fun _ -> <@@ possibleError @@>)
                            t.AddXmlDoc possibleError
                            rootType.AddMember t
                       else                
                       for table in tablesforced do
                        let t = ProvidedTypeDefinition(table.FullName + "Entity", Some typeof<SqlEntity>, HideObjectMethods = true)
                        t.AddMemberDelayed(fun () -> ProvidedConstructor([ProvidedParameter("dataContext",typeof<ISqlDataContext>)],
                                                        InvokeCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).CreateEntity(table.FullName) @@>))
                        let desc = (sprintf "An instance of the %s %s belonging to schema %s" table.Type table.Name table.Schema)
                        t.AddXmlDoc desc
                        yield table.FullName,(t,sprintf "The %s %s belonging to schema %s" table.Type table.Name table.Schema,"", table.Schema) ]

        let createIndividualsType (table:Table) =
            let (et,_,_,_) = baseTypes.Force().[table.FullName]
            let t = ProvidedTypeDefinition(table.Schema + "." + table.Name + "." + "Individuals", None, HideObjectMethods = true)
            let individualsTypes = ResizeArray<_>()
            individualsTypes.Add t
            
            t.AddXmlDocDelayed(fun _ -> sprintf "A sample of %s individuals from the SQL object as supplied in the static parameters" table.Name)
            t.AddMember(ProvidedConstructor([ProvidedParameter("dataContext", typeof<ISqlDataContext>)]))
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
                        let ty = ProvidedTypeDefinition(name, None, HideObjectMethods = true )
                        ty.AddMember(ProvidedConstructor([ProvidedParameter("sqlService", typeof<ISqlDataContext>)]))
                        individualsTypes.Add ty
                        Some(col.Key,(ty,ProvidedProperty(sprintf "As %s" (buildFieldName col.Key),ty, GetterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext)@@> ))))
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
                                  fun()->ProvidedProperty(sprintf "%s, %s" (pkValue.ToString()) (if v = null then "<null>" else v.ToString()) ,et,
                                            GetterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).GetIndividual(name,pkValue) @@> )))
                            // return the primary key property
                            Some <| ProvidedProperty(pkValue.ToString(),et,GetterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).GetIndividual(name,pkValue) @@> )
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
                        let ct = ProvidedTypeDefinition(table.FullName, None ,HideObjectMethods=false)                        
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
                                GetterCode = (fun args ->
                                    let meth = if nullable then typeof<SqlEntity>.GetMethod("GetColumnOption").MakeGenericMethod([|ty|])
                                               else  typeof<SqlEntity>.GetMethod("GetColumn").MakeGenericMethod([|ty|])
                                    Expr.Call(args.[0],meth,[Expr.Value name])
                                ),
                                SetterCode = (fun args ->
                                    if nullable then 
                                        let meth = typeof<SqlEntity>.GetMethod("SetColumnOption").MakeGenericMethod([|ty|])
                                        Expr.Call(args.[0],meth,[Expr.Value name;args.[1]])
                                    else      
                                        let meth = typeof<SqlEntity>.GetMethod("SetColumn").MakeGenericMethod([|ty|])
                                        Expr.Call(args.[0],meth,[Expr.Value name;args.[1]]))
                                 )
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
                        let prop = ProvidedProperty(niceName,ty,GetterCode = fun args -> 
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
                        let prop = ProvidedProperty(niceName,ty,GetterCode = fun args -> 
                            let pt = r.PrimaryTable
                            let pk = r.PrimaryKey
                            let ft = r.ForeignTable
                            let fk = r.ForeignKey
                            <@@ (%%args.[0] : SqlEntity).DataContext.CreateRelated((%%args.[0] : SqlEntity),constraintName,pt,pk,ft,fk,RelationshipDirection.Parents) @@> )
                        prop.AddXmlDoc(sprintf "Related %s entities from the primary side of the relationship, where the primary key is %s and the foreign key is %s. Constraint: %s" r.PrimaryTable r.PrimaryKey r.ForeignKey constraintName)
                        yield prop ]
                attProps @ relProps)
        
        let generateSprocMethod (container:ProvidedTypeDefinition) (con:IDbConnection) (sproc:CompileTimeSprocDefinition) =             
            let rt = ProvidedTypeDefinition(SchemaProjections.buildSprocName(sproc.Name.DbName),None, HideObjectMethods = true)
            let resultType = ProvidedTypeDefinition("Result",None, HideObjectMethods = true)
            resultType.AddMember(ProvidedConstructor([ProvidedParameter("sqlDataContext", typeof<ISqlDataContext>)]))
            rt.AddMember resultType
            container.AddMember(rt)
            
            resultType.AddMemberDelayed(fun () ->
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
                              let rt = ProvidedTypeDefinition("SprocResult",Some typeof<SqlEntity>, HideObjectMethods = true)
                              rt.AddMember(ProvidedConstructor([]))
                                      
                              retCols
                              |> Array.iter(fun col ->
                                  let name = col.Name
                                  let ty = Type.GetType col.TypeMapping.ClrType
                                  let prop = 
                                      ProvidedProperty(
                                          name,ty,
                                          GetterCode = (fun args ->
                                              let meth = typeof<SqlEntity>.GetMethod("GetColumn").MakeGenericMethod([|ty|])
                                              Expr.Call(args.[0],meth,[Expr.Value name])),
                                          SetterCode = (fun args ->
                                              let meth = typeof<SqlEntity>.GetMethod("SetColumn").MakeGenericMethod([|typeof<obj>|])
                                              Expr.Call(args.[0],meth,[Expr.Value name;Expr.Coerce(args.[1], typeof<obj>)])))
                                  rt.AddMember prop)
                              resultType.AddMember(rt)
                              rt :> Type
                    let retColsExpr =
                        QuotationHelpers.arrayExpr retCols |> snd
                    ProvidedMethod("Invoke", parameters, returnType, InvokeCode = QuotationHelpers.quoteRecord runtimeSproc (fun args var ->                      
                        <@@ (((%%args.[0] : obj):?>ISqlDataContext)).CallSproc(%%var, %%retColsExpr,  %%Expr.NewArray(typeof<obj>,List.map(fun e -> Expr.Coerce(e,typeof<obj>)) args.Tail)) @@>))
            )

            ProvidedProperty(SchemaProjections.buildSprocName(sproc.Name.ProcName), resultType, GetterCode = (fun args -> <@@ ((%%args.[0] : obj) :?>ISqlDataContext) @@>) ) 
            
        
        let rec walkSproc con (path:string list) (parent:ProvidedTypeDefinition option) (createdTypes:Map<string list,ProvidedTypeDefinition>) (sproc:Sproc) =
            match sproc with
            | Root(typeName, next) -> 
                let path = (path @ [typeName])
                match createdTypes.TryFind path with
                | Some(typ) -> 
                    walkSproc con path (Some typ) createdTypes next 
                | None ->
                    let typ = ProvidedTypeDefinition(typeName, None, HideObjectMethods = true)
                    typ.AddMember(ProvidedConstructor([ProvidedParameter("sqlDataContext", typeof<ISqlDataContext>)]))
                    walkSproc con path (Some typ) (createdTypes.Add(path, typ)) next 
            | Package(typeName, packageDefn) ->       
                match parent with
                | Some(parent) ->
                    let path = (path @ [typeName])
                    let typ = ProvidedTypeDefinition(typeName, None, HideObjectMethods = true)
                    parent.AddMember(typ)
                    parent.AddMember(ProvidedProperty(SchemaProjections.nicePascalName typeName, typ, GetterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext) @@>))
                    typ.AddMember(ProvidedConstructor([ProvidedParameter("sqlDataContext", typeof<ISqlDataContext>)]))
                    typ.AddMembersDelayed(fun () -> Sql.ensureOpen con; (packageDefn.Sprocs con) |> List.map (generateSprocMethod typ con)) 
                    createdTypes.Add(path, typ)
                    createdTypes 
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
                    let pt = new ProvidedTypeDefinition(name + "Schema", Some typeof<obj>)
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
                        |> List.map(fun c -> ProvidedParameter(c.Name,Type.GetType c.TypeMapping.ClrType))
                        |> List.sortBy(fun p -> p.Name)
                    
                    // Create: unit -> SqlEntity 
                    let create1 = ProvidedMethod("Create", [], entityType, InvokeCode = fun args ->                         
                        <@@ 
                            let e = ((%%args.[0] : obj ):?> IWithDataContext).DataContext.CreateEntity(key)
                            e._State <- Created
                            ((%%args.[0] : obj ):?> IWithDataContext ).DataContext.SubmitChangedEntity e
                            e 
                        @@> )
                    
                    // Create: ('a * 'b * 'c * ...) -> SqlEntity 
                    let create2 = ProvidedMethod("Create", normalParameters, entityType, InvokeCode = fun args -> 
                          
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
                    let create3 = ProvidedMethod("Create", [ProvidedParameter("data",typeof< (string*obj) seq >)] , entityType, InvokeCode = fun args -> 
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
                    let create4 = ProvidedMethod(template, normalParameters, entityType, InvokeCode = fun args -> 
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
                     yield ProvidedProperty("Individuals",Seq.head it, GetterCode = fun args -> <@@ ((%%args.[0] : obj ):?> IWithDataContext ).DataContext @@> ) :> MemberInfo
                     if normalParameters.Length > 0 then yield create2 :> MemberInfo
                     yield create3 :> MemberInfo
                     yield create1 :> MemberInfo
                     yield create4 :> MemberInfo } |> Seq.toList
                )

                let buildTableName = SchemaProjections.buildTableName >> caseInsensitivityCheck
                let prop = ProvidedProperty(buildTableName(ct.Name),ct, GetterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).CreateEntities(key) @@> )
                prop.AddXmlDoc (sprintf "<summary>%s</summary>" desc)
                schemaType.AddMember ct
                schemaType.AddMember prop



                yield entityType :> MemberInfo
                //yield ct         :> MemberInfo                
                //yield prop       :> MemberInfo
                yield! Seq.cast<MemberInfo> it

              yield! containers |> Seq.map(fun p ->  ProvidedProperty(p.Name.Replace("Container",""), p, GetterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext) @@>)) |> Seq.cast<MemberInfo>
              yield ProvidedMethod("SubmitUpdates",[],typeof<unit>,     InvokeCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).SubmitPendingChanges() @@>)  :> MemberInfo
              yield ProvidedMethod("SubmitUpdatesAsync",[],typeof<Async<unit>>,     InvokeCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).SubmitPendingChangesAsync() @@>)  :> MemberInfo
              yield ProvidedMethod("GetUpdates",[],typeof<SqlEntity list>, InvokeCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).GetPendingEntities() @@>)  :> MemberInfo
              yield ProvidedMethod("ClearUpdates",[],typeof<SqlEntity list>,InvokeCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).ClearPendingChanges() @@>)  :> MemberInfo
              yield ProvidedMethod("CreateConnection",[],typeof<IDbConnection>,InvokeCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).CreateConnection() @@>)  :> MemberInfo
             ] @ [
                for KeyValue(name,pt) in schemaMap do
                    yield pt :> MemberInfo
                    yield ProvidedProperty(SchemaProjections.buildTableName(name),pt, GetterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext) @@> ) :> MemberInfo
             ])
        
        let referencedAssemblyExpr = QuotationHelpers.arrayExpr config.ReferencedAssemblies |> snd
        rootType.AddMembers [ serviceType ]
        rootType.AddMembersDelayed (fun () -> 
            [ let meth = 
                ProvidedMethod ("GetDataContext", [],
                                serviceType, IsStaticMethod=true,
                                InvokeCode = (fun _ ->
                                    let runtimePath = config.ResolutionFolder
                                    let runtimeAssembly = config.ResolutionFolder
                                    let runtimeConStr = 
                                        <@@ match ConfigHelpers.tryGetConnectionString true runtimePath conStringName connnectionString with
                                            | "" -> failwithf "No connection string specified or could not find a connection string with name %s" conStringName
                                            | cs -> cs @@>
                                    <@@ SqlDataContext(rootTypeName,%%runtimeConStr,dbVendor,resolutionPath,%%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames) :> ISqlDataContext @@>))

              meth.AddXmlDoc "<summary>Returns an instance of the SQL Provider using the static parameters</summary>"
                   
              yield meth

              let meth = ProvidedMethod ("GetDataContext", [ProvidedParameter("connectionString",typeof<string>);], 
                                                            serviceType, IsStaticMethod=true,
                                                            InvokeCode = (fun args ->
                                                                let runtimeAssembly = config.ResolutionFolder
                                                                <@@ SqlDataContext(rootTypeName, %%args.[0], dbVendor, resolutionPath, %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames) :> ISqlDataContext @@> ))
                      
              meth.AddXmlDoc "<summary>Returns an instance of the SQL Provider</summary>
                              <param name='connectionString'>The database connection string</param>"
              yield meth


              let meth = ProvidedMethod ("GetDataContext", [ProvidedParameter("connectionString",typeof<string>);ProvidedParameter("resolutionPath",typeof<string>);],
                                                            serviceType, IsStaticMethod=true,
                                                            InvokeCode = (fun args -> 
                                                                let runtimeAssembly = config.ResolutionFolder
                                                                <@@ SqlDataContext(rootTypeName,%%args.[0],dbVendor,%%args.[1], %%referencedAssemblyExpr, runtimeAssembly, owner, caseSensitivity, tableNames) :> ISqlDataContext  @@>))

              meth.AddXmlDoc "<summary>Returns an instance of the SQL Provider</summary>
                              <param name='connectionString'>The database connection string</param>
                              <param name='resolutionPath'>The location to look for dynamically loaded assemblies containing database vendor specific connections and custom types</param>"
              yield meth
            ])
        if (dbVendor <> DatabaseProviderTypes.MSACCESS) then con.Close()
        rootType
    
    let paramSqlType = ProvidedTypeDefinition(sqlRuntimeInfo.RuntimeAssembly, ns, "SqlDataProvider", Some(typeof<obj>), HideObjectMethods = true)
    
    let conString = ProvidedStaticParameter("ConnectionString",typeof<string>, "")
    let connStringName = ProvidedStaticParameter("ConnectionStringName", typeof<string>, "")    
    let optionTypes = ProvidedStaticParameter("UseOptionTypes",typeof<bool>,false)
    let dbVendor = ProvidedStaticParameter("DatabaseVendor",typeof<DatabaseProviderTypes>,DatabaseProviderTypes.MSSQLSERVER)
    let individualsAmount = ProvidedStaticParameter("IndividualsAmount",typeof<int>,1000)
    let owner = ProvidedStaticParameter("Owner", typeof<string>, "")    
    let resolutionPath = ProvidedStaticParameter("ResolutionPath",typeof<string>, "")    
    let caseSensitivity = ProvidedStaticParameter("CaseSensitivityChange",typeof<CaseSensitivityChange>,CaseSensitivityChange.ORIGINAL)
    let tableNames = ProvidedStaticParameter("TableNames", typeof<string>, "")
    let helpText = "<summary>Typed representation of a database</summary>
                    <param name='ConnectionString'>The connection string for the SQL database</param>
                    <param name='ConnectionStringName'>The connection string name to select from a configuration file</param>
                    <param name='DatabaseVendor'> The target database vendor</param>
                    <param name='IndividualsAmount'>The amount of sample entities to project into the type system for each SQL entity type. Default 1000.</param>
                    <param name='UseOptionTypes'>If true, F# option types will be used in place of nullable database columns.  If false, you will always receive the default value of the column's type even if it is null in the database.</param>
                    <param name='ResolutionPath'>The location to look for dynamically loaded assemblies containing database vendor specific connections and custom types.</param>
                    <param name='Owner'>The owner of the schema for this provider to resolve (Oracle Only)</param>
                    <param name='CaseSensitivityChange'>Should we do ToUpper or ToLower when generating table names?</param>
                    <param name='TableNames'>Comma separated table names list to limit a number of tables in big instances. The names can have '%' sign to handle it as in the 'LIKE' query (Oracle Only)</param>
                    "
        
    do paramSqlType.DefineStaticParameters([dbVendor;conString;connStringName;resolutionPath;individualsAmount;optionTypes;owner;caseSensitivity; tableNames], fun typeName args -> 
        createTypes(args.[1] :?> string,                  // ConnectionString URL
                    args.[2] :?> string,                  // ConnectionString Name
                    args.[0] :?> DatabaseProviderTypes,   // db vendor
                    args.[3] :?> string,                  // Assembly resolution path for db connectors and custom types
                    args.[4] :?> int,                     // Individuals Amount
                    args.[5] :?> bool,                    // Use option types?
                    args.[6] :?> string,                  // Schema owner currently only used for oracle
                    args.[7] :?> CaseSensitivityChange,   // Should we do ToUpper or ToLower when generating table names?
                    args.[8] :?> string,                  // Table names list (Oracle Only)
                    typeName))

    do paramSqlType.AddXmlDoc helpText               
    
    // add them to the namespace    
    do this.AddNamespace(ns, [paramSqlType])
                            
[<assembly:TypeProviderAssembly>] 
do()


