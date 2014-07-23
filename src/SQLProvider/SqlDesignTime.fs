namespace FSharp.Data.Sql

open FSharp.Data.Sql.Runtime
open FSharp.Data.Sql.Common

open System
open System.Data
open System.Data.SqlClient
open System.IO
open System.Net
open System.Reflection
open System.Collections.Generic
open System.ServiceModel.Description
open Microsoft.FSharp.Core.CompilerServices
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns

open Samples.FSharp.ProvidedTypes
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.SchemaProjections

type internal SqlRuntimeInfo (config : TypeProviderConfig) =
    let runtimeAssembly = Assembly.LoadFrom(config.RuntimeAssembly)    
    member this.RuntimeAssembly = runtimeAssembly   

[<AbstractClass>]
type SqlTypeProvider(config: TypeProviderConfig) as this =     
    inherit TypeProviderForNamespaces()
    let sqlRuntimeInfo = SqlRuntimeInfo(config)
    let ns = "FSharp.Data.Sql";   
    let asm = Assembly.GetExecutingAssembly()
    
    let createTypes(conString,dbVendor,resolutionPath,individualsAmount,useOptionTypes,owner, rootTypeName) =       
        let prov = this.CreateSqlProvider ()
        let con = prov.CreateConnection conString
        con.Open()
        prov.CreateTypeMappings con
        
        let tables = lazy prov.GetTables con
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
        let getTableData name = tableColumns.Force().[name].Force()
        let serviceType = ProvidedTypeDefinition( "dataContext", None, HideObjectMethods = true)
        let designTimeDc = SqlDataContext(rootTypeName,conString,this.CreateSqlProvider(),owner)
        // first create all the types so we are able to recursively reference them in each other's definitions
        let baseTypes =
            lazy
                dict [ for table in tables.Force() do
                        let t = ProvidedTypeDefinition(table.FullName + "Entity", Some typeof<SqlEntity>, HideObjectMethods = true)
                        t.AddMemberDelayed(fun () -> ProvidedConstructor([ProvidedParameter("dataContext",typeof<ISqlDataContext>)],
                                                        InvokeCode = fun args -> <@@ new SqlEntity(((%%args.[0] : obj) :?> ISqlDataContext) ,table.FullName) @@>))
                        let desc = (sprintf "An instance of the %s %s belonging to schema %s" table.Type table.Name table.Schema)
                        t.AddXmlDoc desc
                        yield table.FullName,(t,sprintf "The %s %s belonging to schema %s" table.Type table.Name table.Schema,"") ]

        let createIndividualsType (table:Table) =
            let (et,_,_) = baseTypes.Force().[table.FullName]
            let t = ProvidedTypeDefinition(table.Schema + "." + table.Name + "." + "Individuals", None, HideObjectMethods = true)
            let individualsTypes = ResizeArray<_>()
            individualsTypes.Add t
            
            t.AddXmlDocDelayed(fun _ -> sprintf "A sample of %s individuals from the SQL object as supplied in the static parameters" table.Name)
            t.AddMember(ProvidedConstructor([ProvidedParameter("dataContext", typeof<ISqlDataContext>)]))
            t.AddMembersDelayed( fun _ ->
               prov.GetColumns(con,table) |> ignore 
               match prov.GetPrimaryKey table with
               | Some pk ->
                   let entities = 
                        use com = prov.CreateCommand(con,prov.GetIndividualsQueryText(table,individualsAmount))
                        if con.State <> ConnectionState.Open then con.Open()
                        use reader = com.ExecuteReader()
                        let ret = SqlEntity.FromDataReader(designTimeDc,table.FullName,reader)
                        if (dbVendor <> DatabaseProviderTypes.MSACCESS) then con.Close()
                        ret
                   if entities.IsEmpty then [] else
                   let e = entities.Head
                   // for each column in the entity except the primary key, create a new type that will read ``As Column 1`` etc
                   // inside that type the individuals will be listed again but with the text for the relevant column as the name 
                   // of the property and the primary key e.g. ``1, Dennis The Squirrel``
                   let propertyMap =
                      prov.GetColumns(con,table)
                      |> Seq.choose(fun col -> 
                        if col.Name = pk then None else
                        let name = table.Schema + "." + table.Name + "." + col.Name + "Individuals"
                        let ty = ProvidedTypeDefinition(name, None, HideObjectMethods = true )
                        ty.AddMember(ProvidedConstructor([ProvidedParameter("sqlService", typeof<ISqlDataContext>)]))
                        individualsTypes.Add ty
                        Some(col.Name,(ty,ProvidedProperty(sprintf "As %s" col.Name,ty, GetterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext)@@> ))))
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
                      |> List.choose(fun e -> 
                         match e.GetColumn pk with
                         | FixedType pkValue -> 
                            let name = table.FullName
                            // this next bit is just side effect to populate the "As Column" types for the supported columns
                            e.ColumnValues 
                            |> Seq.iter(fun (k,v) -> 
                               if k = pk then () else      
                               (fst propertyMap.[k]).AddMemberDelayed(
                                  fun()->ProvidedProperty(sprintf "%s, %s" (pkValue.ToString()) (v.ToString()) ,et,
                                            GetterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).GetIndividual(name,pkValue) @@> )))
                            // return the primary key property
                            Some <| ProvidedProperty(pkValue.ToString(),et,GetterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).GetIndividual(name,pkValue) @@> )
                         | _ -> None)
                      |> List.append( propertyMap |> Map.toList |> List.map (snd >> snd))

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
                        let (et,_,_) = baseTypes.Force().[name]
                        let ct = ProvidedTypeDefinition(table.FullName + "Set", None ,HideObjectMethods=false)                        
                        ct.AddInterfaceImplementationsDelayed( fun () -> [ProvidedTypeBuilder.MakeGenericType(typedefof<System.Linq.IQueryable<_>>,[et :> Type]); typeof<ISqlDataContext>])
                        let it = createIndividualsType table 
                        yield table.FullName,(ct,it) ]
        
        // add the attributes and relationships
        for KeyValue(key,(t,desc,_)) in baseTypes.Force() do 
            t.AddMembersDelayed(fun () -> 
                let (columns,(children,parents)) = getTableData key
                let attProps = 
                    let createColumnProperty c =
                        let nullable = useOptionTypes && c.IsNullable                        
                        let ty = if nullable then typedefof<option<_>>.MakeGenericType(c.ClrType)
                                 else c.ClrType 
                        let name = c.Name
                        let prop = 
                            ProvidedProperty(
                                buildFieldName(name),ty,
                                GetterCode = (fun args ->
                                    let meth = if nullable then typeof<SqlEntity>.GetMethod("GetColumnOption").MakeGenericMethod([|c.ClrType|])
                                               else  typeof<SqlEntity>.GetMethod("GetColumn").MakeGenericMethod([|ty|])
                                    Expr.Call(args.[0],meth,[Expr.Value name])),
                                SetterCode = (fun args ->
                                    if nullable then 
                                        // setter code is not going to work yet.
                                        let meth = typeof<SqlEntity>.GetMethod("SetColumnOption").MakeGenericMethod([|c.ClrType|])
                                        Expr.Call(args.[0],meth,[Expr.Value name;args.[1]])
                                    else      
                                        let meth = typeof<SqlEntity>.GetMethod("SetColumn").MakeGenericMethod([|c.ClrType|])
                                        Expr.Call(args.[0],meth,[Expr.Value name;args.[1]])))
                        prop
                    List.map createColumnProperty columns
                let relProps =
                    [ for r in children do                       
                        let (tt,_,_) = (baseTypes.Force().[r.ForeignTable])
                        let ty = typedefof<System.Linq.IQueryable<_>>
                        let ty = ty.MakeGenericType tt
                        let name = r.Name
                        let prop = ProvidedProperty(name,ty,GetterCode = fun args -> 
                            let pt = r.PrimaryTable
                            let pk = r.PrimaryKey
                            let ft = r.ForeignTable
                            let fk = r.ForeignKey
                            <@@ (%%args.[0] : SqlEntity).DataContext.CreateRelated((%%args.[0] : SqlEntity),name,pt,pk,ft,fk,RelationshipDirection.Children) @@> )
                        prop.AddXmlDoc(sprintf "Related %s entities from the foreign side of the relationship, where the primary key is %s and the foreign key is %s" r.ForeignTable r.PrimaryKey r.ForeignKey)
                        yield prop ] @
                    [ for r in parents do
                        let (tt,_,_) = (baseTypes.Force().[r.PrimaryTable])
                        let ty = typedefof<System.Linq.IQueryable<_>>
                        let ty = ty.MakeGenericType tt
                        let name = r.Name
                        let prop = ProvidedProperty(name,ty,GetterCode = fun args -> 
                            let pt = r.PrimaryTable
                            let pk = r.PrimaryKey
                            let ft = r.ForeignTable
                            let fk = r.ForeignKey
                            <@@ (%%args.[0] : SqlEntity).DataContext.CreateRelated((%%args.[0] : SqlEntity),name,pt,pk,ft,fk,RelationshipDirection.Parents) @@> )
                        prop.AddXmlDoc(sprintf "Related %s entities from the primary side of the relationship, where the primary key is %s and the foreign key is %s" r.PrimaryTable r.PrimaryKey r.ForeignKey)
                        yield prop ]
                attProps @ relProps)

        // add sprocs 
        let sprocContainer = ProvidedTypeDefinition("SprocContainer",None)
        sprocContainer.AddMember(ProvidedConstructor([ProvidedParameter("sqlDataContext", typeof<ISqlDataContext>)]))
        let sprocs =
            sprocData.Force()
            |> List.map(fun sproc -> 
                let containerType = ProvidedTypeDefinition(sproc.FullName,None)
                containerType.AddMember(ProvidedConstructor([ProvidedParameter("dataContext", typeof<ISqlDataContext>)]))
                containerType.AddMemberDelayed(fun () -> 
                    let rt = ProvidedTypeDefinition(sproc.FullName,Some typeof<SqlEntity>)
                    rt.AddMember(ProvidedConstructor([]))
                    serviceType.AddMember rt
                    sproc.ReturnColumns.Force()
                    |> List.iter(fun col ->
                        let name = col.Name
                        let ty = col.ClrType
                        let prop = 
                            ProvidedProperty(
                                name,ty,
                                GetterCode = (fun args ->
                                    let meth = typeof<SqlEntity>.GetMethod("GetColumn").MakeGenericMethod([|ty|])
                                    Expr.Call(args.[0],meth,[Expr.Value name])),
                                SetterCode = (fun args ->
                                    let meth = typeof<SqlEntity>.GetMethod "SetColumn"
                                    Expr.Call(args.[0],meth,[Expr.Value name;Expr.Coerce(args.[1], typeof<obj>)])))
                        rt.AddMember prop)
                    let parameters = 
                        sproc.Params
                        |> List.filter (fun p -> p.Direction = ParameterDirection.Input || p.Direction = ParameterDirection.InputOutput)
                        |> List.map(fun p -> ProvidedParameter(p.Name,p.ClrType))
                    let ty = typedefof<Microsoft.FSharp.Collections.List<_>>
                    let ty = ty.MakeGenericType rt
                    ProvidedMethod("Execute",parameters,ty,
                        InvokeCode = fun args -> 
                            let name = sproc.DbName
                            let paramtyp = typeof<string * DbType * ParameterDirection * int>
                            let ps = 
                                sproc.Params 
                                |> List.map(fun p -> Expr.NewTuple [Expr.Value p.Name; Expr.Value p.DbType; Expr.Value p.Direction; Expr.Value (if p.MaxLength.IsSome then p.MaxLength.Value else -1) ])

                            <@@ (((%%args.[0] : obj) :?> ISqlDataContext)).CallSproc(name,%%Expr.NewArray(paramtyp, ps), %%Expr.NewArray(typeof<obj>,List.map(fun e -> Expr.Coerce(e,typeof<obj>)) args.Tail)) @@>))
                containerType)
        
        sprocContainer.AddMembersDelayed(fun _ -> sprocs)
        sprocContainer.AddMembersDelayed(fun _ -> sprocs |> List.map(fun s -> ProvidedProperty(s.Name,s,GetterCode = fun args -> <@@ (((%%args.[0] : obj) :?> ISqlDataContext)) @@> )))

        serviceType.AddMembersDelayed( fun () ->
            [ yield sprocContainer :> MemberInfo
              for (KeyValue(key,(entityType,desc,_))) in baseTypes.Force() do
                // collection type, individuals type
                let (ct,it) = baseCollectionTypes.Force().[key]
                
                
                ct.AddMembersDelayed( fun () -> 
                    // creation methods.
                    // we are forced to load the columns here, but this is ok as the user has already 
                    // pressed . on an IQueryable type so they are obviously interested in using this entity..
                    let columns,_ = getTableData key 
                    let (optionalColumns,columns) = columns |> List.partition(fun c->c.IsNullable || c.IsPrimarKey)
                    let normalParameters = 
                        columns 
                        |> List.map(fun c -> ProvidedParameter(c.Name, c.ClrType))
                        |> List.sortBy(fun p -> p.Name)                 
                    let create1 = ProvidedMethod("Create", [], entityType, InvokeCode = fun args ->                         
                        <@@ 
                            let e = new SqlEntity(((%%args.[0] : obj ):?> IWithDataContext ).DataContext,key)
                            e._State <- Created
                            ((%%args.[0] : obj ):?> IWithDataContext ).DataContext.SubmitChangedEntity e
                            e 
                        @@> )
                    
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
                              let e = new SqlEntity(((%%dc : obj ):?> IWithDataContext ).DataContext,key)
                              e._State <- Created                            
                              e.SetData(%%columns : (string *obj) array)
                              ((%%dc : obj ):?> IWithDataContext ).DataContext.SubmitChangedEntity e
                              e 
                          @@>)

                    let create3 = ProvidedMethod("Create", [ProvidedParameter("data",typeof< (string*obj) seq >)] , entityType, InvokeCode = fun args -> 
                          let dc = args.[0]
                          let data = args.[1]
                          <@@
                              let e = new SqlEntity(((%%dc : obj ):?> IWithDataContext ).DataContext,key)
                              e._State <- Created                            
                              e.SetData(%%data : (string * obj) seq)
                              ((%%dc : obj ):?> IWithDataContext ).DataContext.SubmitChangedEntity e
                              e 
                          @@>)

                    
                    seq {
                     yield ProvidedProperty("Individuals",Seq.head it, GetterCode = fun args -> <@@ ((%%args.[0] : obj ):?> IWithDataContext ).DataContext @@> ) :> MemberInfo
                     yield create1 :> MemberInfo
                     if normalParameters.Length > 0 then yield create2 :> MemberInfo
                     yield create3 :> MemberInfo } |> Seq.toList
                )

   
                let prop = ProvidedProperty(buildTableName(ct.Name),ct, GetterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).CreateEntities(key) @@> )
                prop.AddXmlDoc (sprintf "<summary>%s</summary>" desc)
                yield entityType :> MemberInfo
                yield ct         :> MemberInfo                
                yield prop       :> MemberInfo
                yield! Seq.cast<MemberInfo> it

              yield ProvidedProperty("Stored Procedures",sprocContainer, GetterCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext) @@>)                     :> MemberInfo
              yield ProvidedMethod("SubmitUpdates",[],typeof<unit>,     InvokeCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).SubmitPendingChanges() @@>)  :> MemberInfo
              yield ProvidedMethod("GetUpdates",[],typeof<SqlEntity list>, InvokeCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).GetPendingEntities() @@>)  :> MemberInfo
              yield ProvidedMethod("ClearUpdates",[],typeof<SqlEntity list>,InvokeCode = fun args -> <@@ ((%%args.[0] : obj) :?> ISqlDataContext).ClearPendingChanges() @@>)  :> MemberInfo
             ] )

        let rootType = ProvidedTypeDefinition(sqlRuntimeInfo.RuntimeAssembly,ns,rootTypeName,baseType=Some typeof<obj>, HideObjectMethods=true)
        rootType.AddMembers [ serviceType ]
        rootType.AddMembersDelayed (fun () -> 
            [ let meth = 
                ProvidedMethod ("GetDataContext", [],
                                serviceType, IsStaticMethod=true,
                                InvokeCode = (fun _ -> 
                                    <@@ SqlDataContext(rootTypeName,conString,this.CreateSqlProvider(),owner) :> ISqlDataContext @@>))

              meth.AddXmlDoc "<summary>Returns an instance of the SQL Provider using the static parameters</summary>"
                   
              yield meth

              let meth = ProvidedMethod ("GetDataContext", [ProvidedParameter("connectionString",typeof<string>);], 
                                                            serviceType, IsStaticMethod=true,
                                                            InvokeCode = (fun args ->
                                                                <@@ SqlDataContext(rootTypeName, %%args.[0], this.CreateSqlProvider(), owner) :> ISqlDataContext @@> ))
                      
              meth.AddXmlDoc "<summary>Returns an instance of the SQL Provider</summary>
                              <param name='connectionString'>The database connection string</param>"
              yield meth


              let meth = ProvidedMethod ("GetDataContext", [ProvidedParameter("connectionString",typeof<string>);ProvidedParameter("resolutionPath",typeof<string>);],
                                                            serviceType, IsStaticMethod=true,
                                                            InvokeCode = (fun args -> <@@ SqlDataContext(rootTypeName,%%args.[0],this.CreateSqlProvider(), owner) :> ISqlDataContext  @@>))

              meth.AddXmlDoc "<summary>Returns an instance of the SQL Provider</summary>
                              <param name='connectionString'>The database connection string</param>
                              <param name='resolutionPath'>The location to look for dynamically loaded assemblies containing database vendor specific connections and custom types</param>"
              yield meth
            ])
        if (dbVendor <> DatabaseProviderTypes.MSACCESS) then con.Close()
        rootType

    abstract member CreateSqlProvider : unit -> ISqlProvider
                            