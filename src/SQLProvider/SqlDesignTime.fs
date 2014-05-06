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

[<TypeProvider>]
type SqlTypeProvider(config: TypeProviderConfig) as this =     
    inherit TypeProviderForNamespaces()
    let sqlRuntimeInfo = SqlRuntimeInfo(config)
    let ns = "FSharp.Data.Sql";   
    let asm = Assembly.GetExecutingAssembly()
    
    let createTypes(conString,dbVendor,resolutionPath,individualsAmount,useOptionTypes,owner, rootTypeName) =       
        let prov = Common.Utilities.createSqlProvider dbVendor resolutionPath owner
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
        let getTableColumns name = tableColumns.Force().[name].Force()
        let serviceType = ProvidedTypeDefinition( "SqlService", Some typeof<SqlDataContext>, HideObjectMethods = true)
        
        // first create all the types so we are able to recursively reference them in each other's definitions
        let baseTypes =
            lazy
                dict [ for table in tables.Force() do
                        let t = ProvidedTypeDefinition(table.FullName + "Entity", Some typeof<SqlEntity>, HideObjectMethods = true)
                        t.AddMemberDelayed(fun () -> ProvidedConstructor([ProvidedParameter("connection",typeof<string>)],InvokeCode = fun args -> <@@ new SqlEntity((%%args.[0] : string) ,table.FullName) @@>))
                        let desc = (sprintf "An instance of the %s %s belonging to schema %s" table.Type table.Name table.Schema)
                        t.AddXmlDoc desc
                        yield table.FullName,(t,sprintf "The %s %s belonging to schema %s" table.Type table.Name table.Schema,"") ]

        let createIndividualsType (table:Table) =
            let (et,_,_) = baseTypes.Force().[table.FullName]
            let t = ProvidedTypeDefinition(table.Schema + "." + table.Name + "." + "Individuals", None, HideObjectMethods = true)
            let individualsTypes = ResizeArray<_>()
            individualsTypes.Add t
            
            t.AddXmlDocDelayed(fun _ -> sprintf "A sample of %s individuals from the SQL object as supplied in the static parameters" table.Name)
            t.AddMember(ProvidedConstructor([ProvidedParameter("sqlService", typeof<IWithConnectionString>)]))
            t.AddMembersDelayed( fun _ ->
               prov.GetColumns(con,table) |> ignore 
               match prov.GetPrimaryKey table with
               | Some pk ->
                   let entities = 
                        use com = prov.CreateCommand(con,prov.GetIndividualsQueryText(table,individualsAmount))
                        if con.State <> ConnectionState.Open then con.Open()
                        use reader = com.ExecuteReader()
                        let ret = SqlEntity.FromDataReader(conString,table.FullName,reader)
                        if (dbVendor <> DatabaseProviderTypes.MSACCESS) then con.Close()
                        ret
                   if entities.IsEmpty then [] else
                   let e = entities.Head
                   // for each column in the entity except the primary key, creaate a new type that will read ``As Column 1`` etc
                   // inside that type the individuals will be listed again but with the text for the relevant column as the name 
                   // of the property and the primary key eg ``1, Dennis The Squirrel``
                   let propertyMap =
                      prov.GetColumns(con,table)
                      |> Seq.choose(fun col -> 
                        if col.Name = pk then None else
                        let name = table.Schema + "." + table.Name + "." + col.Name + "Individuals"
                        let ty = ProvidedTypeDefinition(name, None, HideObjectMethods = true )
                        ty.AddMember(ProvidedConstructor([ProvidedParameter("sqlService", typeof<IWithConnectionString>)]))
                        individualsTypes.Add ty
                        Some(col.Name,(ty,ProvidedProperty(sprintf "As %s" col.Name,ty, GetterCode = fun args -> <@@ %%args.[0] : obj  @@> ))))
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
                            |> Seq.iter(fun kvp -> 
                               if kvp.Key = pk then () else      
                               (fst propertyMap.[kvp.Key]).AddMemberDelayed(
                                  fun()->ProvidedProperty(sprintf "%s, %s" (pkValue.ToString()) (kvp.Value.ToString()) ,et,
                                            GetterCode = fun args -> <@@ SqlDataContext._GetIndividual(rootTypeName,((%%(args.[0]) : obj) :?> IWithConnectionString).ConnectionString,name,pkValue) @@> )))
                            // return the primary key property
                            Some <| ProvidedProperty(pkValue.ToString(),et,GetterCode = fun args -> <@@ SqlDataContext._GetIndividual(rootTypeName,((%%(args.[0]) : obj ) :?> IWithConnectionString).ConnectionString, name,pkValue) @@> )
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
                        let ct = ProvidedTypeDefinition(table.FullName + "Set", None,HideObjectMethods=false)
                        ct.AddInterfaceImplementationsDelayed( fun () -> [ProvidedTypeBuilder.MakeGenericType(typedefof<System.Linq.IQueryable<_>>,[et :> Type]); typeof<IWithConnectionString>])
                        let it = createIndividualsType table 
                        yield table.FullName,(ct,it) ]
        
        // add the attributes and relationships
        for KeyValue(key,(t,desc,_)) in baseTypes.Force() do 
            t.AddMembersDelayed(fun () -> 
                let (columns,(children,parents)) = getTableColumns key
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
                            <@@ SqlDataContext._CreateRelated(rootTypeName,(%%(args.[0]) : SqlEntity), name,pt,pk,ft,fk,"",RelationshipDirection.Children) @@> )
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
                            <@@ SqlDataContext._CreateRelated(rootTypeName,(%%(args.[0]) : SqlEntity), name,pt,pk,ft,fk,"",RelationshipDirection.Parents) @@> )
                        prop.AddXmlDoc(sprintf "Related %s entities from the primary side of the relationship, where the primary key is %s and the foreign key is %s" r.PrimaryTable r.PrimaryKey r.ForeignKey)
                        yield prop ]
                attProps @ relProps)

        // add sprocs 
        let sprocContainer = ProvidedTypeDefinition("SprocContainer",Some typeof<SqlDataContext>)
        sprocContainer.AddMember(ProvidedConstructor([ProvidedParameter("sqlDataContext", typeof<SqlDataContext>)]))
        let genSprocs() =
            sprocData.Force()
            |> List.map(fun sproc -> 
                let rt = ProvidedTypeDefinition(sproc.FullName,Some typeof<SqlEntity>)
                rt.AddMember(ProvidedConstructor([]))
                serviceType.AddMember rt
                sproc.ReturnColumns
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
                    rt.AddMember prop
                )
                let parameters = 
                    sproc.Params
                    |> List.map(fun p -> ProvidedParameter(p.Name,p.ClrType))
                let ty = typedefof<Microsoft.FSharp.Collections.List<_>>
                let ty = ty.MakeGenericType rt
                ProvidedMethod(sproc.FullName,parameters,ty,
                    InvokeCode = fun args -> 
                        let name = sproc.FullName
                        let rawNames = sproc.Params |> List.map(fun p -> p.Name) |> Array.ofList
                        let rawTypes = sproc.Params |> List.map(fun p -> p.DbType) |> Array.ofList
                        <@@ SqlDataContext._CallSproc(rootTypeName,(%%(args.[0]) : SqlDataContext).ConnectionString, name,rawNames,rawTypes, %%Expr.NewArray(typeof<obj>,List.map(fun e -> Expr.Coerce(e,typeof<obj>)) args.Tail)) @@>)
                )
        sprocContainer.AddMembersDelayed(fun _ -> genSprocs())

        serviceType.AddMembersDelayed( fun () ->
            [ yield sprocContainer :> MemberInfo
              for (KeyValue(key,(t,desc,_))) in baseTypes.Force() do
                let (ct,it) = baseCollectionTypes.Force().[key]
                let prop = ProvidedProperty("Individuals",Seq.head it, GetterCode = fun args -> <@@ (%%(args.[0]) : obj) @@> )
                ct.AddMemberDelayed( fun () -> prop :> MemberInfo )                    
                let prop = ProvidedProperty(buildTableName(ct.Name),ct, GetterCode = fun args -> <@@ SqlDataContext._CreateEntities(rootTypeName,(%%(args.[0]) : SqlDataContext).ConnectionString,key) @@> )
                prop.AddXmlDoc (sprintf "<summary>%s</summary>" desc)
                yield t :> MemberInfo
                yield ct :> MemberInfo
                yield! it |> Seq.map( fun it -> it :> MemberInfo)
                yield prop :> MemberInfo
              yield ProvidedProperty("Stored Procedures",sprocContainer,GetterCode = fun args -> <@@ (%%(args.[0]) : SqlDataContext) @@>) :> MemberInfo
             ] )

        let rootType = ProvidedTypeDefinition(sqlRuntimeInfo.RuntimeAssembly,ns,rootTypeName,baseType=Some typeof<obj>, HideObjectMethods=true)
        rootType.AddMembers [ serviceType ]
        rootType.AddMembersDelayed (fun () -> 
            [ let meth = 
                ProvidedMethod ("GetDataContext", [],
                                serviceType, IsStaticMethod=true,
                                InvokeCode = (fun _ -> 
                                    let meth = typeof<SqlDataContext>.GetMethod "_Create"
                                    Expr.Call(meth, [Expr.Value rootTypeName; Expr.Value conString; Expr.Value dbVendor; Expr.Value resolutionPath; Expr.Value owner])
                                    ))
              meth.AddXmlDoc "<summary>Returns an instance of the Sql provider using the static parameters</summary>"
                   
              yield meth

              let meth = ProvidedMethod ("GetDataContext", [ProvidedParameter("connectionString",typeof<string>);], 
                                                            serviceType, IsStaticMethod=true,
                                                            InvokeCode = (fun args ->
                                                                let meth = typeof<SqlDataContext>.GetMethod "_Create"
                                                                Expr.Call(meth, [Expr.Value rootTypeName;args.[0];Expr.Value dbVendor; Expr.Value resolutionPath; Expr.Value owner])))
                      
              meth.AddXmlDoc "<summary>Retuns an instance of the Sql provider</summary>
                              <param name='connectionString'>The database connection string</param>"
              yield meth


              let meth = ProvidedMethod ("GetDataContext", [ProvidedParameter("connectionString",typeof<string>);ProvidedParameter("resolutionPath",typeof<string>);],
                                                            serviceType, IsStaticMethod=true,
                                                            InvokeCode = (fun args ->
                                                                let meth = typeof<SqlDataContext>.GetMethod "_Create"
                                                                Expr.Call(meth, [Expr.Value rootTypeName;args.[0];Expr.Value dbVendor; args.[1]; Expr.Value owner])))

              meth.AddXmlDoc "<summary>Retuns an instance of the Sql provider</summary>
                              <param name='connectionString'>The database connection string</param>
                              <param name='resolutionPath'>The location to look for dynamically loaded assemblies containing database vendor specifc connections and custom types</param>"
              yield meth
            ])
        if (dbVendor <> DatabaseProviderTypes.MSACCESS) then con.Close()
        rootType
    
    let paramSqlType = ProvidedTypeDefinition(sqlRuntimeInfo.RuntimeAssembly, ns, "SqlDataProvider", Some(typeof<obj>), HideObjectMethods = true)
    
    let conString = ProvidedStaticParameter("ConnectionString",typeof<string>)    
    let optionTypes = ProvidedStaticParameter("UseOptionTypes",typeof<bool>,false)
    let dbVendor = ProvidedStaticParameter("DatabaseVendor",typeof<DatabaseProviderTypes>,DatabaseProviderTypes.MSSQLSERVER)
    let individualsAmount = ProvidedStaticParameter("IndividualsAmount",typeof<int>,1000)
    let owner = ProvidedStaticParameter("Owner", typeof<string>, "")    
    let resolutionPath = ProvidedStaticParameter("ResolutionPath",typeof<string>,"")    
    let helpText = "<summary>Typed representation of a database</summary>
                    <param name='ConnectionString'>The connection string for the sql server</param>
                    <param name='DatabaseVendor'> The target database vendor</param>
                    <param name='IndividualsAmount'>The amount of sample entities to project into the type system for each sql entity type. Default 1000.</param>
                    <param name='UseOptionTypes'>If true, F# option types will be used in place of nullable database columns.  If false, you will always receive the default value of the column's type even if it is null in the database.</param>
                    <param name='ResolutionPath'>The location to look for dynamically loaded assemblies containing database vendor specifc connections and custom types.</param>
                    <param name='Owner'>The owner of the schema for this provider to resolve (Oracle Only)</param>"
        
    do paramSqlType.DefineStaticParameters([conString;dbVendor;resolutionPath;individualsAmount;optionTypes;owner], fun typeName args -> 
        createTypes(args.[0] :?> string,                  // OrganizationServiceUrl
                    args.[1] :?> DatabaseProviderTypes,   // db vendor
                    args.[2] :?> string,                  // Assembly resolution path for db connectors and custom types
                    args.[3] :?> int,                     // Individuals Amount
                    args.[4] :?> bool,                    // Use option types?
                    args.[5] :?> string,                  // Schema owner currently only used for oracle
                    typeName))

    do paramSqlType.AddXmlDoc helpText               
    
    // add them to the namespace    
    do this.AddNamespace(ns, [paramSqlType])
                            
[<assembly:TypeProviderAssembly>] 
do()


