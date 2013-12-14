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

type internal SqlRuntimeInfo (config : TypeProviderConfig) =
    let runtimeAssembly = Assembly.LoadFrom(config.RuntimeAssembly)    
    member this.RuntimeAssembly = runtimeAssembly   

[<TypeProvider>]
type SqlTypeProvider(config: TypeProviderConfig) as this =     
    inherit TypeProviderForNamespaces()
    let sqlRuntimeInfo = SqlRuntimeInfo(config)
    let ns = "FSharp.Data.Sql"     
    let asm = Assembly.GetExecutingAssembly()
    
    let createTypes(conString,(*nullables,*)dbVendor,resolutionPath,individualsAmount,rootTypeName) =       
        let prov = Common.Utilities.createSqlProvider dbVendor resolutionPath
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
                        t.AddMemberDelayed(fun () -> ProvidedConstructor([],InvokeCode = fun _ -> <@@ new SqlEntity(table.FullName) @@>  ))
                        let desc = (sprintf "An instance of the %s %s belonging to schema %s" table.Type table.Name table.Schema)
                        t.AddXmlDoc desc
                        yield table.FullName,(t,sprintf "The %s %s belonging to schema %s" table.Type table.Name table.Schema,"") ]

        let createIndividualsType (table:Table) =
            let (et,_,_) = baseTypes.Force().[table.FullName]
            let t = ProvidedTypeDefinition(table.Schema + "." + table.Name + "." + "Individuals", Some typeof<obj>, HideObjectMethods = true)
            let individualsTypes = ResizeArray<_>()
            individualsTypes.Add t
            
            t.AddXmlDocDelayed(fun _ -> sprintf "A sample of %s individuals from the SQL object as supplied in the static parameters" table.Name)
            t.AddMembersDelayed( fun _ ->
               prov.GetColumns(con,table) |> ignore 
               match prov.GetPrimaryKey table with
               | Some pk ->
                   let entities =   
                        use com = prov.CreateCommand(con,prov.GetIndividualsQueryText(table,individualsAmount))
                        use reader = com.ExecuteReader()
                        SqlEntity.FromDataReader(table.FullName,reader)
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
                        individualsTypes.Add ty
                        Some(col.Name,(ty,ProvidedProperty(sprintf "As %s" col.Name,ty, GetterCode = fun _ -> <@@ new obj() @@> ))))
                      |> Map.ofSeq
                    
                   // on the main object create a property for each entity simply using the primary key 
                   let props =
                      entities
                      |> List.map(fun e ->  
                         let pkValue = e.GetColumn pk
                         let name = table.FullName
                         // this next bit is just side effect to populate the "As Column" types
                         e.ColumnValues 
                         |> Seq.iter(fun kvp -> 
                            if kvp.Key = pk then () else                         
                            (fst propertyMap.[kvp.Key]).AddMemberDelayed(
                               fun()->ProvidedProperty(sprintf "%s, %s" (pkValue.ToString()) (kvp.Value.ToString()) ,et,
                                         GetterCode = fun _ -> <@@ SqlDataContext._GetIndividual(name,pkValue) @@> )))
                         // return the primary key property
                         ProvidedProperty(pkValue.ToString(),et,GetterCode = fun _ -> <@@ SqlDataContext._GetIndividual(name,pkValue) @@> ))
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
                        ct.AddInterfaceImplementationsDelayed( fun () -> [ProvidedTypeBuilder.MakeGenericType(typedefof<System.Linq.IQueryable<_>>,[et :> Type])])
                        let it = createIndividualsType table 
                        let prop = ProvidedProperty("Individuals",Seq.head it, GetterCode = fun _ -> <@@ new obj() @@> )
                        prop.AddXmlDoc(sprintf "A sample of %s individuals from the SQL object" name)
                        ct.AddMemberDelayed( fun () -> prop :> MemberInfo )                        
//                        let meth = ProvidedMethod("Create",[],et, IsStaticMethod = false, InvokeCode = fun _ -> <@@ SqlEntity(name) @@>)
//                        meth.AddXmlDoc(sprintf "Creates a new instance of the %s entity" name)
//                        ct.AddMemberDelayed( fun () -> meth :> MemberInfo )
                        yield table.FullName,(ct,it) ]  
        
        // add the attributes and relationships
        for KeyValue(key,(t,desc,_)) in baseTypes.Force() do 
            t.AddMembersDelayed(fun () -> 
                let (columns,(children,parents)) = getTableColumns key
                let attProps = 
                    let createColumnProperty ty (name:string) description =
                        let prop = 
                            ProvidedProperty(
                                name,ty,
                                GetterCode = (fun args ->
                                    let meth = typeof<SqlEntity>.GetMethod("GetColumn").MakeGenericMethod([|ty|])
                                    Expr.Call(args.[0],meth,[Expr.Value name])),
                                SetterCode = (fun args ->
                                    let meth = typeof<SqlEntity>.GetMethod "SetColumn" 
                                    Expr.Call(args.[0],meth,[Expr.Value name;Expr.Coerce(args.[1], typeof<obj>)])))
                        prop
                    columns |> List.map (fun c -> createColumnProperty c.ClrType c.Name "")
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
                            <@@ SqlDataContext._CreateRelated((%%(args.[0]) : SqlEntity), name,pt,pk,ft,fk,"",RelationshipDirection.Children) @@> )
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
                            <@@ SqlDataContext._CreateRelated((%%(args.[0]) : SqlEntity), name,pt,pk,ft,fk,"",RelationshipDirection.Parents) @@> )
                        prop.AddXmlDoc(sprintf "Related %s entities from the primary side of the relationship, where the primary key is %s and the foreign key is %s" r.PrimaryTable r.PrimaryKey r.ForeignKey)
                        yield prop ]
                attProps @ relProps)

        // add sprocs 
        let sprocContainer = ProvidedTypeDefinition("SprocContainer",None)
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
                        <@@ SqlDataContext._CallSproc(name,rawNames,rawTypes, %%Expr.NewArray(typeof<obj>,List.map(fun e -> Expr.Coerce(e,typeof<obj>)) args.Tail)) @@>)
                )
        sprocContainer.AddMembersDelayed(fun _ -> genSprocs())

        serviceType.AddMembersDelayed( fun () ->
            [ yield sprocContainer :> MemberInfo
              for (KeyValue(key,(t,desc,_))) in baseTypes.Force() do
                let (ct,it) = baseCollectionTypes.Force().[key]
                let prop = ProvidedProperty(ct.Name.Substring(0,ct.Name.LastIndexOf("]")+1),ct, GetterCode = fun args -> <@@ SqlDataContext._CreateEntities(key) @@> )
                prop.AddXmlDoc (sprintf "<summary>%s</summary>" desc)
                yield t :> MemberInfo
                yield ct :> MemberInfo
                yield! it |> Seq.map( fun it -> it :> MemberInfo)
                yield prop :> MemberInfo
              yield ProvidedProperty("Stored Procedures",sprocContainer,GetterCode = fun _ -> <@@ obj() @@>) :> MemberInfo
             ] )

        let rootType = ProvidedTypeDefinition(sqlRuntimeInfo.RuntimeAssembly,ns,rootTypeName,baseType=Some typeof<obj>, HideObjectMethods=true)
        rootType.AddMembers [ serviceType ]
        rootType.AddMembersDelayed (fun () -> 
            [ let meth = 
                ProvidedMethod ("GetDataContext", [],
                                serviceType, IsStaticMethod=true,
                                InvokeCode = (fun _ -> 
                                    let meth = typeof<SqlDataContext>.GetMethod "_Create"
                                    Expr.Call(meth, [Expr.Value conString; Expr.Value dbVendor; Expr.Value resolutionPath])
                                    ))
              meth.AddXmlDoc "<summary>Returns an instance of the Sql provider using the static parameters</summary>"
                   
              yield meth
//                                        
//              let meth = ProvidedMethod ("GetDataContext", [ProvidedParameter("connection",typeof<IDbConnection>)], 
//                                                            serviceType, IsStaticMethod=true,
//                                                            InvokeCode = (fun args ->
//                                                                let meth = typeof<SqlDataContext>.GetMethod "_CreateWithInstance"
//                                                                Expr.Call(meth, [args.[0];])));
//              meth.AddXmlDoc "<summary>Retuns an instance of the Sql provider</summary>
//                              <param name='connection'>An instance of a datbase connection</param>"
//              yield meth

              let meth = ProvidedMethod ("GetDataContext", [ProvidedParameter("connectionString",typeof<string>);], 
                                                            serviceType, IsStaticMethod=true,
                                                            InvokeCode = (fun args ->
                                                                let meth = typeof<SqlDataContext>.GetMethod "_Create"
                                                                Expr.Call(meth, [args.[0];Expr.Value dbVendor; Expr.Value resolutionPath])))
                      
              meth.AddXmlDoc "<summary>Retuns an instance of the Sql provider</summary>
                              <param name='connectionString'>The database connection string</param>"
              yield meth                                
              
            ])
        rootType
    
    let paramSqlType = ProvidedTypeDefinition(sqlRuntimeInfo.RuntimeAssembly, ns, "SqlDataProvider", Some(typeof<obj>), HideObjectMethods = true)
    
    let conString = ProvidedStaticParameter("ConnectionString",typeof<string>)    
    //let nullables = ProvidedStaticParameter("UseNullableValues",typeof<bool>,false)
    let dbVendor = ProvidedStaticParameter("DatabaseVendor",typeof<DatabaseProviderTypes>,DatabaseProviderTypes.MSSQLSERVER)
    let individualsAmount = ProvidedStaticParameter("IndividualsAmount",typeof<int>,1000)    
    let resolutionPath = ProvidedStaticParameter("ResolutionPath",typeof<string>,"")    
    let helpText = "<summary>Typed representation of a database</summary>
                    <param name='ConnectionString'>The connection string for the sql server</param>
                    <param name='DatabaseVendor'> The target database vendor</param>
                    <param name='IndividualsAmount'>The amount of sample entities to project into the type system for each sql entity type. Default 1000.</param>
                    <param name='ResolutionPath'>The location to look for dynamically loaded assemblies containing database vendor specifc connections and custom types.</param>"
        
    do paramSqlType.DefineStaticParameters([conString;dbVendor;resolutionPath;individualsAmount], fun typeName args -> 
        createTypes(args.[0] :?> string,                  // OrganizationServiceUrl
                    args.[1] :?> DatabaseProviderTypes,   // db vendor
                    args.[2] :?> string,                  // Assembly resolution path for db connectors and custom types
                    args.[3] :?> int,                     // Indiduals Amount
                    
                    typeName))

    do paramSqlType.AddXmlDoc helpText               

    // add them to the namespace    
    do this.AddNamespace(ns, [paramSqlType])
                            
[<assembly:TypeProviderAssembly>] 
do()


