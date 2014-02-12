namespace FSharp.Data.Sql.Common
    // I don't really like having these in this file..
    module internal Utilities =
        open FSharp.Data.Sql.Providers

        let createSqlProvider vendor resolutionPath owner =
            match vendor with                
            | DatabaseProviderTypes.MSSQLSERVER -> MSSqlServerProvider() :> ISqlProvider
            | DatabaseProviderTypes.SQLITE -> SQLiteProvider(resolutionPath) :> ISqlProvider
            | DatabaseProviderTypes.POSTGRESQL -> PostgresqlProvider(resolutionPath) :> ISqlProvider
            | DatabaseProviderTypes.MYSQL -> MySqlProvider(resolutionPath) :> ISqlProvider
            | DatabaseProviderTypes.ORACLE -> OracleProvider(resolutionPath, owner) :> ISqlProvider
            | DatabaseProviderTypes.MSACCESS -> MSAccessProvider() :> ISqlProvider
            | _ -> failwith "Unsupported database provider"        

        let resolveTuplePropertyName name (tupleIndex:string ResizeArray) =
            match name with // could do this by parsing the number from the end of the string...
            | "Item1" -> tupleIndex.[0] | "Item11" -> tupleIndex.[10]
            | "Item2" -> tupleIndex.[1] | "Item12" -> tupleIndex.[11]
            | "Item3" -> tupleIndex.[2] | "Item13" -> tupleIndex.[12]
            | "Item4" -> tupleIndex.[3] | "Item14" -> tupleIndex.[13]
            | "Item5" -> tupleIndex.[4] | "Item15" -> tupleIndex.[14]
            | "Item6" -> tupleIndex.[5] | "Item16" -> tupleIndex.[15]
            | "Item7" -> tupleIndex.[6] | "Item17" -> tupleIndex.[16]
            | "Item8" -> tupleIndex.[7] | "Item18" -> tupleIndex.[17]
            | "Item9" -> tupleIndex.[8] | "Item19" -> tupleIndex.[18]
            | "Item10"-> tupleIndex.[9] | "Item20" -> tupleIndex.[19]
            | _ -> failwith "currently only support up to 20 nested entity aliases"

namespace FSharp.Data.Sql.QueryExpression

open System
open System.Reflection
open System.Linq.Expressions
open System.Collections.Generic

open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Patterns
open FSharp.Data.Sql.Schema

module internal QueryExpressionTransformer =    
    /// Visitor has two uses - 1. extracting the columns the select statement
    /// 2. transform the projection epxression into something that will work with the SqlEntity runtime object eg it replaces chunks of the 
    // expression tree where fields are referenced with the relevant calls to GetColumn and GetSubTable
    type private ProjectionTransformer(tupleIndex:string ResizeArray,BaseTableParam:ParameterExpression,baseTableAlias,aliasEntityDict:Map<string,Table>) =
        inherit ExpressionVisitor()
        static let getSubEntityMi = typeof<SqlEntity>.GetMethod("GetSubTable",BindingFlags.NonPublic ||| BindingFlags.Instance)

        let mutable singleEntityName = ""

        /// holds the columns to select for each entity appearing in the projection tree, or a blank list if all columns         
        member val ProjectionMap = Dictionary<string,string ResizeArray>()

        override x.VisitLambda(exp) = 
            if exp.Parameters.Count = 1 && exp.Parameters.[0].Type = typeof<SqlEntity> then
                // this is a speical case when there were no select manys and as a result the projection parameter is just the single entity rather than a tuple
                // this still includes cases where tuples are created by the user directly, that is fine - it is for avoiding linq auto generated tuples
                singleEntityName <- exp.Parameters.[0].Name
                match x.ProjectionMap.TryGetValue singleEntityName with
                | true, values -> ()
                | false, _ -> x.ProjectionMap.Add(singleEntityName,new ResizeArray<_>())
                let body = base.Visit exp.Body
                upcast Expression.Lambda(body,BaseTableParam) 
            else base.VisitLambda exp

        override x.VisitMethodCall(exp) =
            let(|PropName|) (pi:PropertyInfo) = pi.Name
            match exp with
            | MethodCall(Some(ParamName name | PropertyGet(_,PropName name)),(MethodWithName "GetColumn" | MethodWithName "GetColumnOption"),[FSharp.Data.Sql.Patterns.String key]) ->
                // add this attribute to the select list for the alias
                let alias = if tupleIndex.Count = 0 then singleEntityName else Utilities.resolveTuplePropertyName name tupleIndex
                match x.ProjectionMap.TryGetValue alias with
                | true, values -> values.Add key
                | false, _ -> x.ProjectionMap.Add(alias,new ResizeArray<_>(seq{yield key}))
            | _ -> ()
            base.VisitMethodCall exp

        override __.VisitParameter(exp) = 
            // special case as above
            if singleEntityName <> "" && exp.Type = typeof<SqlEntity> && exp.Name = singleEntityName then upcast BaseTableParam
            else base.VisitParameter exp 
                       
        override x.VisitMember(exp) = 
            // convert the member expression into a function call that retrieves the child entity from the result entity
            // only interested in anonymous objects that were created by the LINQ infrastructure
            // ignore other cases 
            if exp.Type = typeof<SqlEntity> && exp.Expression.Type.FullName.StartsWith "Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject" then             
                let (alias,name) = 
                    if baseTableAlias = "" then ("","")
                    else
                        let alias = Utilities.resolveTuplePropertyName exp.Member.Name tupleIndex
                        match x.ProjectionMap.TryGetValue alias with
                        | true, values -> ()
                        | false, _ -> x.ProjectionMap.Add(alias,new ResizeArray<_>())
                        (alias,aliasEntityDict.[alias].FullName)
            
                // convert this expression into a GetSubEntity call with the correct alias
                upcast
                    Expression.Convert(
                        Expression.Call(BaseTableParam,getSubEntityMi,Expression.Constant(alias),Expression.Constant(name))
                            ,exp.Type)   
                                     
            else base.VisitMember exp 
    
    let convertExpression exp (entityIndex:string ResizeArray) con (provider:ISqlProvider) =
        // first convert the abstract query tree into a more useful format
        let legaliseName (alias:alias) = 
                if alias.StartsWith("_") then alias.TrimStart([|'_'|]) else alias

        let entityIndex = new ResizeArray<_>(entityIndex |> Seq.map (legaliseName))
            
                 
        let sqlQuery = SqlQuery.ofSqlExp(exp,entityIndex)
        
         // note : the baseAlias here will always be "" when no criteria has been applied, because the LINQ tree never needed to refer to it     
        let baseAlias,baseTable =
            match sqlQuery.UltimateChild with
            | Some(baseAlias,baseTable) when baseAlias = ""-> (baseTable.Name,baseTable)
            | Some(baseAlias,baseTable) -> (baseAlias,baseTable)
            | _ -> failwith ""

        let (projectionDelegate,projectionColumns) = 
            let param = Expression.Parameter(typeof<SqlEntity>,"result")
            match sqlQuery.Projection with
            | Some(proj) -> let megatron = ProjectionTransformer(entityIndex,param,baseAlias,sqlQuery.Aliases)
                            let newProjection = megatron.Visit(proj) :?> LambdaExpression
                            (Expression.Lambda(newProjection.Body,param).Compile(),megatron.ProjectionMap)
            | none -> 
                // this case happens when there are only where clauses with a single table and a projection cotaining just the table's entire rows. example:
                // for x in dc.john 
                // where x.y = 1
                // select x
                // this does not create a call to .select() after .where(), therefore in this case we must provide our own projection that simply selects a whole row 
                let pmap = Dictionary<string,string ResizeArray>()
                pmap.Add(baseAlias, new ResizeArray<_>()) 
                (Expression.Lambda(param,param).Compile(),pmap)       

        // a special case here to handle queries that start from the relationship of an individual
        let sqlQuery,baseAlias = 
            if sqlQuery.Aliases.Count = 0 then 
               let alias =  projectionColumns.Keys |> Seq.head 
               { sqlQuery with UltimateChild = Some(alias,snd sqlQuery.UltimateChild.Value) }, alias
            else sqlQuery,baseAlias

        let resolve name =
            // name will be blank when there is only a single table as it never gets
            // tupled by the linq infrastructure. In this case we know it must be referring
            // to the only table in the query, so replace it
            if String.IsNullOrWhiteSpace(name) || name = "__base__" then (fst sqlQuery.UltimateChild.Value)
            else Utilities.resolveTuplePropertyName name entityIndex
            
        let rec resolveFilterList = function
            | And(xs,y) -> And(xs|>List.map(fun (a,b,c,d) -> resolve a,b,c,d),Option.map (List.map resolveFilterList) y)
            | Or(xs,y) -> Or(xs|>List.map(fun (a,b,c,d) -> resolve a,b,c,d),Option.map (List.map resolveFilterList) y)

        // the crazy linq infrastructure does all kinds of weird things with joins which means some information
        // is lost up and down the expression tree, but now with all the data available we can resolve the problems...

        // 1.
        // re-map the tuple arg names to their proper aliases in the filters
        // its possible to do this when converting the expression but its
        // much easier at this stage once we have knowledge of the whole query
        let sqlQuery = { sqlQuery with Filters = List.map resolveFilterList sqlQuery.Filters}
        
        // 2.
        // Some aliases will have blank table information, but these can be resolved by looking
        // in the link data or ultimate base entity
        let resolveAlias alias table =
            if table.Name <> "" then table else
            match sqlQuery.UltimateChild with
            | Some(uc) when alias = fst uc -> snd uc
            | _ -> sqlQuery.Links 
                    |> Map.pick(fun outerLinkAlias links ->
                        links 
                        |> List.tryPick(fun (innerAlias,linkData) -> 
                            if innerAlias = alias then Some(linkData.PrimaryTable) else None))     
        let sqlQuery = { sqlQuery with Aliases = Map.map resolveAlias sqlQuery.Aliases }
        
        // 3.
        // Some link data will be missing its foreign table data which needs setting to the resolved table of the 
        // outer alias - this happens depending on the which way the join is around - infromation is "lost" up the tree which
        // able to be resolved now.
        let resolveLinks outerAlias linkData =
            let resolved = sqlQuery.Aliases.[outerAlias]
            linkData 
            |> List.map(fun (a,data:LinkData) ->
                (a,if data.ForeignTable.Name <> "" then data else { data with ForeignTable = resolved }))
        let sqlQuery = { sqlQuery with Links = Map.map resolveLinks sqlQuery.Links }
        
        // make sure the provider has cached the columns for the tables within the projection
        projectionColumns
        |> Seq.iter(function KeyValue(k,_) ->  
                                let table = match sqlQuery.Aliases.TryFind k with
                                            | Some v -> v
                                            | None -> snd sqlQuery.UltimateChild.Value
                                provider.GetColumns (con,table) |> ignore )

        let (sql,parameters) = provider.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns)

        (sql,parameters,projectionDelegate,baseTable)
