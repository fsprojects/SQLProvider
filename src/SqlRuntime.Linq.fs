namespace FSharp.Data.Sql.Runtime

open System
open System.Linq
open System.ServiceModel.Description
open System.Reflection
open System.Collections.Generic
open System.Data

open FSharp.Data.Sql
open FSharp.Data.Sql.Common
open FSharp.Data.Sql.QueryExpression
open FSharp.Data.Sql.Schema

module internal QueryImplementation = 
    open System.Linq
    open System.Linq.Expressions
    open System.Reflection
    open Patterns       

    type IWithSqlService = 
        abstract ConnectionString : string
        abstract SqlExpression : SqlExp  
        abstract TupleIndex : string ResizeArray // indexes where in the anonymous object created by the compiler during a select many that each entity alias appears       
        abstract Provider : ISqlProvider
    
    let (|SourceWithQueryData|_|) = function Constant ((:? IWithSqlService as org), _)    -> Some org | _ -> None     
    let (|RelDirection|_|)        = function Constant ((:? RelationshipDirection as s),_) -> Some s   | _ -> None

    let executeQuery conString (provider:ISqlProvider) sqlExp ti =        
       use con = provider.CreateConnection(conString) 
       con.Open()
       let (query,parameters,projector,baseTable) = QueryExpressionTransformer.convertExpression sqlExp ti con provider
       Common.QueryEvents.PublishSqlQuery query
       // todo: make this lazily evaluated? or optionally so. but have to deal with disposing stuff somehow       
       use cmd = provider.CreateCommand(con,query)   
       for p in parameters do cmd.Parameters.Add p |> ignore
       let results = SqlEntity.FromDataReader(baseTable.FullName, cmd.ExecuteReader())       
       let results = seq { for e in results -> projector.DynamicInvoke(e) } |> Seq.cache :> System.Collections.IEnumerable
       con.Close()
       results
       
    type SqlQueryable<'T>(conString:string,provider,sqlQuery,tupleIndex) =       
        static member Create(table,conString,provider) = 
            SqlQueryable<'T>(conString,provider,BaseTable("",table),ResizeArray<_>()) :> IQueryable<'T> 
        interface IQueryable<'T>
        interface IQueryable with
            member x.Provider = SqlQueryProvider.Provider
            member x.Expression =  Expression.Constant(x,typeof<IQueryable<'T>>) :> Expression 
            member x.ElementType = typeof<'T>
        interface seq<'T> with 
             member x.GetEnumerator() = (Seq.cast<'T> (executeQuery conString provider sqlQuery tupleIndex)).GetEnumerator()
        interface System.Collections.IEnumerable with 
             member x.GetEnumerator() = (x :> seq<'T>).GetEnumerator() :> System.Collections.IEnumerator
        interface IWithSqlService with 
             member x.ConnectionString = conString
             member x.SqlExpression = sqlQuery
             member x.TupleIndex = tupleIndex
             member x.Provider = provider

    and SqlOrderedQueryable<'T>(conString:string,provider,sqlQuery,tupleIndex) =       
        static member Create(table,conString,provider) = 
            SqlOrderedQueryable<'T>(conString,provider,BaseTable("",table),ResizeArray<_>()) :> IQueryable<'T> 
        interface IOrderedQueryable<'T>
        interface IQueryable<'T> 
        interface IQueryable with 
            member x.Provider = SqlQueryProvider.Provider
            member x.Expression =  Expression.Constant(x,typeof<IOrderedQueryable<'T>>) :> Expression 
            member x.ElementType = typeof<'T>
        interface seq<'T> with 
             member x.GetEnumerator() = (Seq.cast<'T> (executeQuery conString provider sqlQuery tupleIndex)).GetEnumerator()
        interface System.Collections.IEnumerable with 
             member x.GetEnumerator() = (x :> seq<'T>).GetEnumerator() :> System.Collections.IEnumerator
        interface IWithSqlService with 
             member x.ConnectionString = conString
             member x.SqlExpression = sqlQuery
             member x.TupleIndex = tupleIndex
             member x.Provider = provider

    and SqlQueryProvider() =
         static member val Provider = 
             { new System.Linq.IQueryProvider with 
                member provider.CreateQuery(e:Expression) : IQueryable = failwithf "CreateQuery, e = %A" e
                member provider.CreateQuery<'T>(e:Expression) : IQueryable<'T> =                     
                    Common.QueryEvents.PublishExpression e
                    match e with                    
                    | MethodCall(None, (MethodWithName "Skip" as meth), [SourceWithQueryData source; Int amount]) ->                                                
                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0]) 
                        ty.GetConstructors().[0].Invoke [| source.ConnectionString ; source.Provider; Skip(amount,source.SqlExpression) ; source.TupleIndex; |] :?> IQueryable<_>                         

                    | MethodCall(None, (MethodWithName "Take" as meth), [SourceWithQueryData source; Int amount]) ->                                                
                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0]) 
                        ty.GetConstructors().[0].Invoke [| source.ConnectionString ; source.Provider; Take(amount,source.SqlExpression) ; source.TupleIndex; |] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "OrderBy" | MethodWithName "OrderByDescending" as meth), [SourceWithQueryData source; OptionalQuote (Lambda([ParamName param], SqlColumnGet(entity,key,_))) ]) ->
                        let alias = if entity = "" then param else Utilities.resolveTuplePropertyName entity source.TupleIndex
                        let ty = typedefof<SqlOrderedQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0]) 
                        let ascending = meth.Name = "OrderBy"
                        let x = ty.GetConstructors().[0].Invoke [| source.ConnectionString ; source.Provider; OrderBy(alias,key,ascending,source.SqlExpression) ; source.TupleIndex; |] 
                        x :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "ThenBy" | MethodWithName "ThenByDescending" as meth), [SourceWithQueryData source; OptionalQuote (Lambda([ParamName param], SqlColumnGet(entity,key,_))) ]) ->
                        let alias = if entity = "" then param else Utilities.resolveTuplePropertyName entity source.TupleIndex
                        let ty = typedefof<SqlOrderedQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0]) 
                        let ascending = meth.Name = "ThenBy"
                        match source.SqlExpression with 
                        | OrderBy(_) ->
                            let x = ty.GetConstructors().[0].Invoke [| source.ConnectionString; source.Provider; OrderBy(alias,key,ascending,source.SqlExpression) ; source.TupleIndex; |] 
                            x :?> IQueryable<_>
                        | _ -> failwith (sprintf "'thenBy' operations must come immediately after a 'sortBy' operation in a query")

                    | MethodCall(None, (MethodWithName "Distinct" as meth), [ SourceWithQueryData source ]) ->
                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])                            
                        ty.GetConstructors().[0].Invoke [| source.ConnectionString; source.Provider; Distinct(source.SqlExpression) ; source.TupleIndex; |] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "Where" as meth), [ SourceWithQueryData source; OptionalQuote qual ]) ->
                        let paramNames = HashSet<string>()

                        let (|Condition|_|) exp =   
                            // IMPORTANT : for now it is always assumed that the table column being checked on the server side is on the left hand side of the condition expression.
                            match exp with
                            | SqlSpecialOpArr(ti,op,key,value) -> 
                                paramNames.Add(ti) |> ignore
                                Some(ti,key,op,Some (box value))
                            | SqlSpecialOp(ti,op,key,value) ->  
                                paramNames.Add(ti) |> ignore
                                Some(ti,key,op,Some value)
                            // matches column to constant with any operator eg c => c.name = "john", c => c.age > 42
                            | SqlCondOp(op,(SqlColumnGet(ti,key,_)),ConstantOrNullableConstant(c)) -> 
                                paramNames.Add(ti) |> ignore
                                Some(ti,key,op,c)
                            // if the left side is a memberexpression it is likely referencing a column of a table in a join, or the Value / HasValue property of a nullable type.
                            // when accessing the value checking the column as normal, the Value can be ignored.  HasValue should be translated into
                            // Null / Not null 
                            | PropertyGet(Some(SqlColumnGet(ti,key,_)),pi) when pi.Name = "HasValue" -> 
                                paramNames.Add(ti) |> ignore
                                Some(ti,key,ConditionOperator.NotNull,None)
                            | SqlCondOp(op,PropertyGet(Some(SqlColumnGet(ti,key,_)),pi),ConstantOrNullableConstant(c)) when pi.Name = "Value" -> 
                                // this is a special case for nullable enums, which you cannot use the nullable operators with due to restrictions in the type provider
                                // mechanics meaning there is no way to trick the compiler into treating the provided enum as a value type.
                                // this is a workaround allowing the nullable enums to be used in the format where  a.code.Value = Enum.Item
                                paramNames.Add(ti) |> ignore
                                Some(ti,key,op,c)
                            | SqlCondOp(op,PropertyGet(Some(SqlColumnGet(ti,key,_) ),pi),Bool(c)) when pi.Name = "HasValue"  -> 
                                paramNames.Add(ti) |> ignore
                                match c with
                                | true -> Some(ti,key,ConditionOperator.NotNull,None)
                                | false -> Some(ti,key,ConditionOperator.IsNull,None)
                            | SqlCondOp(op,(:? MemberExpression as methL),ConstantOrNullableConstant(c)) -> 
                                let ti,key =  
                                  match methL.Expression with
                                  | SqlColumnGet(ti,key,ty) -> ti,key
                                  | _ -> failwith "Unsupported member expression on left side"    
                                paramNames.Add(ti) |> ignore
                                Some(ti,key,op,c)
                            // matches to another property getter, method call or new expression
                            | SqlCondOp(op,SqlColumnGet(ti,key,_),(((:? MemberExpression) | (:? MethodCallExpression) | (:? NewExpression)) as meth)) ->                                 
                                paramNames.Add(ti) |> ignore
                                Some(ti,key,op,Some(Expression.Lambda(meth).Compile().DynamicInvoke()))
                            | _ -> None
                                
                        let rec filterExpression (exp:Expression)  =
                            let extendFilter conditions nextFilter = 
                                match exp with
                                | AndAlso(_) -> And(conditions,nextFilter)
                                | OrElse(_) -> Or(conditions,nextFilter)
                                | _ -> failwith ""                                
                            match exp with                            
                            | AndAlsoOrElse(AndAlsoOrElse(_,_) as left, (AndAlsoOrElse(_,_) as right)) as outer ->                                                                
                                extendFilter [] (Some ([filterExpression left; filterExpression right]))
                            | AndAlsoOrElse(AndAlsoOrElse(_,_) as left,Condition(c) as cond)  ->
                                extendFilter [c] (Some ([filterExpression left]))
                            | AndAlsoOrElse(Condition(c) as cond,(AndAlsoOrElse(_,_) as right))  ->
                                extendFilter [c] (Some ([filterExpression right]))
                            | AndAlsoOrElse(Condition(c1) ,Condition(c2)) as outer ->    
                                extendFilter [c1;c2] None                                                                                                   
                            | Condition(cond) -> 
                                Condition.And([cond],None)
                            | _ -> failwith "Unsupported expression. Ensure all server-side objects appear on the left hand side of predicates.  The In and Not In operators only support the inline array syntax."

                        match qual with
                        | Lambda([name],ex) -> 
                            // name here will either be the alias the user entered in the where clause if no joining / select many has happened before this
                            // otherwise, it will be the compiler-generated alias eg _arg2.  this might be the first method called in which case set the 
                            // base entity alias to this name. 
                            let filter = filterExpression ex
                            let sqlExpression = 
                                match source.SqlExpression with
                                | BaseTable(alias,entity) when alias = "" -> 
                                    // special case here as above - this is the first call so replace the top of the tree here with the current base entity alias and the filter                                    
                                    FilterClause(filter,BaseTable(name.Name,entity))
                                | current -> 
                                    // the following case can happen with multiple where clauses when only a single entity is selected
                                    if paramNames.First() = "" || source.TupleIndex.Count = 0 then FilterClause(filter,current) 
                                    else FilterClause(filter,current)

                            let ty = typedefof<SqlQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])                            
                            ty.GetConstructors().[0].Invoke [| source.ConnectionString; source.Provider; sqlExpression; source.TupleIndex; |] :?> IQueryable<_>
                        | _ -> failwith "only support lambdas in a where"

                    | MethodCall(None, (MethodWithName "Join" as meth), 
                                    [ SourceWithQueryData source;
                                      SourceWithQueryData dest
                                      OptionalQuote (Lambda([ParamName sourceAlias],SqlColumnGet(sourceTi,sourceKey,_)))                                       
                                      OptionalQuote (Lambda([ParamName destAlias],SqlColumnGet(destTi,destKey,_)))                                       
                                      OptionalQuote projection ]) ->
                        let (BaseTable(_,destEntity)) = dest.SqlExpression
                        let sqlExpression = 
                            match source.SqlExpression with
                            | BaseTable(alias,entity) when alias = "" -> 
                                // special case here as above - this is the first call so replace the top of the tree here with the current base table alias and the select many                                                                                                                                        
                                let data = { PrimaryKey = destKey; PrimaryTable = destEntity; ForeignKey = sourceKey; ForeignTable = entity; OuterJoin = false; RelDirection = RelationshipDirection.Parents}
                                if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias) 
                                if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias) 
                                SelectMany(sourceAlias,destAlias, data,BaseTable(sourceAlias,entity))
                            | current -> 
                                let sourceAlias = if sourceTi <> "" then Utilities.resolveTuplePropertyName sourceTi source.TupleIndex else sourceAlias
                                if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias) 
                                if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias) 
                                // we don't actually have the "foreign" table name here in a join as that information is "lost" further up the expression tree.
                                // it's ok though because it can always be resolved later after the whole expression tree has been evaluated
                                let data = { PrimaryKey = destKey; PrimaryTable = destEntity; ForeignKey = sourceKey; 
                                             ForeignTable = {Schema="";Name="";Type=""}; 
                                             OuterJoin = false; RelDirection = RelationshipDirection.Parents }                                
                                SelectMany(sourceAlias,destAlias,data,source.SqlExpression)  

                        let ty =
                            match projection with
                                | :? LambdaExpression as meth -> typedefof<SqlQueryable<_>>.MakeGenericType(meth.ReturnType)
                                | _ -> failwith "unsupported projection in join"
                        ty.GetConstructors().[0].Invoke [| source.ConnectionString; source.Provider; sqlExpression; source.TupleIndex; |] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "SelectMany" as meth),                     
                                    [ SourceWithQueryData source; 
                                      OptionalQuote (Lambda([ v1 ], inner )); 
                                      OptionalQuote (Lambda(projectionParams,_) as projection)  ]) ->
                        let ty =
                            match projection with
                                | :? LambdaExpression as meth -> typedefof<SqlQueryable<_>>.MakeGenericType(meth.ReturnType)
                                | _ -> failwith "unsupported projection in select many"      

                        // multiple SelectMany calls in sequence are represented in the same expression tree which must be parsed recursively (and joins too!)
                        let rec processSelectManys toAlias inExp outExp =
                             let (|OptionalOuterJoin|) e =
                                match e with
                                | MethodCall(None, (!!), [inner]) -> (true,inner)
                                | _ -> (false,e)
                             match inExp with                             
                             | MethodCall(None, (MethodWithName "SelectMany"), [ createRelated ; OptionalQuote (Lambda([v1], inner)); OptionalQuote (Lambda(projectionParams,_)) ]) ->
                                let outExp = processSelectManys projectionParams.[0].Name createRelated outExp                                
                                processSelectManys projectionParams.[1].Name inner outExp
                             | MethodCall(None, (MethodWithName "Join"), 
                                [createRelated
                                 Convert(MethodCall(_, (MethodWithName "_CreateEntities"), [String destEntity] ))
                                 OptionalQuote (Lambda([ParamName sourceAlias],SqlColumnGet(sourceTi,sourceKey,_)))                                       
                                 OptionalQuote (Lambda([ParamName destAlias],SqlColumnGet(destTi,destKey,_)))                                       
                                 OptionalQuote (Lambda(projectionParams,_))]) ->
                                // this case happens when the select many also includes one or more joins in the same tree.
                                // in this situation, the first agrument will either be an additional nested join method call,
                                // or finally it will be the call to _CreatedRelated which is handled recursively in the next case
                                let outExp = processSelectManys projectionParams.[0].Name createRelated outExp 
                                let sourceAlias = if sourceTi <> "" then Utilities.resolveTuplePropertyName sourceTi source.TupleIndex else sourceAlias
                                if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias) 
                                if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias) 
                                // we don't actually have the "foreign" table name here in a join as that information is "lost" further up the expression tree.
                                // it's ok though because it can always be resolved later after the whole expression tree has been evaluated
                                let data = { PrimaryKey = destKey; PrimaryTable = Table.FromFullName destEntity; ForeignKey = sourceKey; 
                                                ForeignTable = {Schema="";Name="";Type=""}; 
                                                OuterJoin = false; RelDirection = RelationshipDirection.Parents }                                
                                SelectMany(sourceAlias,destAlias,data,outExp)  
                             | OptionalOuterJoin(outerJoin,MethodCall(None,(MethodWithName "_CreateRelated"), [param; _; String PE; String PK; String FE; String FK; String IE; RelDirection dir;])) ->                   
                                let fromAlias =
                                    match param with
                                    | ParamName x -> x
                                    | PropertyGet(_,p) -> Utilities.resolveTuplePropertyName p.Name source.TupleIndex
                                    | _ -> failwith "unsupported parameter expression in CreatedRelated method call"
                                let data = { PrimaryKey = PK; PrimaryTable = Table.FromFullName PE; ForeignKey = FK; ForeignTable = Table.FromFullName FE; OuterJoin = outerJoin; RelDirection = dir  }
                                let sqlExpression = 
                                    match outExp with
                                    | BaseTable(alias,entity) when alias = "" -> 
                                        // special case here as above - this is the first call so replace the top of the tree here with the current base entity alias and the select many                                        
                                        SelectMany(fromAlias,toAlias,data,BaseTable(alias,entity))                                            
                                    | current -> 
                                        SelectMany(fromAlias,toAlias,data,outExp)  
                                // add new aliases to the tuple index 
                                if source.TupleIndex.Any(fun v -> v = fromAlias) |> not then source.TupleIndex.Add(fromAlias)                                
                                if source.TupleIndex.Any(fun v -> v = toAlias) |> not then  source.TupleIndex.Add(toAlias)
                                sqlExpression
                             | _ -> failwith ""

                        let ex = processSelectManys projectionParams.[1].Name inner source.SqlExpression 
                        ty.GetConstructors().[0].Invoke [| source.ConnectionString; source.Provider; ex; source.TupleIndex;|] :?> IQueryable<_>
                        
                    | MethodCall(None, (MethodWithName "Select" as meth), [ SourceWithQueryData source; OptionalQuote (Lambda([ v1 ], e) as lambda) ]) ->
                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType((lambda :?> LambdaExpression).ReturnType )
                        if v1.Name.StartsWith "_arg" then
                            // this is the projection from a join - ignore 
                            ty.GetConstructors().[0].Invoke [| source.ConnectionString; source.Provider; source.SqlExpression; source.TupleIndex; |] :?> IQueryable<_>
                        else
                            ty.GetConstructors().[0].Invoke [| source.ConnectionString; source.Provider; Projection(lambda,source.SqlExpression); source.TupleIndex;|] :?> IQueryable<_>
                    | _ -> failwith "unrecognised method call"
                    
                member provider.Execute(e:Expression) : obj = failwith "Execute not implemented"
                member provider.Execute<'T>(e:Expression) : 'T = 
                    let (|SqlQueryableParam|_|) = function Constant ((:? SqlQueryable<'T>  as query), _) -> Some query | _ -> None
                    match e with
                    | MethodCall(o, (MethodWithName "Single" as meth), [SqlQueryableParam(query)] ) ->   
                        match query |> Seq.toList with
                        | x::[] -> x
                        | _ -> raise <| InvalidOperationException("Encountered more than one element in the input sequence")
                    | _ -> failwith "Unuspported execution expression" }

type public SqlDataContext (connectionString:string,providerType,resolutionPath) =   
    static let mutable conString = ""
    static let mutable provider = None
    do 
        conString <- connectionString  
        let prov = Common.Utilities.createSqlProvider providerType resolutionPath
        provider <- Some prov
        use con = prov.CreateConnection(connectionString)
        con.Open()
        // create type mappings and also trigger the table info read so the provider has 
        // the minimum base set of data available
        prov.CreateTypeMappings(con)
        prov.GetTables(con) |> ignore
        con.Close()
    static member _Create(connectionString,dbVendor,resolutionPath) =
        SqlDataContext(connectionString,dbVendor,resolutionPath)    
    static member _CreateRelated(inst:SqlEntity,entity,pe,pk,fe,fk,ie,direction) : IQueryable<SqlEntity> =
        if direction = RelationshipDirection.Children then
            QueryImplementation.SqlQueryable<_>(conString,provider.Value,
               FilterClause(
                  Condition.And(["__base__",fk,ConditionOperator.Equal, Some(inst.GetColumn pk)],None), 
                     BaseTable("__base__",Table.FromFullName fe)),ResizeArray<_>()) :> IQueryable<_> 
        else
            QueryImplementation.SqlQueryable<_>(conString,provider.Value,
               FilterClause(
                  Condition.And(["__base__",pk,ConditionOperator.Equal, Some(box<|inst.GetColumn fk)],None), 
                     BaseTable("__base__",Table.FromFullName pe)),ResizeArray<_>()) :> IQueryable<_> 
    static member _CreateEntities(table:string) : IQueryable<SqlEntity> =  
        QueryImplementation.SqlQueryable.Create(Table.FromFullName table,conString,provider.Value) 
    static member _CallSproc(name,parameters,types:DbType array,values:obj array) =
        use con = provider.Value.CreateConnection(conString)
        con.Open()
        use com = provider.Value.CreateCommand(con,name)
        com.CommandType <- CommandType.StoredProcedure
        parameters
        |> Array.iteri(fun i name ->
            let p = provider.Value.CreateCommandParameter(name,values.[i],Some types.[i])
            com.Parameters.Add p |> ignore)
        use reader = com.ExecuteReader()
        let entity = SqlEntity.FromDataReader(name,reader)
        con.Close()
        entity
    static member _GetIndividual(table,id) : SqlEntity =
        use con = provider.Value.CreateConnection(conString)
        con.Open()
        let table = Table.FromFullName table
        // this line is to ensure the columns for the table have been retrieved and therefore
        // its primary key exists in the lookup
        provider.Value.GetColumns (con,table) |> ignore
        let pk = 
            match provider.Value.GetPrimaryKey table with
            | Some v -> v
            | None -> 
               // this fail case should not really be possible unless the runime database is different to the design-time one
               failwithf "Primary key could not be found on object %s. Individuals only supported on objects with a single primary key." table.FullName         
        
        use com = provider.Value.CreateCommand(con,sprintf "SELECT * FROM %s WHERE %s = @id" table.FullName pk)
        //todo: establish pk sql data type
        com.Parameters.Add (provider.Value.CreateCommandParameter("@id",id,None)) |> ignore
        use reader = com.ExecuteReader()
        let entity = List.head <| SqlEntity.FromDataReader(table.FullName,reader)
        con.Close()
        entity
    member x.ConnectionString with get() = conString
        
