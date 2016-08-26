namespace FSharp.Data.Sql.Runtime

open System
open System.Collections
open System.Collections.Generic
open System.Data

open FSharp.Data.Sql
open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Common.Utilities
open FSharp.Data.Sql.QueryExpression
open FSharp.Data.Sql.Schema

// this is publically exposed and used in the runtime
type IWithDataContext =
    abstract DataContext : ISqlDataContext

module internal QueryImplementation =
    open System.Linq
    open System.Linq.Expressions
    open Patterns

    type IWithSqlService =
        abstract DataContext : ISqlDataContext
        abstract SqlExpression : SqlExp
        abstract TupleIndex : string ResizeArray // indexes where in the anonymous object created by the compiler during a select many that each entity alias appears
        abstract Provider : ISqlProvider

    let (|SourceWithQueryData|_|) = function Constant ((:? IWithSqlService as org), _)    -> Some org | _ -> None
    let (|RelDirection|_|)        = function Constant ((:? RelationshipDirection as s),_) -> Some s   | _ -> None

    let executeQuery (dc:ISqlDataContext) (provider:ISqlProvider) sqlExp ti =
       use con = provider.CreateConnection(dc.ConnectionString)
       let (query,parameters,projector,baseTable) = QueryExpressionTransformer.convertExpression sqlExp ti con provider
       let paramsString = parameters |> Seq.fold (fun acc p -> acc + (sprintf "%s - %A; " p.ParameterName p.Value)) ""
       Common.QueryEvents.PublishSqlQuery (sprintf "%s - params %s" query paramsString)
       // todo: make this lazily evaluated? or optionally so. but have to deal with disposing stuff somehow
       use cmd = provider.CreateCommand(con,query)
       for p in parameters do cmd.Parameters.Add p |> ignore
       let columns = provider.GetColumns(con, baseTable)
       if con.State <> ConnectionState.Open then con.Open()
       use reader = cmd.ExecuteReader()
       let results = dc.ReadEntities(baseTable.FullName, columns, reader)
       let results = seq { for e in results -> projector.DynamicInvoke(e) } |> Seq.cache :> System.Collections.IEnumerable
       if (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close() //else get 'COM object that has been separated from its underlying RCW cannot be used.'
       results

    let executeQueryAsync (dc:ISqlDataContext) (provider:ISqlProvider) sqlExp ti =
       async {
           use con = provider.CreateConnection(dc.ConnectionString) :?> System.Data.Common.DbConnection
           let (query,parameters,projector,baseTable) = QueryExpressionTransformer.convertExpression sqlExp ti con provider
           let paramsString = parameters |> Seq.fold (fun acc p -> acc + (sprintf "%s - %A; " p.ParameterName p.Value)) ""
           Common.QueryEvents.PublishSqlQuery (sprintf "%s - params %s" query paramsString)
           // todo: make this lazily evaluated? or optionally so. but have to deal with disposing stuff somehow
           use cmd = provider.CreateCommand(con,query) :?> System.Data.Common.DbCommand
           for p in parameters do cmd.Parameters.Add p |> ignore
           let columns = provider.GetColumns(con, baseTable) // TODO : provider.GetColumnsAsync() ??
           if con.State <> ConnectionState.Open then
                do! con.OpenAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
           use! reader = cmd.ExecuteReaderAsync() |> Async.AwaitTask
           let! results = dc.ReadEntitiesAsync(baseTable.FullName, columns, reader)
           let results = seq { for e in results -> projector.DynamicInvoke(e) } |> Seq.cache :> System.Collections.IEnumerable
           if (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close() //else get 'COM object that has been separated from its underlying RCW cannot be used.'
           return results
       }

    let executeQueryScalar (dc:ISqlDataContext) (provider:ISqlProvider) sqlExp ti =
       use con = provider.CreateConnection(dc.ConnectionString)
       con.Open()
       let (query,parameters,_,_) = QueryExpressionTransformer.convertExpression sqlExp ti con provider
       Common.QueryEvents.PublishSqlQuery (sprintf "%s - params %A" query parameters)
       use cmd = provider.CreateCommand(con,query)
       for p in parameters do cmd.Parameters.Add p |> ignore
       // ignore any generated projection and just expect a single integer back
       if con.State <> ConnectionState.Open then con.Open()
       let result =
        match cmd.ExecuteScalar() with
        | :? string as s when Int32.TryParse s |> fst -> Int32.Parse s |> box
        | :? string as s when Decimal.TryParse s |> fst -> Decimal.Parse s |> box
        | :? string as s when DateTime.TryParse s |> fst -> DateTime.Parse s |> box
        | :? int as i -> i |> box
        | :? int16 as i -> int32 i |> box
        | :? int64 as i -> int32 i |> box  // LINQ says we must return a 32bit int
        | :? float as i -> decimal i |> box
        | :? decimal as i -> decimal i |> box
        | :? double as i -> decimal i |> box
        | x -> if (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close()
               failwithf "Scalar operation returned something other than a numeric value : %s " (x.GetType().ToString())
       if (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close() //else get 'COM object that has been separated from its underlying RCW cannot be used.'
       result

    let executeQueryScalarAsync (dc:ISqlDataContext) (provider:ISqlProvider) sqlExp ti =
       async {
           use con = provider.CreateConnection(dc.ConnectionString) :?> System.Data.Common.DbConnection
           do! con.OpenAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
           let (query,parameters,_,_) = QueryExpressionTransformer.convertExpression sqlExp ti con provider
           Common.QueryEvents.PublishSqlQuery (sprintf "%s - params %A" query parameters)
           use cmd = provider.CreateCommand(con,query) :?> System.Data.Common.DbCommand
           for p in parameters do cmd.Parameters.Add p |> ignore
           // ignore any generated projection and just expect a single integer back
           if con.State <> ConnectionState.Open then
                do! con.OpenAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
           let! executed = cmd.ExecuteScalarAsync() |> Async.AwaitTask
           let result =
            match executed with
            | :? string as s -> Int32.Parse s
            | :? int as i -> i
            | :? int16 as i -> int32 i
            | :? int64 as i -> int32 i  // LINQ says we must return a 32bit int
            | x -> if (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close()
                   failwithf "Count returned something other than a 32 bit integer : %s " (x.GetType().ToString())
           if (provider.GetType() <> typeof<Providers.MSAccessProvider>) then con.Close() //else get 'COM object that has been separated from its underlying RCW cannot be used.'
           return box result
       }

    type SqlQueryable<'T>(dc:ISqlDataContext,provider,sqlQuery,tupleIndex) =
        static member Create(table,conString,provider) =
            SqlQueryable<'T>(conString,provider,BaseTable("",table),ResizeArray<_>()) :> IQueryable<'T>
        interface IQueryable<'T>
        interface IQueryable with
            member __.Provider = SqlQueryProvider.Provider
            member x.Expression =  Expression.Constant(x,typeof<IQueryable<'T>>) :> Expression
            member __.ElementType = typeof<'T>
        interface seq<'T> with
             member __.GetEnumerator() = (Seq.cast<'T> (executeQuery dc provider sqlQuery tupleIndex)).GetEnumerator()
        interface IEnumerable with
             member x.GetEnumerator() = (x :> seq<'T>).GetEnumerator() :> IEnumerator
        interface IWithDataContext with
             member __.DataContext = dc
        interface IWithSqlService with
             member __.DataContext = dc
             member __.SqlExpression = sqlQuery
             member __.TupleIndex = tupleIndex
             member __.Provider = provider
        member __.GetAsyncEnumerator() =
            async {
                let! executeSql = executeQueryAsync dc provider sqlQuery tupleIndex
                return (Seq.cast<'T> (executeSql)).GetEnumerator()
            }

    and SqlOrderedQueryable<'T>(dc:ISqlDataContext,provider,sqlQuery,tupleIndex) =
        static member Create(table,conString,provider) =
            SqlOrderedQueryable<'T>(conString,provider,BaseTable("",table),ResizeArray<_>()) :> IQueryable<'T>
        interface IOrderedQueryable<'T>
        interface IQueryable<'T>
        interface IQueryable with
            member __.Provider = SqlQueryProvider.Provider
            member x.Expression =  Expression.Constant(x,typeof<IOrderedQueryable<'T>>) :> Expression
            member __.ElementType = typeof<'T>
        interface seq<'T> with
             member __.GetEnumerator() = (Seq.cast<'T> (executeQuery dc provider sqlQuery tupleIndex)).GetEnumerator()
        interface IEnumerable with
             member x.GetEnumerator() = (x :> seq<'T>).GetEnumerator() :> IEnumerator
        interface IWithDataContext with
            member __.DataContext = dc
        interface IWithSqlService with
             member __.DataContext = dc
             member __.SqlExpression = sqlQuery
             member __.TupleIndex = tupleIndex
             member __.Provider = provider
        member __.GetAsyncEnumerator() =
            async {
                let! executeSql = executeQueryAsync dc provider sqlQuery tupleIndex
                return (Seq.cast<'T> (executeSql)).GetEnumerator()
            }

    and SqlQueryProvider() =
         static member val Provider =

             let parseWhere (meth:Reflection.MethodInfo) (source:IWithSqlService) (qual:Expression) =
                let paramNames = HashSet<string>()

                let (|Condition|_|) exp =
                    // IMPORTANT : for now it is always assumed that the table column being checked on the server side is on the left hand side of the condition expression.
                    match exp with
                    | SqlSpecialOpArrQueryable(ti,op,key,qry)
                    | SqlSpecialNegativeOpArrQueryable(ti,op,key,qry) ->

                        let svc = (qry :?> IWithSqlService)
                        use con = svc.Provider.CreateConnection(svc.DataContext.ConnectionString)
                        let (query,parameters,projector,baseTable) = QueryExpressionTransformer.convertExpression svc.SqlExpression svc.TupleIndex con svc.Provider

                        let modified = 
                            parameters |> Seq.map(fun p ->
                                p.ParameterName <- p.ParameterName.Replace("@param", "@paramnested")
                                p
                            ) |> Seq.toArray
                        let subquery = 
                            let paramfixed = query.Replace("@param", "@paramnested")
                            match paramfixed.EndsWith(";") with
                            | false -> paramfixed
                            | true -> paramfixed.Substring(0, paramfixed.Length-1)
                        
                        Some(ti,key,op,Some (box (subquery, modified)))
                    | SqlSpecialOpArr(ti,op,key,value)
                    | SqlSpecialNegativeOpArr(ti,op,key,value) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,op,Some (box value))
                    | SqlSpecialOp(ti,op,key,value) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,op,Some value)
                    // if using nullable types
                    | OptionIsSome(SqlColumnGet(ti,key,_)) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,ConditionOperator.NotNull,None)
                    | OptionIsNone(SqlColumnGet(ti,key,_))
                    | SqlCondOp(ConditionOperator.Equal,(SqlColumnGet(ti,key,_)),OptionNone) 
                    | SqlNegativeCondOp(ConditionOperator.Equal,(SqlColumnGet(ti,key,_)),OptionNone) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,ConditionOperator.IsNull,None)
                    | SqlCondOp(ConditionOperator.NotEqual,(SqlColumnGet(ti,key,_)),OptionNone) 
                    | SqlNegativeCondOp(ConditionOperator.NotEqual,(SqlColumnGet(ti,key,_)),OptionNone) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,ConditionOperator.NotNull,None)
                    // matches column to constant with any operator eg c.name = "john", c.age > 42
                    | SqlCondOp(op,(SqlColumnGet(ti,key,_)),OptionalFSharpOptionValue(ConstantOrNullableConstant(c))) 
                    | SqlNegativeCondOp(op,(SqlColumnGet(ti,key,_)),OptionalFSharpOptionValue(ConstantOrNullableConstant(c))) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,op,c)
                    // matches to another property getter, method call or new expression
                    | SqlCondOp(op,SqlColumnGet(ti,key,_),OptionalFSharpOptionValue((((:? MemberExpression) | (:? MethodCallExpression) | (:? NewExpression)) as meth)))
                    | SqlNegativeCondOp(op,SqlColumnGet(ti,key,_),OptionalFSharpOptionValue((((:? MemberExpression) | (:? MethodCallExpression) | (:? NewExpression)) as meth))) ->
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,op,Some(Expression.Lambda(meth).Compile().DynamicInvoke()))
                    | SqlColumnGet(ti,key,ret) when exp.Type.FullName = "System.Boolean" -> 
                        paramNames.Add(ti) |> ignore
                        Some(ti,key,ConditionOperator.Equal, Some(true |> box))
                    | _ -> None

                let rec filterExpression (exp:Expression)  =
                    let exp = ExpressionOptimizer.doReduction exp
                    let extendFilter conditions nextFilter =
                        match exp with
                        | AndAlso(_) -> And(conditions,nextFilter)
                        | OrElse(_) -> Or(conditions,nextFilter)
                        | _ -> failwith ("Filter problem: " + exp.ToString())
                    match exp with
                    | AndAlsoOrElse(AndAlsoOrElse(_,_) as left, (AndAlsoOrElse(_,_) as right)) ->
                        extendFilter [] (Some ([filterExpression left; filterExpression right]))
                    | AndAlsoOrElse(AndAlsoOrElse(_,_) as left,Condition(c))  ->
                        extendFilter [c] (Some ([filterExpression left]))
                    | AndAlsoOrElse(Condition(c),(AndAlsoOrElse(_,_) as right))  ->
                        extendFilter [c] (Some ([filterExpression right]))
                    | AndAlsoOrElse(Condition(c1) ,Condition(c2)) ->
                        extendFilter [c1;c2] None
                    | Condition(cond) ->
                        Condition.And([cond],None)

                    // Support for simple boolean expressions:
                    | AndAlso(Bool(b), x) | AndAlso(x, Bool(b)) when b = true -> filterExpression x
                    | OrElse(Bool(b), x) | OrElse(x, Bool(b)) when b = false -> filterExpression x
                    | Bool(b) when b -> Condition.ConstantTrue
                    | Bool(b) when not(b) -> Condition.ConstantFalse
                    | _ -> failwith ("Unsupported expression. Ensure all server-side objects appear on the left hand side of predicates.  The In and Not In operators only support the inline array syntax. " + exp.ToString())

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
                    ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; sqlExpression; source.TupleIndex; |] :?> IQueryable<_>
                | _ -> failwith "only support lambdas in a where"

             { new System.Linq.IQueryProvider with
                member __.CreateQuery(e:Expression) : IQueryable = failwithf "CreateQuery, e = %A" e
                member __.CreateQuery<'T>(e:Expression) : IQueryable<'T> =
                    Common.QueryEvents.PublishExpression e
                    match e with
                    | MethodCall(None, (MethodWithName "Skip" as meth), [SourceWithQueryData source; Int amount]) ->
                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                        ty.GetConstructors().[0].Invoke [| source.DataContext ; source.Provider; Skip(amount,source.SqlExpression) ; source.TupleIndex; |] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "Take" as meth), [SourceWithQueryData source; Int amount]) ->
                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                        ty.GetConstructors().[0].Invoke [| source.DataContext ; source.Provider; Take(amount,source.SqlExpression) ; source.TupleIndex; |] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "OrderBy" | MethodWithName "OrderByDescending" as meth), [SourceWithQueryData source; OptionalQuote (Lambda([ParamName param], SqlColumnGet(entity,key,_))) ]) ->
                        let alias =
                             match entity with
                             | "" when source.SqlExpression.HasAutoTupled() -> param
                             | "" -> ""
                             | _ -> Utilities.resolveTuplePropertyName entity source.TupleIndex
                        let ascending = meth.Name = "OrderBy"
                        let sqlExpression =
                               match source.SqlExpression with
                               | BaseTable("",entity)  -> OrderBy("",key,ascending,BaseTable(alias,entity))
                               | _ ->  OrderBy(alias,key,ascending,source.SqlExpression)
                        let ty = typedefof<SqlOrderedQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                        let x = ty.GetConstructors().[0].Invoke [| source.DataContext ; source.Provider; sqlExpression; source.TupleIndex; |]
                        x :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "ThenBy" | MethodWithName "ThenByDescending" as meth), [SourceWithQueryData source; OptionalQuote (Lambda([ParamName param], SqlColumnGet(entity,key,_))) ]) ->
                        let alias =
                            match entity with
                            | "" when source.SqlExpression.HasAutoTupled() -> param
                            | "" -> ""
                            | _ -> Utilities.resolveTuplePropertyName entity source.TupleIndex
                        let ty = typedefof<SqlOrderedQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                        let ascending = meth.Name = "ThenBy"
                        match source.SqlExpression with
                        | OrderBy(_) ->
                            let x = ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; OrderBy(alias,key,ascending,source.SqlExpression) ; source.TupleIndex; |]
                            x :?> IQueryable<_>
                        | _ -> failwith (sprintf "'thenBy' operations must come immediately after a 'sortBy' operation in a query")

                    | MethodCall(None, (MethodWithName "Distinct" as meth), [ SourceWithQueryData source ]) ->
                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType(meth.GetGenericArguments().[0])
                        ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; Distinct(source.SqlExpression) ; source.TupleIndex; |] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "Where" as meth), [ SourceWithQueryData source; OptionalQuote qual ]) ->
                        parseWhere meth source qual
                    | MethodCall(None, (MethodWithName "Join"),
                                    [ SourceWithQueryData source;
                                      SourceWithQueryData dest
                                      OptionalQuote (Lambda([ParamName sourceAlias],SqlColumnGet(sourceTi,sourceKey,_)))
                                      OptionalQuote (Lambda([ParamName destAlias],SqlColumnGet(_,destKey,_)))
                                      OptionalQuote projection ]) ->
                        let destEntity =
                            match dest.SqlExpression with
                            | BaseTable(_,destEntity) -> destEntity
                            | _ -> failwithf "Unexpected destination entity expression (%A)." dest.SqlExpression
                        let sqlExpression =
                            match source.SqlExpression with
                            | BaseTable(alias,entity) when alias = "" ->
                                // special case here as above - this is the first call so replace the top of the tree here with the current base table alias and the select many
                                let data = { PrimaryKey = [destKey]; PrimaryTable = destEntity; ForeignKey = [sourceKey]; ForeignTable = entity; OuterJoin = false; RelDirection = RelationshipDirection.Parents}
                                if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                                if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                                SelectMany(sourceAlias,destAlias, data,BaseTable(sourceAlias,entity))
                            | _ ->
                                let sourceAlias = if sourceTi <> "" then Utilities.resolveTuplePropertyName sourceTi source.TupleIndex else sourceAlias
                                if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                                if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                                // we don't actually have the "foreign" table name here in a join as that information is "lost" further up the expression tree.
                                // it's ok though because it can always be resolved later after the whole expression tree has been evaluated
                                let data = { PrimaryKey = [destKey]; PrimaryTable = destEntity; ForeignKey = [sourceKey];
                                             ForeignTable = {Schema="";Name="";Type=""};
                                             OuterJoin = false; RelDirection = RelationshipDirection.Parents }
                                SelectMany(sourceAlias,destAlias,data,source.SqlExpression)

                        let ty =
                            match projection with
                                | :? LambdaExpression as meth -> typedefof<SqlQueryable<_>>.MakeGenericType(meth.ReturnType)
                                | _ -> failwith "unsupported projection in join"
                        ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; sqlExpression; source.TupleIndex; |] :?> IQueryable<_>
                    | MethodCall(None, (MethodWithName "Join"),
                                    [ SourceWithQueryData source;
                                      SourceWithQueryData dest
                                      OptionalQuote (Lambda([ParamName sourceAlias],TupleSqlColumnsGet(multisource)))
                                      OptionalQuote (Lambda([ParamName destAlias],TupleSqlColumnsGet(multidest)))
                                      OptionalQuote projection ]) ->
                        let destEntity =
                            match dest.SqlExpression with
                            | BaseTable(_,destEntity) -> destEntity
                            | _ -> failwithf "Unexpected destination entity expression (%A)." dest.SqlExpression
                        let destKeys = multidest |> List.map(fun(_,dest,_)->dest)
                        let sourceKeys = multisource |> List.map(fun(_,source,_)->source)
                        let sqlExpression =
                            match source.SqlExpression with
                            | BaseTable(alias,entity) when alias = "" ->
                                // special case here as above - this is the first call so replace the top of the tree here with the current base table alias and the select many
                                let data = { PrimaryKey = destKeys; PrimaryTable = destEntity; ForeignKey = sourceKeys; ForeignTable = entity; OuterJoin = false; RelDirection = RelationshipDirection.Parents}
                                if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                                if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                                SelectMany(sourceAlias,destAlias, data,BaseTable(sourceAlias,entity))
                            | _ ->
                                let sourceTi = multisource |> List.tryPick(fun(ti,_,_)->match ti with "" -> None | x -> Some x)
                                let sourceAlias = match sourceTi with None -> sourceAlias | Some x -> Utilities.resolveTuplePropertyName x source.TupleIndex
                                if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                                if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                                // we don't actually have the "foreign" table name here in a join as that information is "lost" further up the expression tree.
                                // it's ok though because it can always be resolved later after the whole expression tree has been evaluated
                                let data = { PrimaryKey = destKeys; PrimaryTable = destEntity; ForeignKey = sourceKeys;
                                             ForeignTable = {Schema="";Name="";Type=""};
                                             OuterJoin = false; RelDirection = RelationshipDirection.Parents }
                                SelectMany(sourceAlias,destAlias,data,source.SqlExpression)

                        let ty =
                            match projection with
                                | :? LambdaExpression as meth -> typedefof<SqlQueryable<_>>.MakeGenericType(meth.ReturnType)
                                | _ -> failwith "unsupported projection in join"
                        ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; sqlExpression; source.TupleIndex; |] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "SelectMany"),
                                    [ SourceWithQueryData source;
                                      OptionalQuote (Lambda([_], inner ));
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
                            | MethodCall(None, (MethodWithName "SelectMany"), [ createRelated ; OptionalQuote (Lambda([_], inner)); OptionalQuote (Lambda(projectionParams,_)) ]) ->
                                let outExp = processSelectManys projectionParams.[0].Name createRelated outExp
                                processSelectManys projectionParams.[1].Name inner outExp
                            | MethodCall(None, (MethodWithName "Join"),
                                                    [createRelated
                                                     ConvertOrTypeAs(MethodCall(Some(Lambda(_,MethodCall(_,MethodWithName "CreateEntities",[String destEntity]))),(MethodWithName "Invoke"),_))
                                                     OptionalQuote (Lambda([ParamName sourceAlias],SqlColumnGet(sourceTi,sourceKey,_)))
                                                     OptionalQuote (Lambda([ParamName destAlias],SqlColumnGet(_,destKey,_)))
                                                     OptionalQuote (Lambda(projectionParams,_))])
                            | MethodCall(None, (MethodWithName "Join"),
                                                    [createRelated
                                                     ConvertOrTypeAs(MethodCall(_, (MethodWithName "CreateEntities"), [String destEntity] ))
                                                     OptionalQuote (Lambda([ParamName sourceAlias],SqlColumnGet(sourceTi,sourceKey,_)))
                                                     OptionalQuote (Lambda([ParamName destAlias],SqlColumnGet(_,destKey,_)))
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
                                let data = { PrimaryKey = [destKey]; PrimaryTable = Table.FromFullName destEntity; ForeignKey = [sourceKey];
                                                ForeignTable = {Schema="";Name="";Type=""};
                                                OuterJoin = false; RelDirection = RelationshipDirection.Parents }
                                SelectMany(sourceAlias,destAlias,data,outExp)
                            | OptionalOuterJoin(outerJoin,MethodCall(Some(_),(MethodWithName "CreateRelated"), [param; _; String PE; String PK; String FE; String FK; RelDirection dir;])) ->
                                let fromAlias =
                                    match param with
                                    | ParamName x -> x
                                    | PropertyGet(_,p) -> Utilities.resolveTuplePropertyName p.Name source.TupleIndex
                                    | _ -> failwith "unsupported parameter expression in CreatedRelated method call"
                                let data = { PrimaryKey = [PK]; PrimaryTable = Table.FromFullName PE; ForeignKey = [FK]; ForeignTable = Table.FromFullName FE; OuterJoin = outerJoin; RelDirection = dir  }
                                let sqlExpression =
                                    match outExp with
                                    | BaseTable(alias,entity) when alias = "" ->
                                        // special case here as above - this is the first call so replace the top of the tree here with the current base entity alias and the select many
                                        SelectMany(fromAlias,toAlias,data,BaseTable(alias,entity))
                                    | _ ->
                                        SelectMany(fromAlias,toAlias,data,outExp)
                                // add new aliases to the tuple index
                                if source.TupleIndex.Any(fun v -> v = fromAlias) |> not then source.TupleIndex.Add(fromAlias)
                                if source.TupleIndex.Any(fun v -> v = toAlias) |> not then  source.TupleIndex.Add(toAlias)
                                sqlExpression
                            | MethodCall(None, (MethodWithName "Join"),
                                                    [createRelated
                                                     ConvertOrTypeAs(MethodCall(Some(Lambda(_,MethodCall(_,MethodWithName "CreateEntities",[String destEntity]))),(MethodWithName "Invoke"),_))
                                                     OptionalQuote (Lambda([ParamName sourceAlias],TupleSqlColumnsGet(multisource)))
                                                     OptionalQuote (Lambda([ParamName destAlias],TupleSqlColumnsGet(multidest)))
                                                     OptionalQuote (Lambda(projectionParams,_))]) ->
                                let outExp = processSelectManys projectionParams.[0].Name createRelated outExp

                                let destKeys = multidest |> List.map(fun (_,destKey,_) -> destKey)
                                let aliashandlesSource =
                                    multisource |> List.map(
                                        fun (sourceTi,sourceKey,_) ->
                                            let sourceAlias = if sourceTi <> "" then Utilities.resolveTuplePropertyName sourceTi source.TupleIndex else sourceAlias
                                            if source.TupleIndex.Any(fun v -> v = sourceAlias) |> not then source.TupleIndex.Add(sourceAlias)
                                            if source.TupleIndex.Any(fun v -> v = destAlias) |> not then source.TupleIndex.Add(destAlias)
                                            sourceAlias, sourceKey
                                        )
                                let sourceAlias = match aliashandlesSource with [] -> sourceAlias | (alias,_)::t -> alias
                                let sourceKeys = aliashandlesSource |> List.map snd

                                let data = { PrimaryKey = destKeys; PrimaryTable = Table.FromFullName destEntity; ForeignKey = sourceKeys;
                                                ForeignTable = {Schema="";Name="";Type=""};
                                                OuterJoin = false; RelDirection = RelationshipDirection.Parents }
                                SelectMany(sourceAlias,destAlias,data,outExp)
                            | _ -> failwith ("Unknown: " + inExp.ToString())

                        let ex = processSelectManys projectionParams.[1].Name inner source.SqlExpression
                        ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; ex; source.TupleIndex;|] :?> IQueryable<_>

                    | MethodCall(None, (MethodWithName "Select"), [ SourceWithQueryData source; OptionalQuote (Lambda([ v1 ], _) as lambda) ]) as whole ->
                        let ty = typedefof<SqlQueryable<_>>.MakeGenericType((lambda :?> LambdaExpression).ReturnType )
                        if v1.Name.StartsWith "_arg" && v1.Type <> typeof<SqlEntity> then
                            // this is the projection from a join - ignore
                            ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; source.SqlExpression; source.TupleIndex; |] :?> IQueryable<_>
                        else
                            ty.GetConstructors().[0].Invoke [| source.DataContext; source.Provider; Projection(whole,source.SqlExpression); source.TupleIndex;|] :?> IQueryable<_>
                    | x -> failwith ("unrecognised method call" + x.ToString())

                member __.Execute(_: Expression) : obj =
                    failwith "Execute not implemented"

                member __.Execute<'T>(e: Expression) : 'T =
                    Common.QueryEvents.PublishExpression e
                    match e with
                    | MethodCall(_, (MethodWithName "First"), [Constant(query, _)]) ->
                        let svc = (query :?> IWithSqlService)
                        executeQuery svc.DataContext svc.Provider (Take(1,(svc.SqlExpression))) svc.TupleIndex
                        |> Seq.cast<'T>
                        |> Seq.head
                    | MethodCall(_, (MethodWithName "FirstOrDefault"), [Constant(query, _)]) ->
                        let svc = (query :?> IWithSqlService)
                        executeQuery svc.DataContext svc.Provider (Take(1, svc.SqlExpression)) svc.TupleIndex
                        |> Seq.cast<'T>
                        |> Seq.tryFind (fun _ -> true)
                        |> Option.fold (fun _ x -> x) Unchecked.defaultof<'T>
                    | MethodCall(_, (MethodWithName "Single"), [Constant(query, _)]) ->
                        match (query :?> seq<_>) |> Seq.toList with
                        | x::[] -> x
                        | [] -> raise <| InvalidOperationException("Encountered zero elements in the input sequence")
                        | _ -> raise <| InvalidOperationException("Encountered more than one element in the input sequence")
                    | MethodCall(_, (MethodWithName "SingleOrDefault"), [Constant(query, _)]) ->
                        match (query :?> seq<_>) |> Seq.toList with
                        | [] -> Unchecked.defaultof<'T>
                        | x::[] -> x
                        | _ -> raise <| InvalidOperationException("Encountered more than one element in the input sequence")
                    | MethodCall(None, (MethodWithName "Count"), [Constant(query, _)]) ->
                        let svc = (query :?> IWithSqlService)
                        let res = executeQueryScalar svc.DataContext svc.Provider (Count(svc.SqlExpression)) svc.TupleIndex 
                        match res with 
                        | :? Decimal -> let r:decimal = res |> unbox
                                        Convert.ToInt32(r) |> box :?> 'T
                        | _ ->  res :?> 'T
                    | MethodCall(None, (MethodWithName "Any" as meth), [ SourceWithQueryData source; OptionalQuote qual ]) ->
                        let limitedSource = 
                            {new IWithSqlService with 
                                member t.DataContext = source.DataContext
                                member t.SqlExpression = Take(1, source.SqlExpression) 
                                member t.Provider = source.Provider
                                member t.TupleIndex = source.TupleIndex }
                        let res = parseWhere meth limitedSource qual
                        res |> Seq.length > 0 |> box :?> 'T
                    | MethodCall(None, (MethodWithName "All" as meth), [ SourceWithQueryData source; OptionalQuote qual ]) ->
                        let negativeCheck = 
                            match qual with
                            | :? LambdaExpression as la -> Expression.Lambda(Expression.Not(la.Body), la.Parameters) :> Expression
                            | _ -> Expression.Not(qual) :> Expression

                        let limitedSource = 
                            {new IWithSqlService with 
                                member t.DataContext = source.DataContext
                                member t.SqlExpression = Take(1, source.SqlExpression) 
                                member t.Provider = source.Provider
                                member t.TupleIndex = source.TupleIndex }
                        
                        let res = parseWhere meth limitedSource negativeCheck
                        res |> Seq.length = 0 |> box :?> 'T
                    | MethodCall(None, (MethodWithName "First" as meth), [ SourceWithQueryData source; OptionalQuote qual ]) ->
                        let limitedSource = 
                            {new IWithSqlService with 
                                member t.DataContext = source.DataContext
                                member t.SqlExpression = Take(1, source.SqlExpression) 
                                member t.Provider = source.Provider
                                member t.TupleIndex = source.TupleIndex }
                        let res = parseWhere meth limitedSource qual
                        res |> Seq.head |> box :?> 'T
                    | MethodCall(None, (MethodWithName "Average" | MethodWithName "Sum" | MethodWithName "Max" | MethodWithName "Min" as meth), [SourceWithQueryData source; 
                             OptionalQuote (Lambda([ParamName param], OptionalConvertOrTypeAs(SqlColumnGet(entity,key,_)))) 
                             ]) ->
                        let alias =
                             match entity with
                             | "" when source.SqlExpression.HasAutoTupled() -> param
                             | "" -> ""
                             | _ -> resolveTuplePropertyName entity source.TupleIndex
                        let sqlExpression =
                               
                               match meth.Name, source.SqlExpression with
                               | "Sum", BaseTable("",entity)  -> AggregateOp(Sum,"",key,BaseTable(alias,entity))
                               | "Sum", _ ->  AggregateOp(Sum,alias,key,source.SqlExpression)
                               | "Max", BaseTable("",entity)  -> AggregateOp(Max,"",key,BaseTable(alias,entity))
                               | "Max", _ ->  AggregateOp(Max,alias,key,source.SqlExpression)
                               | "Min", BaseTable("",entity)  -> AggregateOp(Min,"",key,BaseTable(alias,entity))
                               | "Min", _ ->  AggregateOp(Min,alias,key,source.SqlExpression)
                               | "Average", BaseTable("",entity)  -> AggregateOp(Avg,"",key,BaseTable(alias,entity))
                               | "Average", _ ->  AggregateOp(Avg,alias,key,source.SqlExpression)
                               | _ -> failwithf "Unsupported aggregation `%s` in execution expression `%s`" meth.Name (e.ToString())
                        let res = executeQueryScalar source.DataContext source.Provider sqlExpression source.TupleIndex 
                        match box Unchecked.defaultof<'T>, res with 
                        | :? Int32, :? Int32 -> res :?> 'T
                        | :? Decimal, :? Decimal -> res :?> 'T
                        | :? Int32, :? Decimal -> let r:decimal = res |> unbox
                                                  Convert.ToInt32(r) |> box :?> 'T
                        | :? Decimal, :? Int32 -> decimal(unbox(res)) |> box :?> 'T
                        | _ ->  res :?> 'T
                    | MethodCall(None, (MethodWithName "Contains"), [SourceWithQueryData source; 
                             OptionalQuote(OptionalFSharpOptionValue(ConstantOrNullableConstant(c))) 
                             ]) ->
                             
                        let sqlExpression =
                            match source.SqlExpression with 
                            | Projection(MethodCall(None, _, [SourceWithQueryData source; OptionalQuote (Lambda([ParamName param], SqlColumnGet(entity,key,_))) ]),BaseTable(alias,entity2)) ->
                                Count(Take(1,(FilterClause(Condition.And([alias, key, ConditionOperator.Equal, c],None),source.SqlExpression))))
                            | Projection(MethodCall(None, _, [SourceWithQueryData source; OptionalQuote (Lambda([ParamName param], SqlColumnGet(entity,key,_))) ]), current) ->
                                Count(Take(1,(FilterClause(Condition.And(["", key, ConditionOperator.Equal, c],None),current))))
                            | others ->
                                failwithf "Unsupported execution of contains expression `%s`" (e.ToString())

                        let res = executeQueryScalar source.DataContext source.Provider sqlExpression source.TupleIndex 
                        let boolres =
                            match res with 
                            | :? Int32 as ires -> ires > 0
                            | :? Decimal as mres -> mres > 0m
                            | _ -> Convert.ToInt32(res) > 0
                        boolres |> box :?> 'T                      
                    | MethodCall(_, (MethodWithName "ElementAt"), [SourceWithQueryData source; Int position ]) ->
                        let skips = position - 1
                        executeQuery source.DataContext source.Provider (Take(1,(Skip(skips,source.SqlExpression)))) source.TupleIndex
                        |> Seq.cast<'T>
                        |> Seq.head
                    | e -> failwithf "Unsupported execution expression `%s`" (e.ToString())  }


