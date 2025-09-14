namespace FSharp.Data.Sql.QueryExpression

open System
open System.Reflection
open System.Linq.Expressions
open System.Collections.Generic

open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Patterns
open FSharp.Data.Sql.Schema

module internal QueryExpressionTransformer =
    open FSharp.Data.Sql

    let getSubEntityMi =
        match <@ (Unchecked.defaultof<SqlEntity>).GetSubTable("", "") @> with
        | FSharp.Quotations.Patterns.Call(_,mi,_) -> mi
        | _ -> failwith "never"


    let rec directAggregate (exp:Expression) picker =
        match exp.NodeType, exp with
        | _, OptionalConvertOrTypeAs(SqlColumnGet(entity, op, _)) ->
            picker entity op
        | ExpressionType.Quote, (:? UnaryExpression as ce) 
        | ExpressionType.Convert, (:? UnaryExpression as ce) -> directAggregate ce.Operand picker
        | ExpressionType.MemberAccess, ( :? MemberExpression as me2) -> 
            match me2.Member with 
            | :? PropertyInfo as p when p.Name = "Value" && (Utilities.isOpt me2.Member.DeclaringType) -> directAggregate (me2.Expression) picker
            | _ -> None
        | _ -> None

    let transform (projection:Expression) (tupleIndex:string ResizeArray) (databaseParam:ParameterExpression) (aliasEntityDict:Map<string,Table>) (ultimateChild:(string * Table) option) (replaceParams:Dictionary<ParameterExpression, LambdaExpression>) useCanonicalsOnSelect =
        let (|OperationColumnOnly|_|) = function
            | MethodCall(None, MethodWithName "Select", [Constant(_, t) ;
                OptionalQuote (Lambda([ParamName sourceAlias],(SqlColumnGet(entity,(CanonicalOperation(_) | KeyColumn(_) as coltyp),rtyp) as oper))) as exp]) when 
                    (Type.(=)(t, typeof<System.Linq.IQueryable<SqlEntity>>) || Type.(=)(t, typeof<System.Linq.IOrderedQueryable<SqlEntity>>)) && ((not(Common.Utilities.isGrp databaseParam.Type))) ->
                let resolved = Utilities.resolveTuplePropertyName entity tupleIndex
                let al = if String.IsNullOrEmpty resolved then sourceAlias else resolved
                Some ((al,coltyp,rtyp), exp, oper.Type)
            | _ -> None

        let projectionMap = Dictionary<string,ProjectionParameter ResizeArray>()
        let groupProjectionMap = ResizeArray<alias*SqlColumnType>()

        let (|SingleTable|MultipleTables|) = function
            | MethodCall(None, MethodWithName "Select", [Constant(_, t) ;exp]) when
                    (Type.(=)(t, typeof<System.Linq.IQueryable<SqlEntity>>) || Type.(=)(t, typeof<System.Linq.IOrderedQueryable<SqlEntity>>)) && aliasEntityDict.Count < 2 ->
                SingleTable exp
            | MethodCall(None, MethodWithName "Select", [_ ;exp]) ->
                MultipleTables exp
            | Lambda([ParamName sourceAlias],(SqlColumnGet(entity,ct,rtyp) as oper)) as exp ->
                if not(groupProjectionMap.Contains (entity,ct)) then
                    groupProjectionMap.Add(entity,ct)
                MultipleTables exp
            | Lambda([ParamName sourceAlias],NewExpr(_, [
                        (SqlColumnGet(entity1,ct1,rtyp1) as oper1);
                        (SqlColumnGet(entity2,ct2,rtyp2) as oper2)
                        ])) as exp ->
                if not(groupProjectionMap.Contains (entity1,ct1)) then
                    groupProjectionMap.Add(entity1,ct1)
                if not(groupProjectionMap.Contains (entity2,ct2)) then
                    groupProjectionMap.Add(entity2,ct2)
                MultipleTables exp
            | _ -> failwith ("Unsupported projection type " + projection.NodeType.ToString() + ": " + projection.ToString())

            // 1. only one table was involed so the input is a single parameter
            // 2. the input is a n tuple returned from the query
            // in both cases we need to work out what columns were selected from the different tables,
            //   and if at any point a whole table is selected, that should take precedence over any
            //   previously selected individual columns.
            // in the second case we also need to change any property on the input tuple into calls
            // onto GetSubEntity on the result parameter with the correct alias
        
        let (|SourceTupleGet|_|) (e:Expression) =
            match e with
            | PropertyGet(Some(ParamWithName "tupledArg"), info) when Type.(=)(info.PropertyType, typeof<SqlEntity>) ->
                let alias = Utilities.resolveTuplePropertyName (e :?> MemberExpression).Member.Name tupleIndex
                match aliasEntityDict.TryGetValue alias with
                | true, aliasVal ->
                    Some (alias,aliasVal.FullName, None)
                | false, _ ->
                    if ultimateChild.IsSome then
                        Some (alias, fst(ultimateChild.Value), None)
                    else None
            | MethodCall(Some(PropertyGet(Some(ParamWithName "tupledArg"),info) as getter),
                         (MethodWithName "GetColumn" | MethodWithName "GetColumnOption" | MethodWithName "GetColumnValueOption" as mi) ,
                         [String key]) when Type.(=)(info.PropertyType, typeof<SqlEntity>) ->
                let alias = Utilities.resolveTuplePropertyName (getter :?> MemberExpression).Member.Name tupleIndex
                match aliasEntityDict.TryGetValue alias with
                | true, aliasVal ->
                    Some (alias,aliasVal.FullName, Some(key,mi))
                | false, _ ->
                    if ultimateChild.IsSome then
                        Some (alias,fst(ultimateChild.Value), Some(key,mi))
                    else None
            | eOther when eOther.NodeType.ToString().Contains("Parameter") && (eOther :? ParameterExpression) ->
                let param = eOther :?> ParameterExpression
                if Type.(=)(param.Type, typeof<SqlEntity>) then
                    let alias = Utilities.resolveTuplePropertyName (param.Name) tupleIndex
                    match aliasEntityDict.TryGetValue alias with
                    | true, aliasVal ->
                        Some (alias,aliasVal.FullName, None)
                    | false, _ ->
                        if ultimateChild.IsSome then
                            Some (fst(ultimateChild.Value),snd(ultimateChild.Value).FullName, None)
                        else None
                else None
            | PropertyGet(Some(PropertyGet(Some(ParamWithName "tupledArg"), nestedTuple)), info) 
                    when nestedTuple.Name = "Item8" && Type.(=)(info.PropertyType, typeof<SqlEntity>) && nestedTuple.PropertyType.Name.StartsWith("AnonymousObject") ->
                // After 7 members tuple starts to nest Item:s.
                let foundMember, name = Int32.TryParse((e :?> MemberExpression).Member.Name.Replace("Item", ""))
                if not foundMember then None
                else
                let alias = Utilities.resolveTuplePropertyName ("Item" + (string (7 + name))) tupleIndex
                match aliasEntityDict.TryGetValue alias with
                | true, aliasVal ->
                    Some (alias,aliasVal.FullName, None)
                | false, _ -> None
            | MethodCall(Some(PropertyGet(Some(PropertyGet(Some(ParamWithName "tupledArg"), nestedTuple)), info) as getter),
                         (MethodWithName "GetColumn" | MethodWithName "GetColumnOption" | MethodWithName "GetColumnValueOption" as mi) ,
                         [String key]) when nestedTuple.Name = "Item8" && Type.(=)(info.PropertyType, typeof<SqlEntity>) && nestedTuple.PropertyType.Name.StartsWith("AnonymousObject") ->
                let foundMember, name = Int32.TryParse((getter :?> MemberExpression).Member.Name.Replace("Item", ""))
                if not foundMember then None
                else
                let alias = Utilities.resolveTuplePropertyName ("Item" + (string (7 + name)) ) tupleIndex
                match aliasEntityDict.TryGetValue alias with
                | true, aliasVal ->
                    Some (alias,aliasVal.FullName, Some(key,mi))
                | false, _ -> None
            | _ -> None

        let (|GroupByAggregate|_|) (e:Expression) =
            // On group-by aggregates we support currently only direct calls like .Count() or .Sum()
            // and direct parameter calls like .Sum(fun entity -> entity.UnitPrice)
            match e.NodeType, e with
            | ExpressionType.Call, (:? MethodCallExpression as me) ->

                let checkInnerdSelect (m:MethodCallExpression) =
                    if m.Arguments.Count > 0 && (m.Arguments.[0].Type.IsGenericType) then
                        match m.Arguments.[0].NodeType, m.Arguments.[0] with
                        | ExpressionType.Call, (:? MethodCallExpression as me2) ->
                            if me2.Arguments.Count > 0 && (me2.Arguments.[0].Type.IsGenericType) &&
                                    (Common.Utilities.isGrp me2.Arguments.[0].Type || me2.Arguments.[0].Type.Name.StartsWith("Grouping")) && me2.Method.Name = "Select"
                                then Some me2
                                else None
                        | _ -> None
                    else None

                let hasInnerDistinct =
                    if me.Arguments.Count > 0 && (me.Arguments.[0].Type.IsGenericType) then
                        match me.Arguments.[0].NodeType, me.Arguments.[0] with
                        | ExpressionType.Call, (:? MethodCallExpression as me2) ->
                            if me2.Arguments.Count > 0 && (me2.Arguments.[0].Type.IsGenericType) &&
                                    (Common.Utilities.isGrp me2.Arguments.[0].Type || me2.Arguments.[0].Type.Name.StartsWith("Grouping") || (checkInnerdSelect me2).IsSome) && me2.Method.Name = "Distinct"
                                then Some me2
                                else None
                        | _ -> None
                    else None

                let hasInnerdSelect =
                    match hasInnerDistinct with
                    | Some innerDist -> checkInnerdSelect innerDist
                    | None -> checkInnerdSelect me

                let isGrouping = 
                    me.Arguments.Count > 0 && (me.Arguments.[0].Type.IsGenericType) &&
                    (Common.Utilities.isGrp me.Arguments.[0].Type || me.Arguments.[0].Type.Name.StartsWith("Grouping")) || hasInnerdSelect.IsSome

                let isNumType (ty:Type) =
                    decimalTypes  |> Seq.exists(fun t -> Type.(=)(t, ty)) || integerTypes |> Seq.exists(fun t -> Type.(=)(t, ty))

                let op =
                    if me.Arguments.Count = 1 && (me.Arguments.[0].NodeType = ExpressionType.Parameter ||
                                                    (me.Arguments.[0].NodeType = ExpressionType.New && me.Arguments.[0].Type.Name.StartsWith("Grouping")) || 
                                                    (me.Arguments.[0].NodeType = ExpressionType.MemberAccess && (Common.Utilities.isGrp me.Arguments.[0].Type)) ||
                                                    (hasInnerDistinct.IsSome && (
                                                         hasInnerDistinct.Value.Arguments.[0].NodeType = ExpressionType.Parameter ||
                                                        (hasInnerDistinct.Value.Arguments.[0].NodeType = ExpressionType.New && hasInnerDistinct.Value.Arguments.[0].Type.Name.StartsWith("Grouping")) || 
                                                        (hasInnerDistinct.Value.Arguments.[0].NodeType = ExpressionType.MemberAccess && (Common.Utilities.isGrp hasInnerDistinct.Value.Arguments.[0].Type))
                                                    )
                                                 )) then
                        match me.Method.Name with
                        | "Count" when hasInnerDistinct.IsSome -> Some (CountDistOp "", None)
                        | "Count" -> Some (CountOp "", None)
                        | "Sum" when isNumType me.Type -> Some (SumOp "", None)
                        | "Avg" | "Average" when (isNumType me.Type) -> Some (AvgOp "", None)
                        | "Min" when isNumType me.Type -> Some (MinOp "", None)
                        | "Max" when isNumType me.Type -> Some (MaxOp "", None)
                        | "StdDev" | "StDev" | "StandardDeviation" when isNumType me.Type -> Some (StdDevOp "", None)
                        | "Variance" when isNumType me.Type -> Some (VarianceOp "", None)
                        | _ -> None

                    elif me.Arguments.Count = 1 && me.Arguments.[0].NodeType = ExpressionType.Call && [|"Count"; "Sum"; "Avg"; "Min"; "Max"; "StdDev"; "Variance"|] |> Array.contains me.Method.Name then
                        let firstArg =
                            match hasInnerDistinct with
                            | Some innerDist -> innerDist.Arguments.[0].NodeType, innerDist.Arguments.[0]
                            | None -> me.Arguments.[0].NodeType, me.Arguments.[0]
                        match firstArg with
                        | ExpressionType.Call, (:? MethodCallExpression as me2) when me2.Arguments.Count = 2 && (me2.Arguments.[0].Type.IsGenericType) &&
                                    (Common.Utilities.isGrp me2.Arguments.[0].Type || me2.Arguments.[0].Type.Name.StartsWith("Grouping")) && me2.Method.Name = "Select" ->

                            match me2.Arguments.[1] with
                            | :? LambdaExpression as la ->

                                let getOp al op =
                                    let key = Utilities.getBaseColumnName op
                                    let ops =
                                        match op with
                                        | CanonicalOperation _ -> Some (al,op)
                                        | KeyColumn _
                                        | GroupColumn _ -> None

                                    match me.Method.Name with
                                    | "Count" when hasInnerDistinct.IsSome -> Some (CountDistOp key, ops)
                                    | "Count" -> Some (CountOp key, ops)
                                    | "Sum" -> Some (SumOp key, ops)
                                    | "Avg" | "Average" -> Some (AvgOp key, ops)
                                    | "Min" -> Some (MinOp key, ops)
                                    | "Max" -> Some (MaxOp key, ops)
                                    | "StdDev" | "StDev" | "StandardDeviation" -> Some (StdDevOp key, ops)
                                    | "Variance" -> Some (VarianceOp key, ops)
                                    | _ -> None

                                directAggregate la.Body getOp
                            | _ -> None
                        | _ -> None

                    elif me.Arguments.Count = 2 then
                        match me.Arguments.[1] with
                        | :? LambdaExpression as la ->

                            let getOp al op =
                                let key = Utilities.getBaseColumnName op
                                match me.Method.Name with
                                | "Count" when hasInnerDistinct.IsSome -> Some (CountDistOp key, Some (al,op))
                                | "Count" -> Some (CountOp key, Some (al,op))
                                | "Sum" -> Some (SumOp key, Some (al,op))
                                | "Avg" | "Average" -> Some (AvgOp key, Some (al,op))
                                | "Min" -> Some (MinOp key, Some (al,op))
                                | "Max" -> Some (MaxOp key, Some (al,op))
                                | "StdDev" | "StDev" | "StandardDeviation" -> Some (StdDevOp key, Some (al,op))
                                | "Variance" -> Some (VarianceOp key, Some (al,op))
                                | _ -> None

                            directAggregate la.Body getOp
                        | _ -> None
                    else None

                match isGrouping, op with
                | true, Some (o, calcs) ->
                    let methodname =
                        if hasInnerDistinct.IsSome then "Aggregate"+me.Method.Name+"Distinct"
                        else "Aggregate"+me.Method.Name
                    
                    let v = match o with 
                            | CountOp x | SumOp x | AvgOp x | MinOp x | MaxOp x | StdDevOp x | VarianceOp x | CountDistOp x
                            | KeyOp x -> x
                    //Count 1 is over all the items
                    let vf = if v = "" then None else Some v
                    let paramArg =
                        match hasInnerdSelect with
                        | Some selParam -> selParam.Arguments.[0]
                        | None -> me.Arguments.[0]
                        
                    let ty =
                        let retType =
                            if paramArg.Type.GetGenericArguments().Length > 1 then
                                paramArg.Type.GetGenericArguments().[1]
                            else typeof<SqlEntity>
                        typedefof<GroupResultItems<_,_>>.MakeGenericType(paramArg.Type.GetGenericArguments().[0], retType)
                    let aggregateColumn = Expression.Constant(vf, typeof<Option<string>>) :> Expression
                    let meth = ty.GetMethod(methodname)
                    let generic = meth.MakeGenericMethod(me.Method.ReturnType);
                    let replacementExpr =
                        Expression.Call(Expression.Convert(paramArg, ty), generic, aggregateColumn)
                    let res =
                        match calcs with
                        | None -> Some (("",GroupColumn(o,SqlColumnType.KeyColumn(v))), replacementExpr)
                        | Some (al,calculation) -> Some ((al,GroupColumn(o,calculation)), replacementExpr)
                    if res.IsSome && groupProjectionMap.Contains(fst(res.Value)) then None
                    else res
                | _ -> None
            | _ -> None


        let (|OperationItem|_|) e = 
            if not(useCanonicalsOnSelect) then None
            else
            match e with
            | _, (SqlColumnGet(alias,(CanonicalOperation(_,c1) as coltyp),ret) as exp) when ((not(Common.Utilities.isGrp databaseParam.Type))) -> 
                // Ok, this is an operation but not a plain column...
                let foundAlias = 
                    if aliasEntityDict.ContainsKey(alias) then alias
                    elif alias.StartsWith "Item" then
                        let al = Utilities.resolveTuplePropertyName alias tupleIndex
                        if aliasEntityDict.ContainsKey(al) then al
                        else alias
                    elif alias="" && ultimateChild.IsSome then fst ultimateChild.Value
                    else alias

                let name = $"op{abs(coltyp.GetHashCode())}"
                let meth = 
                    if Common.Utilities.isCOpt exp.Type then
                        typeof<IColumnHolder>.GetMethod("GetColumnOption").MakeGenericMethod([|exp.Type.GetGenericArguments().[0]|])
                    elif Common.Utilities.isVOpt exp.Type then
                        typeof<IColumnHolder>.GetMethod("GetColumnValueOption").MakeGenericMethod([|exp.Type.GetGenericArguments().[0]|])
                    else typeof<IColumnHolder>.GetMethod("GetColumn").MakeGenericMethod([|exp.Type|])

                let projection = Expression.Call(databaseParam,meth,Expression.Constant(name)) 

                match projectionMap.TryGetValue foundAlias with
                | true, values when values.Count > 0 -> 
                    match c1 with
                    | KeyColumn x when values.Contains(EntityColumn x) -> None //We have this column already
                    | _ ->
                        values.Add(OperationColumn(name, coltyp))
                        Some projection
                | false, _ -> 
                    projectionMap.Add(foundAlias, ResizeArray<_>(seq{yield OperationColumn(name, coltyp)}))
                    Some projection           
                | _ -> 
                    // This table is alredy fetched as all columns
                    None
            | _ -> None

        let (|ProjectionItem|_|) = function
            | _, SourceTupleGet(alias,name,None) ->
                // at any point if we see a property getter where the input is "tupledArg" this
                // needs to be replaced with a call to GetSubEntity using the result as an input
                match projectionMap.TryGetValue alias with
                | true, values -> values.Clear()
                | false, _ -> projectionMap.Add(alias, ResizeArray<_>())
                Some (Expression.Call(databaseParam,getSubEntityMi,Expression.Constant(alias),Expression.Constant(name)))
            | _, SourceTupleGet(alias,name,Some(key,mi)) ->
                match projectionMap.TryGetValue alias with
                | true, values when values.Count > 0 -> values.Add(EntityColumn(key))
                | false, _ -> projectionMap.Add(alias, ResizeArray<_>(seq{yield EntityColumn(key)}))
                | _ -> ()
                Some
                    (match Common.Utilities.isGrp databaseParam.Type with
                     | false ->
                        (Expression.Call(
                            Expression.Call(databaseParam,getSubEntityMi,Expression.Constant(alias),Expression.Constant(name)),
                                mi,Expression.Constant(key)))
                     | true ->
                        (Expression.Call(
                            Expression.Call(Expression.Parameter(typeof<SqlEntity>,alias),getSubEntityMi,Expression.Constant(alias),Expression.Constant(name)),
                                mi,Expression.Constant(key))))

            | ExpressionType.Call, MethodCall(Some(ParamName pname as p),(MethodWithName "GetColumn" | MethodWithName "GetColumnOption" | MethodWithName "GetColumnValueOption" as mi),[String key]) 
                    when Type.(=)(p.Type, typeof<SqlEntity>) ->

                let foundAlias = 
                    if aliasEntityDict.ContainsKey(pname) then ValueSome pname
                    elif pname.StartsWith "Item" then
                        let al = Utilities.resolveTuplePropertyName pname tupleIndex
                        if aliasEntityDict.ContainsKey(al) then ValueSome al
                        elif ultimateChild.IsSome then ValueSome (fst ultimateChild.Value)
                        else ValueNone
                    else
                    let prevParamKey = replaceParams.Keys |> Seq.toList |> List.tryFind(fun p -> p.Name = pname)

                    let ultimateFallback = if ultimateChild.IsSome then ValueSome (fst ultimateChild.Value) else ValueNone
                    match prevParamKey |> Option.map replaceParams.TryGetValue with
                    | Some (true, par) ->
                        match par.Body.NodeType, par.Body with
                        | ExpressionType.Call, (:? MethodCallExpression as ce) when (ce.Method.Name = "GetSubTable" && (not(isNull ce.Object)) && ce.Object :? ParameterExpression && ce.Arguments.Count = 2) ->
                            match ce.Arguments.[0] with
                            | :? ConstantExpression as c when aliasEntityDict.ContainsKey (c.Value.ToString()) -> ValueSome (c.Value.ToString())
                            | _ -> ultimateFallback
                        | _ -> ultimateFallback
                    | _ -> ultimateFallback

                match foundAlias with
                | ValueSome alias ->
                    match projectionMap.TryGetValue alias with
                    | true, values when values.Count > 0 -> values.Add(EntityColumn(key))
                    | false, _ -> projectionMap.Add(alias, ResizeArray<_>(seq{yield EntityColumn(key)}))
                    | _ -> ()
                    Some
                        (match Common.Utilities.isGrp databaseParam.Type with
                         | false -> Expression.Call(databaseParam,mi,Expression.Constant(key))
                         | true -> Expression.Call(Expression.Parameter(typeof<SqlEntity>,alias),mi,Expression.Constant(key)))
                | ValueNone -> None
            | _ -> None


        // this is not tail recursive but it shouldn't matter in practice ....
        let rec transform  (en:String voption) (e:Expression): Expression =
            let e = ExpressionOptimizer.doReduction e
            if isNull e then null else
            match e.NodeType, e with
            | OperationItem me -> upcast me
            | ProjectionItem me -> upcast me
            | ExpressionType.Negate,             (:? UnaryExpression as e)       -> upcast Expression.Negate(transform en e.Operand,e.Method)
            | ExpressionType.NegateChecked,      (:? UnaryExpression as e)       -> upcast Expression.NegateChecked(transform en e.Operand,e.Method)
            | ExpressionType.Not,                (:? UnaryExpression as e)       -> upcast Expression.Not(transform en e.Operand,e.Method)
            | ExpressionType.Convert,            (:? UnaryExpression as e)       -> upcast Expression.Convert(transform en e.Operand,e.Type)
            | ExpressionType.ConvertChecked,     (:? UnaryExpression as e)       -> upcast Expression.ConvertChecked(transform en e.Operand,e.Type)
            | ExpressionType.ArrayLength,        (:? UnaryExpression as e)       -> upcast Expression.ArrayLength(transform en e.Operand)
            | ExpressionType.Quote,              (:? UnaryExpression as e)       -> upcast Expression.Quote(transform en e.Operand)
            | ExpressionType.TypeAs,             (:? UnaryExpression as e)       -> upcast Expression.TypeAs(transform en e.Operand,e.Type)
            | ExpressionType.Add,                (:? BinaryExpression as e)  when Type.(=) (e.Left.Type, typeof<string>) && Type.(=)(e.Right.Type, typeof<string>) -> 
                                                                             // http://stackoverflow.com/questions/7027384/the-binary-operator-add-is-not-defined-for-the-types-system-string-and-syste
                                                                             let concatMethod = typeof<string>.GetMethod("Concat", [| typeof<string>; typeof<string> |]); 
                                                                             upcast Expression.Add(transform en e.Left, transform en e.Right, concatMethod)
            | ExpressionType.Add,                (:? BinaryExpression as e)      -> upcast Expression.Add(transform en e.Left, transform en e.Right)
            | ExpressionType.AddChecked,         (:? BinaryExpression as e)      -> upcast Expression.AddChecked(transform en e.Left, transform en e.Right)
            | ExpressionType.Subtract,           (:? BinaryExpression as e)      -> upcast Expression.Subtract(transform en e.Left, transform en e.Right)
            | ExpressionType.SubtractChecked,    (:? BinaryExpression as e)      -> upcast Expression.SubtractChecked(transform en e.Left, transform en e.Right)
            | ExpressionType.Multiply,           (:? BinaryExpression as e)      -> upcast Expression.Multiply(transform en e.Left, transform en e.Right)
            | ExpressionType.MultiplyChecked,    (:? BinaryExpression as e)      -> upcast Expression.MultiplyChecked(transform en e.Left, transform en e.Right)
            | ExpressionType.Divide,             (:? BinaryExpression as e)      -> upcast Expression.Divide(transform en e.Left, transform en e.Right)
            | ExpressionType.Modulo,             (:? BinaryExpression as e)      -> upcast Expression.Modulo(transform en e.Left, transform en e.Right)
            | ExpressionType.And,                (:? BinaryExpression as e)      -> upcast Expression.And(transform en e.Left, transform en e.Right)
            | ExpressionType.AndAlso,            (:? BinaryExpression as e)      -> upcast Expression.AndAlso(transform en e.Left, transform en e.Right)
            | ExpressionType.Or,                 (:? BinaryExpression as e)      -> upcast Expression.Or(transform en e.Left, transform en e.Right)
            | ExpressionType.OrElse,             (:? BinaryExpression as e)      -> upcast Expression.OrElse(transform en e.Left, transform en e.Right)
            | ExpressionType.LessThan,           (:? BinaryExpression as e)      -> upcast Expression.LessThan(transform en e.Left, transform en e.Right)
            | ExpressionType.LessThanOrEqual,    (:? BinaryExpression as e)      -> upcast Expression.LessThanOrEqual(transform en e.Left, transform en e.Right)
            | ExpressionType.GreaterThan,        (:? BinaryExpression as e)      -> upcast Expression.GreaterThan(transform en e.Left, transform en e.Right)
            | ExpressionType.GreaterThanOrEqual, (:? BinaryExpression as e)      -> upcast Expression.GreaterThanOrEqual(transform en e.Left, transform en e.Right)
            | ExpressionType.Equal,              (:? BinaryExpression as e)      -> upcast Expression.Equal(transform en e.Left, transform en e.Right)
            | ExpressionType.NotEqual,           (:? BinaryExpression as e)      -> upcast Expression.NotEqual(transform en e.Left, transform en e.Right)
            | ExpressionType.Coalesce,           (:? BinaryExpression as e)   when not (isNull e.Conversion) -> 
                                                                                    let left = transform en e.Left
                                                                                    let right = transform en e.Right
                                                                                    let conv = transform en e.Conversion
                                                                                    upcast Expression.Coalesce(left, right, conv :?> LambdaExpression)
            | ExpressionType.Coalesce,           (:? BinaryExpression as e)      -> upcast Expression.Coalesce(transform en e.Left, transform en e.Right)
            | ExpressionType.ArrayIndex,         (:? BinaryExpression as e)      -> upcast Expression.ArrayIndex(transform en e.Left, transform en e.Right)
            | ExpressionType.RightShift,         (:? BinaryExpression as e)      -> upcast Expression.RightShift(transform en e.Left, transform en e.Right)
            | ExpressionType.LeftShift,          (:? BinaryExpression as e)      -> upcast Expression.LeftShift(transform en e.Left, transform en e.Right)
            | ExpressionType.ExclusiveOr,        (:? BinaryExpression as e)      -> upcast Expression.ExclusiveOr(transform en e.Left, transform en e.Right)
            | ExpressionType.TypeIs,             (:? TypeBinaryExpression as e)  -> upcast Expression.TypeIs(transform en e.Expression, e.Type)
            | ExpressionType.Conditional,        (:? ConditionalExpression as e) -> let testExp = transform en e.Test
                                                                                    match testExp with // For now, only direct booleans conditions are optimized to select query:
                                                                                    | :? ConstantExpression as c when c.Value = box(true) -> transform en e.IfTrue
                                                                                    | :? ConstantExpression as c when c.Value = box(false) -> transform en e.IfFalse
                                                                                    | _ -> upcast Expression.Condition(testExp, transform en e.IfTrue, transform en e.IfFalse)
            | ExpressionType.Constant,           (:? ConstantExpression as e)  when Common.Utilities.isVOpt e.Type ->
                                                                                    // https://github.com/dotnet/fsharp/issues/13370
                                                                                    upcast Expression.Constant(e.Value, typeof<obj>)
            | ExpressionType.Constant,           (:? ConstantExpression as e)    -> upcast e
            | ExpressionType.Parameter,          (:? ParameterExpression as e)   -> match en with //Todo:ValueOption upcast here too
                                                                                    | ValueSome(en) when en = e.Name && (isNull replaceParams || not(replaceParams.ContainsKey(e))) ->
                                                                                         match projectionMap.TryGetValue en with
                                                                                         | true, values -> values.Clear()
                                                                                         | false, _ -> projectionMap.Add(en, ResizeArray<_>())
                                                                                         upcast databaseParam
                                                                                    | _ ->
                                                                                        if (not (isNull replaceParams)) then
                                                                                            match replaceParams.TryGetValue e with
                                                                                            | true, re -> re.Body
                                                                                            | false, _ -> upcast e
                                                                                        else
                                                                                            upcast e
            | ExpressionType.MemberAccess,       (:? MemberExpression as e)      -> let memb = Expression.MakeMemberAccess(transform en e.Expression, e.Member)
                                                                                    // If we have merged new lambdas, just check the combination of anonymous objects
                                                                                    if replaceParams.Count>0 then
                                                                                        ExpressionOptimizer.Methods.``remove AnonymousType`` memb
                                                                                    else upcast memb
            | ExpressionType.Call,               (:? MethodCallExpression as e)  -> let transformed = Expression.Call( (if isNull e.Object then null else transform en e.Object), e.Method, e.Arguments |> Seq.map(fun a -> transform en a))
                                                                                    match transformed with
                                                                                    | GroupByAggregate(param, callreplace) -> 
                                                                                        if not(groupProjectionMap.Contains param) then
                                                                                            groupProjectionMap.Add(param)
                                                                                        upcast callreplace
                                                                                    | _ -> 
                                                                                        upcast transformed
            | ExpressionType.Lambda,             (:? LambdaExpression as e)      -> let exType = e.GetType()
                                                                                    if  exType.IsGenericType
                                                                                        && Type.(=)(exType.GetGenericTypeDefinition(), typeof<Expression<obj>>.GetGenericTypeDefinition())
                                                                                        && exType.GenericTypeArguments.[0].IsSubclassOf typeof<Delegate> then
                                                                                        upcast Expression.Lambda(e.GetType().GenericTypeArguments.[0],transform en e.Body, e.Parameters)
                                                                                    else
                                                                                        upcast Expression.Lambda(transform en e.Body, e.Parameters)
            | ExpressionType.New,                (:? NewExpression as e)         -> if isNull e.Members then
                                                                                      upcast Expression.New(e.Constructor, e.Arguments |> Seq.map(fun a -> transform en a))
                                                                                    else
                                                                                      upcast Expression.New(e.Constructor, e.Arguments |> Seq.map(fun a -> transform en a), e.Members)
            | ExpressionType.NewArrayInit,       (:? NewArrayExpression as e)    -> upcast Expression.NewArrayInit(e.Type.GetElementType(), e.Expressions |> Seq.map(fun e -> transform en e))
            | ExpressionType.NewArrayBounds,     (:? NewArrayExpression as e)    -> upcast Expression.NewArrayBounds(e.Type.GetElementType(), e.Expressions |> Seq.map(fun e -> transform en e))
            | ExpressionType.Invoke,             (:? InvocationExpression as e)  -> upcast Expression.Invoke(transform en e.Expression, e.Arguments |> Seq.map(fun a -> transform en a))
            | ExpressionType.MemberInit,         (:? MemberInitExpression as e)  -> upcast Expression.MemberInit( (transform en e.NewExpression) :?> NewExpression , e.Bindings)
            | ExpressionType.ListInit,           (:? ListInitExpression as e)    -> upcast Expression.ListInit( (transform en e.NewExpression) :?> NewExpression, e.Initializers)
            | _ -> failwith ("encountered unknown LINQ expression: " + e.NodeType.ToString() + " " + e.ToString())

        let newProjection =
            let proj = 
                if useCanonicalsOnSelect then
                    match projection with
                    | OperationColumnOnly((al,coltyp,rtyp), OptionalQuote(lambda), opType) ->
                        projectionMap.Add(al, ResizeArray<_>(seq{yield OperationColumn("result", coltyp)}))
                        let meth = 
                            if Utilities.isCOpt opType then
                                typeof<IColumnHolder>.GetMethod("GetColumnOption").MakeGenericMethod([|opType.GetGenericArguments().[0]|])
                            elif Utilities.isVOpt opType then
                                typeof<IColumnHolder>.GetMethod("GetColumnValueOption").MakeGenericMethod([|opType.GetGenericArguments().[0]|])
                            else typeof<IColumnHolder>.GetMethod("GetColumn").MakeGenericMethod([|opType|])
                        Some meth
                    | _ -> None
                else None
            match proj with
            | Some meth -> Expression.Lambda(Expression.Call(databaseParam,meth,Expression.Constant("result")),[databaseParam]) :> Expression
            | None ->
                match projection with
                | SingleTable(OptionalQuote(Lambda([ParamName _],ParamName x))) ->
                    projectionMap.Add(x,ResizeArray<_>())
                    Expression.Lambda(databaseParam,[databaseParam]) :> Expression
                | SingleTable(OptionalQuote(Lambda([ParamName x], (NewExpr(ci, args ) )))) ->
                    Expression.Lambda(Expression.New(ci, (List.map (transform (ValueSome x)) args)),[databaseParam]) :> Expression
                | SingleTable(OptionalQuote(lambda))
                | MultipleTables(OptionalQuote(lambda)) -> transform ValueNone lambda

        newProjection, projectionMap, groupProjectionMap

    let convertExpression exp (entityIndex:string ResizeArray) con (provider:ISqlProvider) isDeleteScript useCanonicalsOnSelect =
        // first convert the abstract query tree into a more useful format
        let legaliseName (alias:alias) =
                if alias.StartsWith("_") then alias.TrimStart([|'_'|]) else alias

        let entityIndex = ResizeArray<_>(entityIndex |> Seq.map (legaliseName))

        let sqlQuery = SqlQuery.ofSqlExp(exp,entityIndex)
        let groupgin = ResizeArray<_>()

         // note : the baseAlias here will always be "" when no criteria has been applied, because the LINQ tree never needed to refer to it
        let baseAlias,baseTable =
            match sqlQuery.UltimateChild with
            | Some(baseAlias,baseTable) when baseAlias = ""-> baseTable.Name,baseTable
            | Some(baseAlias,baseTable) -> baseAlias,baseTable
            | _ -> failwith ("Unknown sqlQuery.UltimateChild: " + sqlQuery.UltimateChild.ToString())

        let (projectionDelegate,projectionColumns) =
            match sqlQuery.Projection with
            | [] ->

                // this case happens when there are only where clauses with a single table and a projection containing just the table's entire rows. example:
                // for x in dc.john
                // where x.y = 1
                // select x
                // this does not create a call to .select() after .where(), therefore in this case we must provide our own projection that simply selects a whole row
                let initDbParam = 
                    match sqlQuery.Grouping.Length > 0, sqlQuery.Links.Length with
                    | true, 0 -> Expression.Parameter(typeof<System.Linq.IGrouping<_,SqlEntity>>,"result")
                    | true, 1 -> Expression.Parameter(typeof<System.Linq.IGrouping<_,Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<SqlEntity,SqlEntity>>>,"result")
                    | true, 2 -> Expression.Parameter(typeof<System.Linq.IGrouping<_,Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<SqlEntity,SqlEntity,SqlEntity>>>,"result")
                    | true, 3 -> Expression.Parameter(typeof<System.Linq.IGrouping<_,Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<SqlEntity,SqlEntity,SqlEntity,SqlEntity>>>,"result")
                    | _ -> Expression.Parameter(typeof<SqlEntity>,"result")
                let pmap = Dictionary<string,ProjectionParameter ResizeArray>()
                pmap.Add(baseAlias, ResizeArray<_>())
                (Expression.Lambda(initDbParam,initDbParam).Compile(),pmap)
            | projs -> 
                let replaceParams = Dictionary<ParameterExpression, LambdaExpression>()

                // We should fetch the initial parameter type from the fist lambda input type.
                // But currently SQL will always return a list of SqlEntities.

                let initDbParam = 

                    /// The current join would break here, as it fakes anonymous object to be SqlEntity by skipping projection lambda.
                    /// So this method finds if type is join and result should be typeof<SqlEntity> and not the original type.
                    let rec shouldFlattenToSqlEntity (e:Expression) =
                        let callEntityType (exp:Expression) =
                            exp.NodeType = ExpressionType.Call &&
                            (not(isNull (exp :?> MethodCallExpression).Object)) && 
                            Type.(=)((exp :?> MethodCallExpression).Object.Type, typeof<SqlEntity>)
                        
                        let rec tupleofentities (tupleType:Type) =
                            if (tupleType.Name.StartsWith("AnonymousObject") || tupleType.Name.StartsWith("Tuple")) then
                                let ps = tupleType.GetGenericArguments()
                                ps |> Seq.forall(fun t -> (not(isNull t)) && (Type.(=)(t, typeof<SqlEntity>) || (tupleofentities t))) 
                            else false

                        if e.NodeType = ExpressionType.New then
                            let ne = e :?> NewExpression
                            (not(isNull ne)) && (ne.Type.Name.StartsWith("AnonymousObject") || ne.Type.Name.StartsWith("Tuple")) && 
                                (ne.Arguments |> Seq.forall(fun a -> (callEntityType a) || (shouldFlattenToSqlEntity a))) 
                        else if e.NodeType = ExpressionType.Parameter then
                            let p = e :?> ParameterExpression
                            tupleofentities p.Type
                        else callEntityType e

                    // Usually it's just SqlEntity but it can be also tuple in joins etc.
                    let rec foundInitParamType : Expression -> ParameterExpression = function
                        | :? LambdaExpression as lambda when lambda.Parameters.Count = 1 ->
                            if shouldFlattenToSqlEntity lambda.Parameters.[0] then
                                Expression.Parameter(typeof<SqlEntity>,"result")
                            else Expression.Parameter(lambda.Parameters.[0].Type,"result")
                        | :? MethodCallExpression as meth when meth.Arguments.Count = 1 ->
                            if shouldFlattenToSqlEntity meth.Arguments.[0] then
                                Expression.Parameter(typeof<SqlEntity>,"result")
                            else Expression.Parameter(meth.Arguments.[0].Type,"result")
                        | :? UnaryExpression as ce -> 
                            foundInitParamType ce.Operand
                        | _ -> Expression.Parameter(typeof<SqlEntity>,"result")
                    match projs.Head with
                    // We have this wrap and the lambda is the second argument:
                    | :? MethodCallExpression as meth when meth.Arguments.Count = 2 ->
                        foundInitParamType meth.Arguments.[1]
                    | _ -> foundInitParamType projs.Head

                // Multiple projections found. We need to do a function composition: prevProj >> currentProj
                // for Lamda Expressions. So this is an expression tree visitor.
                // What we actually do is go through these lambda-expressions one by one and say:
                // - Find any parameters.
                // - If they are database parameters:
                //   1) Add them to parameter list. We will parse this list to database parameters to SQL-clause
                //   2) For each of these parameters we need to also collect the lambda that is run after the SQL returns.
                // - And if it's a non-database parameter, it's a param from previous lambda-expression. 
                //   So replace the parameter with the previous lambda expression (which might have a database parameter)
                let visitExpression (currentProj:Expression) (prevProj:Expression) (dbParam:ParameterExpression) =
                    
                    let rec generateReplacementParams (proj:Expression) = 
                        match proj.NodeType, proj with
                        | ExpressionType.Lambda, (:? LambdaExpression as lambda) ->  
                            match prevProj with
                            | :? LambdaExpression as prevLambda when prevLambda <> Unchecked.defaultof<LambdaExpression> -> 
                                lambda.Parameters |> Seq.iter(fun p -> replaceParams.[p] <- prevLambda)
                            | _ when prevProj = Unchecked.defaultof<Expression> -> 
                                lambda.Parameters |> Seq.iter(fun p -> replaceParams.[p] <- Expression.Lambda(initDbParam,initDbParam))
                            | _ -> ()
                        | ExpressionType.Quote, (:? UnaryExpression as ce) ->  
                            generateReplacementParams ce.Operand
                        | ExpressionType.Call, (:? MethodCallExpression as me) ->  
                            me.Arguments |> Seq.iter generateReplacementParams
                        | _ -> ()

                    generateReplacementParams(currentProj)
                    let newProjection, projectionMap, groupProjectionMap = transform currentProj entityIndex dbParam sqlQuery.Aliases sqlQuery.UltimateChild replaceParams useCanonicalsOnSelect
                    let fixedParams = Expression.Lambda((newProjection:?>LambdaExpression).Body,initDbParam)
                    
                    if sqlQuery.Grouping.Length > 0 then

                        let rec baseKey(col:SqlColumnType) =
                            match col with
                            | KeyColumn c -> KeyColumn c
                            | CanonicalOperation(op, c) -> baseKey c
                            | c -> c
                            
                        let gatheredAggregations = 
                            sqlQuery.Grouping |> List.map(fun (group,x) ->
                                // GroupBy: collect aggreagte operations
                                // Should gather the column names what we want to aggregate, not just operations.
                                let aggregations:(alias * SqlColumnType) list =
                                    List.concat [ x; (groupProjectionMap |> Seq.toList)]
                                    |> List.collect(fun op ->
                                        if not (group |> List.isEmpty) then 
                                            group 
                                            |> List.choose(fun (baseal, cc) -> 
                                                match baseKey(cc), op with
                                                | KeyColumn c, (a,GroupColumn(AvgOp "", KeyColumn "")) -> Some (a, GroupColumn(AvgOp c, KeyColumn c))
                                                | KeyColumn c, (a,GroupColumn(MinOp "", KeyColumn "")) -> Some (a, GroupColumn(MinOp c, KeyColumn c))
                                                | KeyColumn c, (a,GroupColumn(MaxOp "", KeyColumn "")) -> Some (a, GroupColumn(MaxOp c, KeyColumn c))
                                                | KeyColumn c, (a,GroupColumn(SumOp "", KeyColumn "")) -> Some (a, GroupColumn(SumOp c, KeyColumn c))
                                                | KeyColumn c, (a,GroupColumn(StdDevOp "", KeyColumn "")) -> Some (a, GroupColumn(StdDevOp c, KeyColumn c))
                                                | KeyColumn c, (a,GroupColumn(VarianceOp "", KeyColumn "")) -> Some (a, GroupColumn(VarianceOp c, KeyColumn c))
                                                | KeyColumn c, (a,GroupColumn(KeyOp "", KeyColumn "")) -> Some (a, GroupColumn(KeyOp c, KeyColumn c))
                                                | KeyColumn c, (a,GroupColumn(CountOp "", KeyColumn "")) -> Some (a, GroupColumn(CountOp c, KeyColumn c))
                                                | KeyColumn c, (a,GroupColumn(CountDistOp "", KeyColumn "")) -> Some (a, GroupColumn(CountDistOp c, KeyColumn c))
                                                | KeyColumn c, (a,GroupColumn(agg, KeyColumn g)) when g <> "" -> Some (op)
                                                | KeyColumn c, (a,GroupColumn(_)) when Utilities.getBaseColumnName (snd op) <> "" -> Some (op)
                                                | KeyColumn c, (a,KeyColumn(c2)) when Utilities.getBaseColumnName (snd op) <> "" -> Some (op)
                                                | _ -> None)

                                        else [op]
                                    )
                                group, aggregations)
                        groupgin.AddRange(gatheredAggregations)

                    //QueryEvents.PublishExpression fixedParams
                    fixedParams,projectionMap
                
                let rec composeProjections projs prevLambda (foundparams : Dictionary<string, ResizeArray<ProjectionParameter>>) = 
                    match projs with 
                    | [] -> prevLambda, foundparams
                    | proj::tail ->
                        let operations =
                            // Full entities don't need to be transferred recursively
                            // but Canonical operation structures cannot be lost.
                            [| for KeyValue(k,v) in foundparams do
                                if k = "" then yield k, v
                                else
                                  for colp in v do
                                    match colp with
                                    | OperationColumn _ ->  yield k, v
                                    | EntityColumn _ -> () |]
                        foundparams.Clear()
                        operations |> Array.distinct |> Array.iter(fun (k, v) ->foundparams.Add(k,v))
                        let lambda1, dbparams1 = visitExpression proj prevLambda initDbParam
                        dbparams1 |> Seq.iter(fun k -> foundparams.[k.Key] <- k.Value )
                        composeProjections tail lambda1 foundparams

                let generatedMegaLambda, finalParams = composeProjections projs (Unchecked.defaultof<LambdaExpression>) (Dictionary<string, ResizeArray<ProjectionParameter>>())
                QueryEvents.PublishExpression generatedMegaLambda
                (generatedMegaLambda.Compile(),finalParams)

        let sqlQuery = 
            if groupgin.Count > 0 then
                let cleaned =
                    let lst = groupgin |> Seq.toList
                    match lst |> List.filter(fun (keys,agg) -> not agg.IsEmpty) with
                    | [] -> lst
                    | filtered -> filtered
                { sqlQuery with Grouping = cleaned |> Seq.distinct |> Seq.toList }
            else sqlQuery

        // a special case here to handle queries that start from the relationship of an individual
        let sqlQuery,baseAlias =
            if sqlQuery.Aliases.Count = 0 then
               let alias =
                   match projectionColumns.Keys.Count with
                   | 0 -> baseAlias
                   | 1 -> projectionColumns.Keys |> Seq.head
                   | x -> // Multiple aliases for same basetable name. We have to select one and merge columns.
                        let starSelect = projectionColumns |> Seq.tryPick(fun p -> match p.Value.Count with | 0 -> Some p | _ -> None)
                        match starSelect with
                        | Some sel when useCanonicalsOnSelect && projectionColumns |> Seq.exists(fun kvp ->
                                        kvp.Value |> Seq.exists(fun co -> match co with | OperationColumn _ -> true | EntityColumn _ -> false)) ->
                            // Whole entity plus operation columns. We need urgent table lookup.
                            let myLock = provider.GetLockObject()
                            let cols = lock myLock (fun () -> provider.GetColumns (con,snd sqlQuery.UltimateChild.Value))
                            let opcols = projectionColumns |> Seq.collect(fun p -> p.Value)
                            let allcols = cols |> Map.toSeq |> Seq.map(fun (k,_) -> EntityColumn k)
                            let itms = Seq.concat [|opcols;allcols|] |> Seq.distinct |> Seq.toList
                            let tmp = sel
                            projectionColumns.Clear()
                            projectionColumns.Add(tmp.Key, ResizeArray<ProjectionParameter>(itms))
                            tmp.Key
                        | Some sel ->
                            let tmp = sel
                            projectionColumns.Clear()
                            projectionColumns.Add(tmp.Key, tmp.Value)
                            tmp.Key
                        | None ->
                            let itms = projectionColumns |> Seq.collect(fun p -> p.Value) |> Seq.distinct |> Seq.toList
                            let selKey = (projectionColumns.Keys |> Seq.head)
                            projectionColumns.Clear()
                            projectionColumns.Add(selKey, ResizeArray<ProjectionParameter>(itms))
                            projectionColumns.Keys |> Seq.head

               { sqlQuery with UltimateChild = Some(alias,snd sqlQuery.UltimateChild.Value) }, alias
            else sqlQuery,baseAlias

        let baseName = if baseAlias <> "" then baseAlias else (fst sqlQuery.UltimateChild.Value)
        let resolve defaultTable name =
            // name will be blank when there is only a single table as it never gets
            // tupled by the LINQ infrastructure. In this case we know it must be referring
            // to the only table in the query, so replace it
            if String.IsNullOrWhiteSpace(name) || name = "__base__" || entityIndex.Count = 0 then
                match defaultTable with
                | ValueSome(s) -> s
                | ValueNone -> baseName
            else 
                let tbl = Utilities.resolveTuplePropertyName name entityIndex
                if tbl = "" then baseName else tbl

        let resolve name =
            // name will be blank when there is only a single table as it never gets
            // tupled by the LINQ infrastructure. In this case we know it must be referring
            // to the only table in the query, so replace it
            if String.IsNullOrWhiteSpace(name) || name = "__base__" || entityIndex.Count = 0 then (fst sqlQuery.UltimateChild.Value)
            else 
                let tbl = Utilities.resolveTuplePropertyName name entityIndex
                if tbl = "" || not(entityIndex.Contains tbl) then baseName else tbl

        
        // Resolves aliases on canonical multi-column functions
        let rec visitCanonicals resolverfunc = function
            | CanonicalOperation(subItem, col) -> 
                let inline resolver (al:string) = // Don't try to resolve if already resolved
                    if al = "" || al.StartsWith "Item" || entityIndex.Count = 0 then resolverfunc al else al
                let resolvedSub =
                    match subItem with
                    | BasicMathOfColumns(op,al,col2) -> BasicMathOfColumns(op,resolver al, visitCanonicals resolverfunc col2)
                    | Substring(SqlCol(al, col2)) -> Substring(SqlCol(resolver al, visitCanonicals resolverfunc col2))

                    | SubstringWithLength(SqlCol(al2, col2),SqlCol(al3, col3)) -> SubstringWithLength(SqlCol(resolver al2, visitCanonicals resolver col2),SqlCol(resolver al3, visitCanonicals resolverfunc col3))
                    | SubstringWithLength(x,SqlCol(al3, col3)) -> SubstringWithLength(x,SqlCol(resolver al3, visitCanonicals resolverfunc col3))
                    | SubstringWithLength(SqlCol(al2, col2),x) -> SubstringWithLength(SqlCol(resolver al2, visitCanonicals resolverfunc col2),x)

                    | Replace(SqlCol(al2, col2),SqlCol(al3, col3)) -> Replace(SqlCol(resolver al2, visitCanonicals resolver col2),SqlCol(resolver al3, visitCanonicals resolverfunc col3))
                    | Replace(x,SqlCol(al3, col3)) -> Replace(x,SqlCol(resolver al3, visitCanonicals resolverfunc col3))
                    | Replace(SqlCol(al2, col2),x) -> Replace(SqlCol(resolver al2, visitCanonicals resolverfunc col2),x)

                    | IndexOfStart(SqlCol(al2, col2),SqlCol(al3, col3)) -> IndexOfStart(SqlCol(resolver al2, visitCanonicals resolver col2),SqlCol(resolver al3, visitCanonicals resolverfunc col3))
                    | IndexOfStart(x,SqlCol(al3, col3)) -> IndexOfStart(x,SqlCol(resolver al3, visitCanonicals resolverfunc col3))
                    | IndexOfStart(SqlCol(al2, col2),x) -> IndexOfStart(SqlCol(resolver al2, visitCanonicals resolverfunc col2),x)

                    | IndexOf(SqlCol(al, col2)) -> IndexOf(SqlCol(resolver al, visitCanonicals resolverfunc col2))

                    | AddYears(SqlCol(al, col2)) -> AddYears(SqlCol(resolver al, visitCanonicals resolverfunc col2))
                    | AddDays(SqlCol(al, col2)) -> AddDays(SqlCol(resolver al, visitCanonicals resolverfunc col2))
                    | AddMinutes(SqlCol(al, col2)) -> AddMinutes(SqlCol(resolver al, visitCanonicals resolverfunc col2))
                    | DateDiffDays(SqlCol(al, col2)) -> DateDiffDays(SqlCol(resolver al, visitCanonicals resolverfunc col2))
                    | DateDiffSecs(SqlCol(al, col2)) -> DateDiffSecs(SqlCol(resolver al, visitCanonicals resolverfunc col2))

                    | Greatest(SqlCol(al, col)) -> Greatest(SqlCol(resolver al, visitCanonicals resolverfunc col))
                    | Least(SqlCol(al, col)) -> Least(SqlCol(resolver al, visitCanonicals resolverfunc col))

                    | CaseSql(f, SqlCol(al, col)) -> CaseSql(resolveFilterList f, SqlCol(resolver al, visitCanonicals resolverfunc col))
                    | CaseNotSql(f, SqlCol(al, col)) -> CaseNotSql(resolveFilterList f, SqlCol(resolver al, visitCanonicals resolverfunc col))
                    | CaseSql(f, x) -> CaseSql(resolveFilterList f, x)
                    | CaseNotSql(f, x) -> CaseNotSql(resolveFilterList f, x)
                    | CaseSqlPlain(f, a, b) -> CaseSqlPlain(resolveFilterList f, a, b)
                    | Pow(SqlCol(al, col2)) -> Pow(SqlCol(resolver al, visitCanonicals resolverfunc col2))

                    | x -> x
                CanonicalOperation(resolvedSub, visitCanonicals resolverfunc col)
            | GroupColumn(op, col) -> GroupColumn(op, visitCanonicals resolverfunc col)
            | x -> x

        and resolveC = visitCanonicals resolve

        and tryResolveC (e: Option<obj>) : Option<obj> = e |> Option.map(function
            | :? (alias * SqlColumnType) as a ->
                let al,col = a
                if al = "" || al.StartsWith "Item" || entityIndex.Count = 0 then
                    (resolve al, resolveC col) :> obj
                else // Already resolved
                    (legaliseName al, resolveC col) :> obj
            | x -> x)

        and resolveFilterList = function
            | And(xs,y) -> And(xs|>List.map(fun (a,b,c,d) -> resolve a,resolveC b,c,tryResolveC d),Option.map (List.map resolveFilterList) y)
            | Or(xs,y) -> Or(xs|>List.map(fun (a,b,c,d) -> resolve a,resolveC b,c,tryResolveC d),Option.map (List.map resolveFilterList) y)
            | ConstantTrue -> ConstantTrue
            | ConstantFalse -> ConstantFalse
            | NotSupported x ->  failwithf "Not supported: %O" x
            
        // the crazy LINQ infrastructure does all kinds of weird things with joins which means some information
        // is lost up and down the expression tree, but now with all the data available we can resolve the problems...

        // 1.
        // re-map the tuple arg names to their proper aliases in the filters
        // its possible to do this when converting the expression but its
        // much easier at this stage once we have knowledge of the whole query
        let sqlQuery = { sqlQuery with Filters = List.map resolveFilterList sqlQuery.Filters
                                       Ordering = List.map(function 
                                                            |("",b,c) ->
                                                                match resolve "" with
                                                                | "" when baseAlias <> "" -> (baseAlias,resolveC b,c) 
                                                                | x -> (x,resolveC b,c) 
                                                            | a,c,b -> resolve a, resolveC c,b) sqlQuery.Ordering 
                                       // Resolve other canonical function columns also:
                                       Grouping = sqlQuery.Grouping |> List.map(fun (g,a) -> g|>List.map(fun (ga, gk) -> resolve ga, resolveC gk), a|>List.map(fun(ag,aa)->resolve ag,resolveC aa)) 
                                       AggregateOp = sqlQuery.AggregateOp |> List.map(fun (a,c) -> (resolve a, resolveC c))
                       }

        // 2.
        // Some aliases will have blank table information, but these can be resolved by looking
        // in the link data or ultimate base entity
        let inline resolveAlias alias table =
            if table.Name <> "" then table else
            match sqlQuery.UltimateChild with
            | Some(uc) when alias = fst uc -> snd uc
            | _ -> sqlQuery.Links
                   |> List.pick(fun (_,linkData,innerAlias) -> if innerAlias = alias then Some(linkData.PrimaryTable) else None)
        let sqlQuery = { sqlQuery with Aliases = Map.map resolveAlias sqlQuery.Aliases }

        // 3.
        // Some link data will be missing its foreign table data which needs setting to the resolved table of the
        // outer alias - this happens depending on the which way the join is around - information is "lost" up the tree which
        // able to be resolved now.
        let resolveLinks (outerAlias:alias, linkData:LinkData, innerAlias) =
            let resolved = sqlQuery.Aliases.[outerAlias]
            let inline resolvePrimary name =
                if String.IsNullOrWhiteSpace(name) || name = "__base__" then innerAlias
                else 
                    let tbl = Utilities.resolveTuplePropertyName name entityIndex
                    if tbl = "" then innerAlias else tbl
            let inline resolveForeign name =
                if String.IsNullOrWhiteSpace(name) || name = "__base__" then outerAlias
                else 
                    let tbl = Utilities.resolveTuplePropertyName name entityIndex
                    if tbl = "" then outerAlias else tbl

            let resolvedLinkData =
                if linkData.RelDirection = RelationshipDirection.Children then
                    { 
                        linkData with PrimaryKey = linkData.PrimaryKey |> List.map(visitCanonicals resolvePrimary)
                                      ForeignKey = linkData.ForeignKey |> List.map(visitCanonicals resolveForeign)
                    }
                else
                    {
                        linkData with PrimaryKey = linkData.PrimaryKey |> List.map(visitCanonicals resolveForeign)
                                      ForeignKey = linkData.ForeignKey |> List.map(visitCanonicals resolvePrimary)
                    }

            if linkData.ForeignTable.Name <> "" then (outerAlias, resolvedLinkData, innerAlias)
            else
                let oa = if outerAlias = "" then resolved.Name else outerAlias
                (oa, { resolvedLinkData with ForeignTable = resolved }, innerAlias)

        let sqlQuery = { sqlQuery with Links = List.map resolveLinks sqlQuery.Links }

        let opAliasResolves = seq {
                for KeyValue(k, v) in projectionColumns do
                    if v.Exists(fun i -> match i with OperationColumn _ -> true | _ -> false) then
                        let ops = v |> Seq.map (function | OperationColumn (k,o) -> OperationColumn (k,resolveC o) | x -> x)
                        yield k, ResizeArray(ops)
            } 

        opAliasResolves |> Seq.toList |> List.iter(fun (k, ops) -> projectionColumns.[k] <- ops)
        
        // make sure the provider has cached the columns for the tables within the projection
        projectionColumns
        |> Seq.map(function KeyValue(k,_) ->
                                match sqlQuery.Aliases.TryFind k with
                                | Some v -> v
                                | None -> snd sqlQuery.UltimateChild.Value)
        |> Seq.distinct
        |> Seq.iter(function table ->
                                let myLock = provider.GetLockObject()
                                lock myLock (fun () -> provider.GetColumns (con,table) |> ignore ))

        let (sql,parameters) = provider.GenerateQueryText(sqlQuery,baseAlias,baseTable,projectionColumns,isDeleteScript,con)

        (sql,parameters,projectionDelegate,baseTable)
