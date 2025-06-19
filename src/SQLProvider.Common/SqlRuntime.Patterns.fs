module internal FSharp.Data.Sql.Patterns

open System
open System.Linq.Expressions
open System.Reflection
open FSharp.Data.Sql
open FSharp.Data.Sql.Schema

/// Marker interface
type ISqlQueryable = interface end

[<return: Struct>]
let inline (|MethodWithName|_|)   (s:string) (m:MethodInfo)   = if s = m.Name then ValueSome () else ValueNone
[<return: Struct>]
let inline (|PropertyWithName|_|) (s:string) (m:PropertyInfo) = if s = m.Name then ValueSome () else ValueNone

let inline (|MemberAccess|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.MemberAccess, ( :? MemberExpression as me) -> Some me
    | _ -> None

let (|MethodCall|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Call, (:? MethodCallExpression as e) -> 
        Some ((match e.Object with null -> None | obj -> Some obj), e.Method, Seq.toList e.Arguments)
    | _ -> None

let (|NewExpr|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.New , (:? NewExpression as e) -> 
        Some (e.Constructor, Seq.toList e.Arguments)
    | _ -> None

let (|Constant|_|) (exp:Expression) =
    let e = ExpressionOptimizer.doReduction exp
    match e.NodeType, e with 
    | ExpressionType.Constant, (:? ConstantExpression as ce) -> Some (ce.Value, ce.Type)
    | _ -> None

let rec (|OptionalConvertOrTypeAs|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Convert, (:? UnaryExpression as ue ) 
    | ExpressionType.TypeAs, (:? UnaryExpression as ue ) -> 
        match ue.Operand with OptionalConvertOrTypeAs(x) -> x
    | ExpressionType.Call, (:? MethodCallExpression as e) when e.Method.Name = "Parse" && e.Arguments.Count = 1 ->
        // Don't do any magic, just: DateTime.Parse('2000-01-01') -> '2000-01-01'
        e.Arguments.[0]
    | ExpressionType.Call, (:? MethodCallExpression as e) when e.Method.Name = "Box" && e.Arguments.Count = 1 && e.Arguments.[0].Type.IsValueType ->
        e.Arguments.[0]
    | ExpressionType.Call, (:? MethodCallExpression as e) when isNull e.Object && e.Method.Name = "ToString" && e.Arguments.Count = 1 ->
        e.Arguments.[0]
    | _ -> e

let (|SeqValuesQueryable|_|) (e:Expression) =
    let rec isQueryable (ty : Type) = 
        ty.FindInterfaces((fun ty _ -> Type.(=)(ty, typeof<System.Linq.IQueryable>)), null)
        |> (not << Seq.isEmpty)

    match (isQueryable e.Type) with
    | false -> None
    | true ->
        match e.NodeType, e with
        | ExpressionType.Constant, (:? ConstantExpression as ce) ->
            match ce.Value with
            | :? ISqlQueryable ->
                let values = (Expression.Lambda(e).Compile() :?> Func<System.Linq.IQueryable>).Invoke()
                Some values
            | _ -> None
        | _ ->
            let values = (Expression.Lambda(e).Compile() :?> Func<System.Linq.IQueryable>).Invoke()
            match values with
            | :? ISqlQueryable -> Some values
            | _ -> None

let (|SeqValues|_|) (e:Expression) =
    if e.Type.FullName = "System.String" then None // String is char[] but we don't want to hit that!
    else
    let rec isEnumerable (ty : Type) = 
        ty.FindInterfaces((fun ty _ -> Type.(=)(ty, typeof<System.Collections.IEnumerable>)), null)
        |> (not << Seq.isEmpty)

    match (isEnumerable e.Type) with
    | false -> None
    | true ->
        // Here, if e is SQLQueryable<_>, we could avoid execution and nest the queries like
        // select x from xs where x in (select y from ys)
        // ...but instead we just execute the sub-query. Works when sub-query results are small.

        let values = 
            match e.NodeType, e with
            | ExpressionType.Constant, (:? ConstantExpression as ce) when not(isNull ce.Value) ->
                ce.Value :?> System.Collections.IEnumerable
            | ExpressionType.MemberAccess, (:? MemberExpression as me) when (me.Expression :? ConstantExpression) ->
                let ceVal = (me.Expression :?> ConstantExpression).Value
                let myVal = 
                    match me.Member with
                    | :? FieldInfo as fieldInfo when not(isNull(fieldInfo)) ->
                        fieldInfo.GetValue ceVal
                    | :? PropertyInfo as propInfo when not(isNull(propInfo)) ->
                        propInfo.GetValue(ceVal, null)
                    | _ -> ceVal
                myVal :?> System.Collections.IEnumerable
            | _ ->
                (Expression.Lambda(e).Compile() :?> Func<System.Collections.IEnumerable>).Invoke()

        // Working with untyped IEnumerable so need to do a lot manually instead of using Seq
        // Work out the size the sequence

        let count =
            match values with
            | :? System.Collections.ICollection as ic -> ic.Count
            | _ ->
                let mutable count = 0
                for obj in values do
                    count <- count + 1
                count
        // Create and populate the array
        let array = Array.CreateInstance(typeof<System.Object>, count)
        let mutable i = 0
        for obj in values do
            array.SetValue(obj, i)
            i <- i + 1
        // Return the array
        Some array


let (|PropertyGet|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.MemberAccess, ( :? MemberExpression as e) -> 
        match e.Member with 
        | :? PropertyInfo as p -> Some ((match e.Expression with null -> None | obj -> Some obj), p)
        | _ -> None
    | _ -> None

let (|ConvertOrTypeAs|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Convert, (:? UnaryExpression as ue ) 
    | ExpressionType.TypeAs, (:? UnaryExpression as ue ) -> Some ue.Operand
    | _ -> None

[<return: Struct>]
let inline (|OptionNone|_|) (e: Expression) =
    match e with
    | MethodCall(None,MethodWithName("get_None"),[]) ->
        match e with
        | :? MethodCallExpression as e when Common.Utilities.isOpt e.Method.DeclaringType -> ValueSome()
        | _ -> ValueNone
    | _ -> ValueNone

[<return: Struct>]
let (|NullConstant|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Constant, (:? ConstantExpression as ce) when isNull ce.Value -> ValueSome()
    | _ -> ValueNone

let (|ConstantOrNullableConstant|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Constant, (:? ConstantExpression as ce) ->
        if Common.Utilities.isOpt ce.Type then
            match ce.Type.GetProperty("Value").GetValue(ce.Value,[||]) with
            | null -> Some(Some(ce.Value))
            | optVal -> Some(Some(optVal))
        else
            Some(Some(ce.Value))
    | ExpressionType.Convert, (:? UnaryExpression as ue ) -> 
        match ue.Operand.NodeType, ue.Operand with
        | ExpressionType.Constant, (:? ConstantExpression as ce) -> if isNull ce.Value then Some(None) else Some(Some(ce.Value))
        | ExpressionType.New, (:? NewExpression as ne) -> Some(Some(Expression.Lambda(ne).Compile().DynamicInvoke()))
        | _ -> failwith ("unsupported nullable expression " + e.ToString())
    | _ -> None

[<return: Struct>]
let (|BoolStrict|_|)   = function Constant((:? bool   as b),_) -> ValueSome b | _ -> ValueNone
[<return: Struct>]
let (|String|_|) = function Constant((:? string as s),_) -> ValueSome s | _ -> ValueNone
[<return: Struct>]
let (|Int|_|)    = function Constant((:? int    as i),_) -> ValueSome i | _ -> ValueNone
[<return: Struct>]
let (|Float|_|)    = function Constant((:? float    as i),_) -> ValueSome i | _ -> ValueNone

let rec (|Bool|_|) (e:Expression) = 
    match e.NodeType, e with
    | _, BoolStrict(b) -> Some b
    | ExpressionType.Not, (:? UnaryExpression as ue) -> 
        match ue.Operand with Bool x -> Some(not x) | _ -> None
    | _ -> None

[<return: Struct>]
let inline (|ParamName|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Parameter, (:? ParameterExpression as pe) ->  ValueSome pe.Name
    | _ -> ValueNone    

[<return: Struct>]
let inline (|ParamWithName|_|) (s:String) (e:Expression) = 
    match e with 
    | ParamName(n) when s = n -> ValueSome ()
    | _ -> ValueNone    
    
let (|Lambda|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Lambda, (:? LambdaExpression as ce) ->  Some (Seq.toList ce.Parameters, ce.Body)
    | _ -> None

let inline (|OptionalQuote|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Quote, (:? UnaryExpression as ce) ->  ce.Operand
    | _ -> e

let (|OptionalCopyOfStruct|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Call, MethodCall(Some (Lambda([ParamName para], (:? MemberExpression as me))),MethodWithName("Invoke"),[inner]) when para = "copyOfStruct" && me.Member.Name = "Value" -> inner
    | _ -> e

let (|CopyOfStruct|_|) (membername:String) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Call, MethodCall(Some (Lambda([ParamName para], (:? MemberExpression as me))),MethodWithName("Invoke"),[inner]) when para = "copyOfStruct" && me.Member.Name = membername -> Some inner
    | _ -> None

let (|OptionalFSharpOptionValue|) (e:Expression) = 
    match e.NodeType, e with
    | ExpressionType.MemberAccess, ( :? MemberExpression as e) -> 
        match e.Member with 
        | :? PropertyInfo as p when p.Name = "Value" && Common.Utilities.isOpt e.Member.DeclaringType -> e.Expression
        | _ -> upcast e
    | ExpressionType.Call, OptionalCopyOfStruct ( :? MethodCallExpression as e)
        when e.Method.Name = "Some" && Common.Utilities.isOpt e.Method.DeclaringType -> e.Arguments.[0]
    | _, OptionalCopyOfStruct n -> n

let (|AndAlso|_|) (e:Expression) =
    match e.NodeType, e with
    | ExpressionType.AndAlso, ( :? BinaryExpression as be) -> Some(be.Left,be.Right)
    | _ -> None
    
let (|OrElse|_|) (e:Expression) =
    match e.NodeType, e with
    | ExpressionType.OrElse, ( :? BinaryExpression as be) -> Some(be.Left,be.Right)
    | _ -> None
    
let (|AndAlsoOrElse|_|) (e:Expression) =
    match e.NodeType, e with
    | ExpressionType.OrElse,  ( :? BinaryExpression as be) 
    | ExpressionType.AndAlso, ( :? BinaryExpression as be)  -> Some(be.Left,be.Right)
    | _ -> None

let (|FSharpIsNullMethod|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Call, (:? MethodCallExpression as e) ->
        if isNull e.Object && e.Method.Name = "IsNull" && e.Arguments.Count = 1 && e.Method.DeclaringType.FullName = "Microsoft.FSharp.Core.Operators" then
            Some (e.Arguments.[0])
        else
            None
    | _ -> None

let (|OptionIsSome|_|) : Expression -> _ = function    
    | MethodCall(None,MethodWithName("get_IsSome"), [e] ) -> Some e
    | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Not ->
        match ue.Operand with
        | MethodCall(None,MethodWithName("get_IsNone"), [e] ) -> Some e
        | MemberAccess me when me.Member.Name = "IsNone" -> Some (me.Expression)
        | MethodCall(Some (Lambda([ParamName para], (:? MemberExpression as me))),MethodWithName("Invoke"),[e]) when para = "copyOfStruct" && me.Member.Name = "IsNone" -> Some e
        | CopyOfStruct "IsNone" exp -> Some exp
        | FSharpIsNullMethod exp -> Some exp
        | _ -> None
    | MemberAccess me when me.Member.Name = "IsSome" -> Some (me.Expression)
    | CopyOfStruct "IsSome" exp -> Some exp
    | _ -> None

let (|OptionIsNone|_|) : Expression -> _ = function    
    | MethodCall(None,MethodWithName("get_IsNone"), [e] ) -> Some e
    | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Not ->
        match ue.Operand with
        | MethodCall(None,MethodWithName("get_IsSome"), [e] ) -> Some e
        | MemberAccess me when me.Member.Name = "IsSome" -> Some (me.Expression)
        | CopyOfStruct "IsSome" exp -> Some exp
        | _ -> None
    | MemberAccess me when me.Member.Name = "IsNone" -> Some (me.Expression)
    | CopyOfStruct "IsNone" exp -> Some exp
    | FSharpIsNullMethod exp -> Some exp
    | _ -> None

let (|SqlCondOp|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Equal,              (:? BinaryExpression as ce) -> Some (ConditionOperator.Equal,        ce.Left,ce.Right)
    | ExpressionType.LessThan,           (:? BinaryExpression as ce) -> Some (ConditionOperator.LessThan,     ce.Left,ce.Right)
    | ExpressionType.LessThanOrEqual,    (:? BinaryExpression as ce) -> Some (ConditionOperator.LessEqual,    ce.Left,ce.Right)
    | ExpressionType.GreaterThan,        (:? BinaryExpression as ce) -> Some (ConditionOperator.GreaterThan,  ce.Left,ce.Right)
    | ExpressionType.GreaterThanOrEqual, (:? BinaryExpression as ce) -> Some (ConditionOperator.GreaterEqual, ce.Left,ce.Right)
    | ExpressionType.NotEqual,           (:? BinaryExpression as ce) -> Some (ConditionOperator.NotEqual,     ce.Left,ce.Right)
    | _ -> None

let (|SqlNegativeCondOp|_|) (e:Expression) = 
    match e.NodeType, e with
    | ExpressionType.Not, (:? UnaryExpression as ue) ->
        match ue.Operand.NodeType, ue.Operand with
        | ExpressionType.NotEqual,           (:? BinaryExpression as ce) -> Some (ConditionOperator.Equal,        ce.Left,ce.Right)
        | ExpressionType.GreaterThanOrEqual, (:? BinaryExpression as ce) -> Some (ConditionOperator.LessThan,     ce.Left,ce.Right)
        | ExpressionType.GreaterThan,        (:? BinaryExpression as ce) -> Some (ConditionOperator.LessEqual,    ce.Left,ce.Right)
        | ExpressionType.LessThanOrEqual,    (:? BinaryExpression as ce) -> Some (ConditionOperator.GreaterThan,  ce.Left,ce.Right)
        | ExpressionType.LessThan,           (:? BinaryExpression as ce) -> Some (ConditionOperator.GreaterEqual, ce.Left,ce.Right)
        | ExpressionType.Equal,              (:? BinaryExpression as ce) -> Some (ConditionOperator.NotEqual,     ce.Left,ce.Right)
        | _ -> None
    | _ -> None

// Unwrap Microsoft.FSharp.Core.Operators.Abs$W(ToFSharpFunc(arg0_0 => Abs(arg0_0)), x) to Abs(x)
let (|MethodCallOrFSharpWrap|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Call, (:? MethodCallExpression as e) ->
        if isNull e.Object && e.Method.Name.Contains("$") && e.Arguments.Count = 2 && e.Method.DeclaringType.FullName = "Microsoft.FSharp.Core.Operators" then
            match e.Arguments.[0].NodeType, e.Arguments.[0] with
            | ExpressionType.Call, (:? MethodCallExpression as ec) when ec.Method.Name = "ToFSharpFunc" && ec.Arguments.Count = 1 ->
                match ec.Arguments.[0] with
                | Lambda(arg, body) ->
                    match body.NodeType, body with
                    | ExpressionType.Call, (:? MethodCallExpression as eop) ->
                        Some (None, eop.Method, [e.Arguments.[1]])
                    | _ -> Some ((match e.Object with null -> None | obj -> Some obj), e.Method, Seq.toList e.Arguments)
                | _ -> Some ((match e.Object with null -> None | obj -> Some obj), e.Method, Seq.toList e.Arguments)
            | _ -> Some ((match e.Object with null -> None | obj -> Some obj), e.Method, Seq.toList e.Arguments)
        else
            Some ((match e.Object with null -> None | obj -> Some obj), e.Method, Seq.toList e.Arguments)
    | _ -> None

let (|SqlPlainColumnGet|_|) = function
    | OptionalFSharpOptionValue(MethodCall(Some(o),((MethodWithName "GetColumn" as meth) | (MethodWithName "GetColumnOption" as meth)| (MethodWithName "GetColumnValueOption" as meth)),[String key])) when o.Type.Name = "SqlEntity" -> 
        match o with
        | MemberAccess m  -> 
            match m.Expression with
            | null -> Some(m.Member.Name,KeyColumn key,meth.ReturnType) // Nested tuples:
            | MemberAccess x when x.Type.Name.StartsWith("AnonymousObject") && x.Member.Name = "Item8" ->
                let rec findNested (expr:MemberExpression) depth = 
                    match expr.Expression with
                    | MemberAccess xx when xx.Type.Name.StartsWith("AnonymousObject") && xx.Member.Name = "Item8" ->
                        findNested xx (depth+7)
                    | _ -> depth
                let isOk, memberName = Int32.TryParse(m.Member.Name.Replace("Item","")) 
                if not isOk then None
                else
                Some(("Item"+(memberName+(findNested x 7)).ToString()),KeyColumn key,meth.ReturnType) 
            | _ -> Some(m.Member.Name,KeyColumn key,meth.ReturnType)
        | p when p.NodeType = ExpressionType.Parameter ->
            let par = p :?> ParameterExpression
            Some((if String.IsNullOrEmpty par.Name then String.Empty else par.Name),KeyColumn key,meth.ReturnType) 
        | _ -> None
    | _ -> None

let (|SqlSubtableColumnGet|_|) = function
    | OptionalFSharpOptionValue(MethodCall(Some(o),((MethodWithName "GetColumn" as meth) | (MethodWithName "GetColumnOption" as meth) | (MethodWithName "GetColumnValueOption" as meth)),[String key])) when o.Type.Name = "SqlEntity" -> 
        match o.NodeType, o with
        | ExpressionType.Call, (:? MethodCallExpression as ce)
                when (ce.Method.Name = "GetSubTable" && (not(isNull ce.Object)) && (ce.Object :? ParameterExpression)) ->
            let par = ce.Object :?> ParameterExpression
            if String.IsNullOrEmpty par.Name then None
            else
            Some(par.Name,KeyColumn key,meth.ReturnType) 
        | _ -> None
    | _ -> None

let decimalTypes = [| typeof<decimal>; typeof<float32>; typeof<double>; typeof<float>;
                      typeof<Option<decimal>>; typeof<Option<float32>>; typeof<Option<double>>; typeof<Option<float>>;
                      typeof<ValueOption<decimal>>; typeof<ValueOption<float32>>; typeof<ValueOption<double>>; typeof<ValueOption<float>>;|]
let integerTypes = [| typeof<Int32>; typeof<Int64>; typeof<Int16>; typeof<int8>;typeof<UInt32>; typeof<UInt64>; typeof<UInt16>; typeof<uint8>; typeof<bigint>;
                      typeof<Option<Int32>>; typeof<Option<Int64>>; typeof<Option<Int16>>; typeof<Option<int8>>; typeof<Option<UInt32>>; typeof<Option<UInt64>>; typeof<Option<UInt16>>; typeof<Option<uint8>>; typeof<Option<bigint>>;
                      typeof<ValueOption<Int32>>; typeof<ValueOption<Int64>>; typeof<ValueOption<Int16>>; typeof<ValueOption<int8>>; typeof<ValueOption<UInt32>>; typeof<ValueOption<UInt64>>; typeof<ValueOption<UInt16>>; typeof<ValueOption<uint8>>; typeof<ValueOption<bigint>>;|]

let intType (typ:Type) = 
    if (not (isNull typ)) && Common.Utilities.isCOpt typ then typeof<Option<int>>
    elif (not (isNull typ)) && Common.Utilities.isVOpt typ then typeof<ValueOption<int>>
    else typeof<int>

let inline internal getRightFromOp (right:Expression) =
    match right.NodeType, right with
    | ExpressionType.Constant, (:? ConstantExpression as ce) -> ce.Value
    | ExpressionType.MemberAccess, (:? MemberExpression as me) when (me.Expression :? ConstantExpression) ->
        let ceVal = (me.Expression :?> ConstantExpression).Value
        let myVal = 
            match me.Member with
            | :? FieldInfo as fieldInfo when not(isNull fieldInfo) ->
                fieldInfo.GetValue ceVal
            | :? PropertyInfo as propInfo when not(isNull propInfo) ->
                propInfo.GetValue(ceVal, null)
            | _ -> ceVal
        myVal
    | ExpressionType.Call, (:? MethodCallExpression as e) when e.Method.ReturnType.IsClass -> (Expression.Lambda(right).Compile() :?> Func<_>).Invoke() |> box
    | _ -> Expression.Lambda(right).Compile().DynamicInvoke()

let rec (|SqlColumnGet|_|) (ex:Expression) =

    let e = ExpressionOptimizer.doReduction ex
    match e.NodeType, e with 
    | _, SqlPlainColumnGet(m, k, t) -> Some(m,k,t)

    // These are aggregations, e.g. GROUPBY, HAVING-clause
    | ExpressionType.MemberAccess, ( :? MemberExpression as me) when 
            (not (isNull me.Expression || isNull me.Expression.Type || isNull me.Expression.Type.Name)) &&
                Common.Utilities.isGrp me.Expression.Type -> 
        match me.Member with 
        | :? PropertyInfo as p when p.Name = "Key" -> Some(String.Empty, GroupColumn (KeyOp "",SqlColumnType.KeyColumn("Key")), p.DeclaringType) 
        | _ -> None
    | ExpressionType.Call, ( :? MethodCallExpression as e) when (not (isNull e.Arguments)) && e.Arguments.Count = 1 && 
            not( isNull e.Arguments.[0] || isNull e.Arguments.[0].Type || isNull e.Arguments.[0].Type.Name) &&
                Common.Utilities.isGrp e.Arguments.[0].Type ->
        if e.Arguments.[0].NodeType = ExpressionType.Parameter then
            let pn = match e.Arguments.[0] with :? ParameterExpression as p -> p.Name | _ -> e.Method.Name
            match e.Method.Name with
            | "Count" -> Some(String.Empty, GroupColumn (CountOp "",SqlColumnType.KeyColumn(pn)), e.Method.DeclaringType)
            | "Average" | "Avg" -> Some(String.Empty, GroupColumn (AvgOp "",SqlColumnType.KeyColumn(pn)), e.Method.DeclaringType)
            | "Min" -> Some(String.Empty, GroupColumn (MinOp "",SqlColumnType.KeyColumn(pn)), e.Method.DeclaringType)
            | "Max" -> Some(String.Empty, GroupColumn (MaxOp "",SqlColumnType.KeyColumn(pn)), e.Method.DeclaringType)
            | "Sum" -> Some(String.Empty, GroupColumn (SumOp "",SqlColumnType.KeyColumn(pn)), e.Method.DeclaringType)
            | "StdDev" | "StDev"| "StandardDeviation" -> Some(String.Empty, GroupColumn (StdDevOp "",SqlColumnType.KeyColumn(pn)), e.Method.DeclaringType)
            | "Variance" -> Some(String.Empty, GroupColumn (VarianceOp "",SqlColumnType.KeyColumn(pn)), e.Method.DeclaringType)
            | _ -> None
        else 
            match e.Arguments.[0], e.Method.Name with
            | MemberAccess m, "Count" -> Some(m.Member.Name, GroupColumn (CountOp "",SqlColumnType.KeyColumn(m.Member.Name)), e.Method.DeclaringType)
            | MemberAccess m, ("Average" | "Avg") -> Some(m.Member.Name, GroupColumn (AvgOp "",SqlColumnType.KeyColumn(m.Member.Name)), e.Method.DeclaringType)
            | MemberAccess m, "Min" -> Some(m.Member.Name, GroupColumn (MinOp "",SqlColumnType.KeyColumn(m.Member.Name)), e.Method.DeclaringType)
            | MemberAccess m, "Max" -> Some(m.Member.Name, GroupColumn (MaxOp "",SqlColumnType.KeyColumn(m.Member.Name)), e.Method.DeclaringType)
            | MemberAccess m, "Sum" -> Some(m.Member.Name, GroupColumn (SumOp "",SqlColumnType.KeyColumn(m.Member.Name)), e.Method.DeclaringType)
            | MemberAccess m, ("StdDev" | "StDev" | "StandardDeviation") -> Some(m.Member.Name, GroupColumn (StdDevOp "",SqlColumnType.KeyColumn(m.Member.Name)), e.Method.DeclaringType)
            | MemberAccess m, "Variance" -> Some(m.Member.Name, GroupColumn (VarianceOp "",SqlColumnType.KeyColumn(m.Member.Name)), e.Method.DeclaringType)
            | _ -> None

    // These are canonical functions
    | _, OptionalFSharpOptionValue(MethodCall(Some(OptionalFSharpOptionValue(SqlColumnGet(alias, col, typ) as p1)), meth, par)) ->
        match meth.Name, par with
        | "ToString", [] ->   Some(alias, CanonicalOperation(CanonicalOp.CastVarchar, col), typ)
        | _ ->
            match p1.Type with
            | t when Type.(=)(t, typeof<System.String>) || Type.(=)(t, typeof<Option<System.String>>) || Type.(=)(t, typeof<ValueOption<System.String>>) -> // String functions
                match meth.Name, par with
                | "Substring", [Int startPos] -> Some(alias, CanonicalOperation(CanonicalOp.Substring(SqlConstant startPos), col), typ)
                | "Substring", [SqlColumnGet(al2,col2,typ2) as pe] when integerTypes |> Seq.exists(fun t -> Type.(=)(pe.Type, t)) -> Some(alias, CanonicalOperation(CanonicalOp.Substring(SqlCol(al2,col2)), col), typ)
                | "Substring", [Int startPos; Int strLen] -> Some(alias, CanonicalOperation(CanonicalOp.SubstringWithLength(SqlConstant startPos,SqlConstant strLen), col), typ)
                | "Substring", [SqlColumnGet(al2,col2,typ2) as pe; Int strLen] when integerTypes |> Seq.exists(fun t -> Type.(=)(pe.Type, t)) -> Some(alias, CanonicalOperation(CanonicalOp.SubstringWithLength(SqlCol(al2,col2),SqlConstant strLen), col), typ)
                | "Substring", [Int startPos; SqlColumnGet(al2,col2,typ2) as pe] when integerTypes |> Seq.exists(fun t -> Type.(=)(pe.Type, t)) -> Some(alias, CanonicalOperation(CanonicalOp.SubstringWithLength(SqlConstant startPos,SqlCol(al2,col2)), col), typ)
                | "Substring", [SqlColumnGet(al2,col2,ty2) as pe1; SqlColumnGet(al3,col3,ty3) as pe2]  when integerTypes |> Seq.exists((=) pe1.Type) && integerTypes |> Seq.exists((=) pe2.Type) -> Some(alias, CanonicalOperation(CanonicalOp.SubstringWithLength(SqlCol(al2,col2),SqlCol(al3,col3)), col), typ)
                | "ToUpper", []
                | "ToUpperInvariant", [] -> Some(alias, CanonicalOperation(CanonicalOp.ToUpper, col), typ)
                | "ToLower", [] 
                | "ToLowerInvariant", [] -> Some(alias, CanonicalOperation(CanonicalOp.ToLower, col), typ)
                | "Trim", [] -> Some(alias, CanonicalOperation(CanonicalOp.Trim, col), typ)
                | "Length", [] -> Some(alias, CanonicalOperation(CanonicalOp.Length, col), intType typ)
                | "Replace", [String itm1; String itm2] -> Some(alias, CanonicalOperation(CanonicalOp.Replace(SqlConstant(box itm1), SqlConstant itm2), col), typ)
                | "Replace", [SqlColumnGet(al2,col2,_); String itm2] -> Some(alias, CanonicalOperation(CanonicalOp.Replace(SqlCol(al2,col2), SqlConstant itm2), col), typ)
                | "Replace", [String itm1; SqlColumnGet(al2,col2,_)] -> Some(alias, CanonicalOperation(CanonicalOp.Replace(SqlConstant(box itm1), SqlCol(al2,col2)), col), typ)
                | "Replace", [SqlColumnGet(al2,col2,_); SqlColumnGet(al3,col3,_)] -> Some(alias, CanonicalOperation(CanonicalOp.Replace(SqlCol(al2,col2), SqlCol(al3,col3)), col), typ)
                | "IndexOf", [String search] -> Some(alias, CanonicalOperation(CanonicalOp.IndexOf(SqlConstant search), col), intType typ)
                | "IndexOf", [SqlColumnGet(al2,col2,_)] -> Some(alias, CanonicalOperation(CanonicalOp.IndexOf(SqlCol(al2,col2)), col), intType typ)
                | "IndexOf", [String search; Int startPos] -> Some(alias, CanonicalOperation(CanonicalOp.IndexOfStart(SqlConstant search, SqlConstant startPos), col), intType typ)
                | "IndexOf", [SqlColumnGet(al2,col2,_); Int startPos] -> Some(alias, CanonicalOperation(CanonicalOp.IndexOfStart(SqlCol(al2,col2), SqlConstant startPos), col), intType typ)
                | "IndexOf", [String search; SqlColumnGet(al2,col2,typ2) as pe] when integerTypes |> Seq.exists(fun t -> t = pe.Type) -> Some(alias, CanonicalOperation(CanonicalOp.IndexOfStart(SqlConstant search, SqlCol(al2,col2)), col), intType typ)
                | "IndexOf", [SqlColumnGet(al2,col2,_); SqlColumnGet(al3,col3,typ2) as pe] when integerTypes |> Seq.exists(fun t -> Type.(=)(pe.Type, t)) -> Some(alias, CanonicalOperation(CanonicalOp.IndexOfStart(SqlCol(al2,col2), SqlCol(al3,col3)), col), intType typ)
                | _ -> None
            | t when Type.(=)(t, typeof<System.DateTime>) || Type.(=)(t, typeof<Option<System.DateTime>>) || Type.(=)(t, typeof<ValueOption<System.DateTime>>) -> // DateTime functions
                match meth.Name, par with
                | "AddYears", [Int x] -> Some(alias, CanonicalOperation(CanonicalOp.AddYears(SqlConstant(box x)), col), typ)
                | "AddYears", [SqlColumnGet(al2,col2,typ2) as pe] when integerTypes |> Seq.exists(fun t -> Type.(=)(pe.Type, t)) -> Some(alias, CanonicalOperation(CanonicalOp.AddYears(SqlCol(al2,col2)), col), typ)
                | "AddMonths", [Int x] -> Some(alias, CanonicalOperation(CanonicalOp.AddMonths(x), col), typ)
                | "AddDays", [Float x] -> Some(alias, CanonicalOperation(CanonicalOp.AddDays(SqlConstant(box x)), col), typ)
                | "AddDays", [OptionalConvertOrTypeAs(Int x)] -> Some(alias, CanonicalOperation(CanonicalOp.AddDays(SqlConstant(box x)), col), typ)
                | "AddDays", [OptionalConvertOrTypeAs(SqlColumnGet(al2,col2,typ2)) as pe] when integerTypes |> Seq.exists(fun t -> Type.(=)(pe.Type, t)) || decimalTypes |> Seq.exists(fun t -> Type.(=)(pe.Type, t))  -> Some(alias, CanonicalOperation(CanonicalOp.AddDays(SqlCol(al2,col2)), col), typ)
                | "AddHours", [Float x] -> Some(alias, CanonicalOperation(CanonicalOp.AddHours(x), col), typ)
                | "AddHours", [OptionalConvertOrTypeAs(Int x)] -> Some(alias, CanonicalOperation(CanonicalOp.AddHours(x), col), typ)
                | "AddMinutes", [Float x] -> Some(alias, CanonicalOperation(CanonicalOp.AddMinutes(SqlConstant(box x)), col), typ)
                | "AddMinutes", [OptionalConvertOrTypeAs(Int x)] -> Some(alias, CanonicalOperation(CanonicalOp.AddMinutes(SqlConstant(box x)), col), typ)
                | "AddMinutes", [OptionalConvertOrTypeAs(SqlColumnGet(al2,col2,typ2)) as pe] when integerTypes |> Seq.exists(fun t -> Type.(=)(pe.Type, t)) || decimalTypes |> Seq.exists(fun t -> Type.(=)(pe.Type, t))  -> Some(alias, CanonicalOperation(CanonicalOp.AddMinutes(SqlCol(al2,col2)), col), typ)
                | "AddSeconds", [Float x] -> Some(alias, CanonicalOperation(CanonicalOp.AddSeconds(x), col), typ)
                | "AddSeconds", [OptionalConvertOrTypeAs(Int x)] -> Some(alias, CanonicalOperation(CanonicalOp.AddSeconds(x), col), typ)
                | _ -> None
            | _ -> None
    // These are canonical properties
    | _, OptionalFSharpOptionValue(PropertyGet(Some(OptionalFSharpOptionValue(SqlColumnGet(alias, col, typ) as p1)), propInfo)) -> 
        match p1.Type with
        | t when Type.(=)(t, typeof<System.String>) || Type.(=)(t, typeof<Option<System.String>>) || Type.(=)(t, typeof<ValueOption<System.String>>)  -> // String functions
            match propInfo.Name with
            | "Length" -> Some(alias, CanonicalOperation(CanonicalOp.Length, col), intType typ)
            | _ -> None
        | t when Type.(=)(t, typeof<System.DateTime>) || Type.(=)(t, typeof<Option<System.DateTime>>) || Type.(=)(t, typeof<ValueOption<System.DateTime>>) -> // DateTime functions
            match propInfo.Name with
            | "Date" -> Some(alias, CanonicalOperation(CanonicalOp.Date, col), typ)
            | "Year" -> Some(alias, CanonicalOperation(CanonicalOp.Year, col), intType typ)
            | "Month" -> Some(alias, CanonicalOperation(CanonicalOp.Month, col), intType typ)
            | "Day" -> Some(alias, CanonicalOperation(CanonicalOp.Day, col), intType typ)
            | "Hour" -> Some(alias, CanonicalOperation(CanonicalOp.Hour, col), intType typ)
            | "Minute" -> Some(alias, CanonicalOperation(CanonicalOp.Minute, col), intType typ)
            | "Second" -> Some(alias, CanonicalOperation(CanonicalOp.Second, col), intType typ)
            | _ -> None
        | _ -> None 
    | _, OptionalFSharpOptionValue(PropertyGet(Some(MethodCall(Some(OptionalFSharpOptionValue(SqlColumnGet(alias, col, typ)) as p1), meth, [par])), propInfo)) 
            when (meth.Name = "Subtract" && (Type.(=)(meth.ReturnType, typeof<System.TimeSpan>) || Type.(=)(meth.ReturnType, typeof<Option<System.TimeSpan>>) || Type.(=)(meth.ReturnType, typeof<ValueOption<System.TimeSpan>>)) && 
                  (Type.(=)(p1.Type, typeof<System.DateTime>) || Type.(=)(p1.Type, typeof<Option<System.DateTime>>) || Type.(=)(p1.Type, typeof<ValueOption<System.DateTime>>))) -> 
        match propInfo.Name, par with
        | "Days", (SqlColumnGet(al2,col2,typ2) as pe) when (Type.(=)(pe.Type, typeof<System.DateTime>) || Type.(=)(pe.Type, typeof<Option<System.DateTime>>) || Type.(=)(pe.Type, typeof<ValueOption<System.DateTime>>)) -> Some(alias, CanonicalOperation(CanonicalOp.DateDiffDays(SqlCol(al2,col2)), col), typ)
        | "Seconds", (SqlColumnGet(al2,col2,typ2) as pe) when (Type.(=)(pe.Type, typeof<System.DateTime>) || Type.(=)(pe.Type, typeof<Option<System.DateTime>>) || Type.(=)(pe.Type, typeof<ValueOption<System.DateTime>>)) -> Some(alias, CanonicalOperation(CanonicalOp.DateDiffSecs(SqlCol(al2,col2)), col), typ)
        | "Days", Constant(c,t) when Type.(=)(t, typeof<System.DateTime>) -> Some(alias, CanonicalOperation(CanonicalOp.DateDiffDays(SqlConstant(box c)), col), typ)
        | "Seconds", Constant(c,t) when Type.(=)(t, typeof<System.DateTime>) -> Some(alias, CanonicalOperation(CanonicalOp.DateDiffSecs(SqlConstant(box c)), col), typ)
        | _ -> None

    // Numerical functions
    | _, OptionalFSharpOptionValue(MethodCallOrFSharpWrap(None, meth, ([OptionalFSharpOptionValue(OptionalConvertOrTypeAs(SqlColumnGet(alias, col, typ)) as pe)] as par)))
        when ((meth.Name = "Abs" || meth.Name = "Ceil" || meth.Name = "Floor" || meth.Name = "Round" || meth.Name = "Truncate" ||
               meth.Name = "Sqrt" || meth.Name = "Sin" || meth.Name = "Cos" || meth.Name = "Tan" || meth.Name = "ASin" || meth.Name = "ACos" || meth.Name = "ATan"
              ) && (decimalTypes |> Array.exists(fun t -> Type.(=)(pe.Type, t)))
            || (meth.Name = "Abs" && integerTypes |> Array.exists(fun t -> Type.(=)(pe.Type, t)))) -> 
            
            match meth.Name, par with
            | "Abs", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Abs, col), typ)
            | "Ceil", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Ceil, col), typ)
            | "Floor", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Floor, col), typ)
            | "Round", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Round, col), typ)
            | "Round", [_; Int decCount] -> Some(alias, CanonicalOperation(CanonicalOp.RoundDecimals(decCount), col), typ)
            | "Sqrt", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Sqrt, col), typ)
            | "Sin", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Sin, col), typ)
            | "Cos", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Cos, col), typ)
            | "Tan", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Tan, col), typ)
            | "ASin", [_] -> Some(alias, CanonicalOperation(CanonicalOp.ASin, col), typ)
            | "ACos", [_] -> Some(alias, CanonicalOperation(CanonicalOp.ACos, col), typ)
            | "ATan", [_] -> Some(alias, CanonicalOperation(CanonicalOp.ATan, col), typ)
            | "Truncate", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Truncate, col), typ)
            | _ -> failwith "Shouldn't hit"

    | _, OptionalFSharpOptionValue(MethodCall(None, meth, ([OptionalFSharpOptionValue(OptionalConvertOrTypeAs(SqlColumnGet(alias, col, typ)) as p1); par])))
        when (meth.Name = "Max" || meth.Name = "Min" || meth.Name = "Pow") -> 
            match meth.Name, par with
            | "Max", (OptionalConvertOrTypeAs(SqlColumnGet(al2,col2,typ2)) as pe) when Type.(=)(pe.Type, p1.Type) -> Some(alias, CanonicalOperation(CanonicalOp.Greatest(SqlCol(al2,col2)), col), typ)
            | "Min", (OptionalConvertOrTypeAs(SqlColumnGet(al2,col2,typ2)) as pe) when Type.(=)(pe.Type, p1.Type) -> Some(alias, CanonicalOperation(CanonicalOp.Least(SqlCol(al2,col2)), col), typ)
            | "Max", Constant(c,t) when t = p1.Type -> Some(alias, CanonicalOperation(CanonicalOp.Greatest(SqlConstant(c)), col), typ)
            | "Min", Constant(c,t) when t = p1.Type -> Some(alias, CanonicalOperation(CanonicalOp.Least(SqlConstant(c)), col), typ)
            | "Pow", (OptionalConvertOrTypeAs(SqlColumnGet(al2,col2,typ2)) as pe) when Type.(=)(pe.Type, p1.Type) -> Some(alias, CanonicalOperation(CanonicalOp.Pow(SqlCol(al2,col2)), col), typ)
            | "Pow", Constant(c,t) when t = p1.Type -> Some(alias, CanonicalOperation(CanonicalOp.Pow(SqlConstant(c)), col), typ)
            | _ -> None
    | _, OptionalFSharpOptionValue(MethodCall(None, meth, ([Constant(c,typ); OptionalFSharpOptionValue(OptionalConvertOrTypeAs(SqlColumnGet(alias, col, typ2)) as p1)])))
        when ((meth.Name = "Max" || meth.Name = "Min" || meth.Name = "Pow") && typ = p1.Type) -> 
            match meth.Name with
            | "Max" -> Some(alias, CanonicalOperation(CanonicalOp.Greatest(SqlConstant(c)), col), typ2)
            | "Min" -> Some(alias, CanonicalOperation(CanonicalOp.Least(SqlConstant(c)), col), typ2)
            | "Pow" -> Some(alias, CanonicalOperation(CanonicalOp.PowConst(SqlConstant(c)), col), typ2)
            | _ -> None

    // Basic math: (x.Column+1), (1+x.Column) and (x.Column1+y.Column2)
    | (ExpressionType.Add as op),      (:? BinaryExpression as be) 
    | (ExpressionType.Subtract as op), (:? BinaryExpression as be) 
    | (ExpressionType.Multiply as op), (:? BinaryExpression as be) 
    | (ExpressionType.Divide as op),   (:? BinaryExpression as be) 
    | (ExpressionType.Modulo as op),   (:? BinaryExpression as be) ->
        let operation =
            match op with
            | ExpressionType.Add -> "+"
            | ExpressionType.Subtract -> "-"
            | ExpressionType.Multiply -> "*"
            | ExpressionType.Divide -> "/"
            | ExpressionType.Modulo -> "%"
            | _ -> failwith ("Shouldn't hit " + op.ToString())

        if Type.(=)(be.Left.Type, typeof<System.DateTime>) || Type.(=)(be.Right.Type, typeof<System.DateTime>) || Type.(=)(be.Left.Type, typeof<Option<System.DateTime>>) || Type.(=)(be.Right.Type, typeof<Option<System.DateTime>>)
                || Type.(=)(be.Left.Type, typeof<ValueOption<System.DateTime>>) || Type.(=)(be.Right.Type, typeof<ValueOption<System.DateTime>>) then
            // DateTime math operations are not supported directly as they return .NET TimeSpan which is not the clear translation of SQL.
            // You can use functions like .AddHours(), .AddDays(), .Subtract().Days and comparison.
            None
        else

        match be.Left, be.Right with
        | OptionalConvertOrTypeAs(OptionalFSharpOptionValue(SqlColumnGet(alias, col, typ))) as p1, OptionalConvertOrTypeAs(Constant(constVal,constTyp)) 
            when (Type.(=)(p1.Type, constTyp) || (Common.Utilities.isOpt p1.Type && p1.Type.GenericTypeArguments.[0] = constTyp )
                 || Type.(=)(be.Left.Type, be.Right.Type)) ->  // Support only numeric and string math
                match p1.Type with
                | t when (operation = "+" && (Type.(=)(t, typeof<System.String>) || Type.(=)(t, typeof<System.Char>) || Type.(=)(t, typeof<Option<System.String>>) || Type.(=)(t, typeof<Option<System.Char>>) || Type.(=)(t, typeof<ValueOption<System.Char>>))) -> 
                    // Standard SQL string concatenation is ||
                    Some(alias, CanonicalOperation(CanonicalOp.BasicMath("||", constVal), col), typ)
                | t when (decimalTypes |> Seq.exists(fun tt -> Type.(=)(t, tt)) || integerTypes |> Seq.exists(fun tt -> Type.(=)(t, tt))) ->
                        Some(alias, CanonicalOperation(CanonicalOp.BasicMath(operation, constVal), col), typ)
                | _ -> None
        | OptionalConvertOrTypeAs(Constant(constVal,constTyp)), (OptionalConvertOrTypeAs(OptionalFSharpOptionValue(SqlColumnGet(alias, col, typ))) as p1)
            when (Type.(=)(p1.Type, constTyp) || (Common.Utilities.isOpt p1.Type && p1.Type.GenericTypeArguments.[0] = constTyp )
                 || Type.(=)(be.Left.Type, be.Right.Type)) ->  // Support only numeric and string math
                match p1.Type with
                | t when (operation = "+" && (Type.(=)(t, typeof<System.String>) || Type.(=)(t, typeof<System.Char>) || Type.(=)(t, typeof<Option<System.String>>) || Type.(=)(t, typeof<Option<System.Char>>) || Type.(=)(t, typeof<ValueOption<System.String>>) || Type.(=)(t, typeof<ValueOption<System.Char>>))) -> 
                    // Standard SQL string concatenation is ||
                    Some(alias, CanonicalOperation(CanonicalOp.BasicMath("||", constVal), col), typ)
                | t when (decimalTypes |> Seq.exists(fun tt -> Type.(=)(t, tt)) || integerTypes |> Seq.exists(fun tt -> Type.(=)(t, tt))) ->
                        Some(alias, CanonicalOperation(CanonicalOp.BasicMathLeft(operation, constVal), col), typ)
                | _ -> None
        | OptionalConvertOrTypeAs(OptionalFSharpOptionValue(SqlColumnGet(aliasLeft, colLeft, typLeft))) as p1, (OptionalConvertOrTypeAs(OptionalFSharpOptionValue(SqlColumnGet(aliasRight, colRight, typRight))) as p2) 
            when (Type.(=)(p1.Type, p2.Type) ||
                     (Common.Utilities.isOpt p1.Type && p1.Type.GenericTypeArguments.[0] = p2.Type ) ||
                     (Common.Utilities.isOpt p2.Type && p2.Type.GenericTypeArguments.[0] = p1.Type ) ||
                         Type.(=)(be.Left.Type, be.Right.Type)) -> 
                let opFix = 
                    match p1.Type with
                    | t when ((Type.(=)(t, typeof<System.String>) || Type.(=)(t, typeof<System.Char>) || Type.(=)(t, typeof<Option<System.String>>) || Type.(=)(t, typeof<Option<System.Char>>) || Type.(=)(t, typeof<ValueOption<System.String>>) || Type.(=)(t, typeof<ValueOption<System.Char>>)) && operation = "+") -> "||"
                    | _ -> operation
                Some(aliasLeft, CanonicalOperation(CanonicalOp.BasicMathOfColumns(opFix, aliasRight, colRight), colLeft), typLeft)
        | _ -> None
    //

    | ExpressionType.Conditional, (:? ConditionalExpression as ce) ->

        let rec filterExpression (exp:Expression)  =
            let extendFilter conditions nextFilter =
                match exp with
                | AndAlso(_) -> And(conditions,nextFilter)
                | OrElse(_) -> Or(conditions,nextFilter)
                | _ -> failwith ("Filter problem: " + exp.ToString())
            match exp with
            | AndAlsoOrElse(AndAlsoOrElse(_) as left, (AndAlsoOrElse(_) as right)) ->
                extendFilter [] (Some ([filterExpression left; filterExpression right]))
            | AndAlsoOrElse(AndAlsoOrElse(_) as left,SimpleCondition(c))  ->
                extendFilter [c] (Some ([filterExpression left]))
            | AndAlsoOrElse(SimpleCondition(c),(AndAlsoOrElse(_) as right))  ->
                extendFilter [c] (Some ([filterExpression right]))
            | AndAlsoOrElse(SimpleCondition(c1) as cc1 ,SimpleCondition(c2)) as cc2 ->
                if cc1 = cc2 then extendFilter [c1] None
                else extendFilter [c1;c2] None
            | SimpleCondition(cond) ->
                Condition.And([cond],None)

            // Support for simple boolean expressions:
            | AndAlso(Bool(b), x) | AndAlso(x, Bool(b)) when b -> filterExpression x
            | OrElse(Bool(b), x) | OrElse(x, Bool(b)) when not b -> filterExpression x
            | Bool(b) when b -> Condition.ConstantTrue
            | Bool(b) when not b -> Condition.ConstantFalse
            | _ -> Condition.NotSupported exp

        let filter = filterExpression (ExpressionOptimizer.visit ce.Test)

        match filter, ce.IfTrue, ce.IfFalse with
        | Condition.NotSupported(x), _, _ -> None
        | Condition.ConstantTrue, OptionalConvertOrTypeAs(SqlColumnGet(alias,col,typ)), _ -> Some(alias, col, typ)
        | Condition.ConstantFalse, _, OptionalConvertOrTypeAs(SqlColumnGet(alias,col,typ)) -> Some(alias, col, typ)
        | _, OptionalConvertOrTypeAs(SqlColumnGet(al2,col2,typ2)), Constant(c, ct)  -> Some(al2, CanonicalOperation(CanonicalOp.CaseSql(filter, SqlConstant(c)), col2), typ2)
        | _, OptionalConvertOrTypeAs(SqlColumnGet(al2,col2,typ2)), OptionalConvertOrTypeAs(SqlColumnGet(al3,col3,typ3)) -> Some(al2, CanonicalOperation(CanonicalOp.CaseSql(filter, SqlCol(al3,col3)), col2), typ2)
        | _, Constant(c, ct), OptionalConvertOrTypeAs(SqlColumnGet(al2,col2,typ2)) -> Some(al2, CanonicalOperation(CanonicalOp.CaseNotSql(filter, SqlConstant(c)), col2), typ2)

        | _, Constant(c1, ct1), Constant(c2, ct2) when ct1=ct2 -> Some("", CanonicalOperation(CanonicalOp.CaseSqlPlain(filter, c1, c2), SqlColumnType.KeyColumn("constant")), ct1)

        | _ -> None

    | ExpressionType.Call, (:? MethodCallExpression as e) when e.Method.Name = "Parse" && e.Arguments.Count = 1 && 
                           (Type.(=)(e.Type, typeof<System.DateTime>) || Type.(=)(e.Type, typeof<Option<System.DateTime>>) || Type.(=)(e.Type, typeof<ValueOption<System.DateTime>>)) ->
        // Don't do any magic, just: DateTime.Parse('2000-01-01') -> '2000-01-01'
        match e.Arguments.[0] with
        | SqlColumnGet(alias, col, typ) when Type.(=)(typ, typeof<String>) || Type.(=)(typ, typeof<Option<String>>) || Type.(=)(typ, typeof<ValueOption<String>>) 
            -> Some(alias, col, e.Type)
        | _ -> None
    | ExpressionType.Call, (:? MethodCallExpression as e) when e.Method.Name = "Parse" && e.Arguments.Count = 1 && 
                           (Type.(=)(e.Type, typeof<System.Int32>) || Type.(=)(e.Type, typeof<Option<System.Int32>>) || Type.(=)(e.Type, typeof<ValueOption<System.Int32>>)) ->
        match e.Arguments.[0] with
        | SqlColumnGet(alias, col, typ) when Type.(=)(typ, typeof<String>) || Type.(=)(typ, typeof<Option<String>>) || Type.(=)(typ, typeof<ValueOption<String>>) 
            -> Some(alias, CanonicalOperation(CanonicalOp.CastInt, col), typ)
        | _ -> None
    | _, SqlSubtableColumnGet(alias,key,typ) -> Some(alias, key, typ)
    | _ -> None

and (|SqlNegativeBooleanColumn|_|) (e:Expression) = 
    match e.NodeType, e with
    | ExpressionType.Not, (:? UnaryExpression as ue) ->
        match ue.Operand with
        | SqlColumnGet(ti,key,ret) when ue.Operand.Type.FullName = "System.Boolean" || Type.(=)(ret, typeof<bool>) -> 
            Some(ti,key,ConditionOperator.Equal, Some(false |> box))
        | _ -> None
    | _ -> None

//Simpler version of where Condition-pattern, used on case-when-clause
and (|SimpleCondition|_|) exp =
    let extractProperty op (meth:Expression) ti key =
        let invokation = 
            match meth.NodeType, meth with
            | ExpressionType.Constant, (:? ConstantExpression as ce) -> Some ce.Value
            | ExpressionType.MemberAccess, (:? MemberExpression as me) when (me.Expression :? ConstantExpression) ->
                let ceVal = (me.Expression :?> ConstantExpression).Value
                let myVal = 
                    match me.Member with
                    | :? FieldInfo as fieldInfo when not(isNull(fieldInfo)) ->
                        fieldInfo.GetValue ceVal
                    | :? PropertyInfo as propInfo when not(isNull(propInfo)) ->
                        propInfo.GetValue(ceVal, null)
                    | _ -> ceVal
                Some myVal
            | ExpressionType.Call, (:? MethodCallExpression as e) when e.Method.ReturnType.IsClass -> Some((Expression.Lambda(meth).Compile() :?> Func<_>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<Int32>) -> Some((Expression.Lambda(meth).Compile() :?> Func<Int32>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<Int64>) -> Some((Expression.Lambda(meth).Compile() :?> Func<Int64>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<Decimal>) -> Some((Expression.Lambda(meth).Compile() :?> Func<Decimal>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<DateTime>) -> Some((Expression.Lambda(meth).Compile() :?> Func<DateTime>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<Guid>) -> Some((Expression.Lambda(meth).Compile() :?> Func<Guid>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<Boolean>) -> Some((Expression.Lambda(meth).Compile() :?> Func<Boolean>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<Single>) -> Some((Expression.Lambda(meth).Compile() :?> Func<Single>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<Int16>) -> Some((Expression.Lambda(meth).Compile() :?> Func<Int16>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<UInt32>) -> Some((Expression.Lambda(meth).Compile() :?> Func<UInt32>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<UInt16>) -> Some((Expression.Lambda(meth).Compile() :?> Func<UInt16>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<UInt64>) -> Some((Expression.Lambda(meth).Compile() :?> Func<UInt64>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<Byte>) -> Some((Expression.Lambda(meth).Compile() :?> Func<Byte>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<Char>) -> Some((Expression.Lambda(meth).Compile() :?> Func<Char>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<DateTimeOffset>) -> Some((Expression.Lambda(meth).Compile() :?> Func<DateTimeOffset>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<TimeSpan>) -> Some((Expression.Lambda(meth).Compile() :?> Func<TimeSpan>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<bigint>) -> Some((Expression.Lambda(meth).Compile() :?> Func<bigint>).Invoke() |> box)

            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<ValueOption<Int32>>) -> Some((Expression.Lambda(meth).Compile() :?> Func<ValueOption<Int32>>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<ValueOption<Int64>>) -> Some((Expression.Lambda(meth).Compile() :?> Func<ValueOption<Int64>>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<ValueOption<Decimal>>) -> Some((Expression.Lambda(meth).Compile() :?> Func<ValueOption<Decimal>>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<ValueOption<DateTime>>) -> Some((Expression.Lambda(meth).Compile() :?> Func<ValueOption<DateTime>>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<ValueOption<Guid>>) -> Some((Expression.Lambda(meth).Compile() :?> Func<ValueOption<Guid>>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<ValueOption<Boolean>>) -> Some((Expression.Lambda(meth).Compile() :?> Func<ValueOption<Boolean>>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<ValueOption<Single>>) -> Some((Expression.Lambda(meth).Compile() :?> Func<ValueOption<Single>>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<ValueOption<Int16>>) -> Some((Expression.Lambda(meth).Compile() :?> Func<ValueOption<Int16>>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<ValueOption<UInt32>>) -> Some((Expression.Lambda(meth).Compile() :?> Func<ValueOption<UInt32>>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<ValueOption<UInt16>>) -> Some((Expression.Lambda(meth).Compile() :?> Func<ValueOption<UInt16>>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<ValueOption<UInt64>>) -> Some((Expression.Lambda(meth).Compile() :?> Func<ValueOption<UInt64>>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<ValueOption<Byte>>) -> Some((Expression.Lambda(meth).Compile() :?> Func<ValueOption<Byte>>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<ValueOption<Char>>) -> Some((Expression.Lambda(meth).Compile() :?> Func<ValueOption<Char>>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<ValueOption<DateTimeOffset>>) -> Some((Expression.Lambda(meth).Compile() :?> Func<ValueOption<DateTimeOffset>>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<ValueOption<TimeSpan>>) -> Some((Expression.Lambda(meth).Compile() :?> Func<ValueOption<TimeSpan>>).Invoke() |> box)
            | ExpressionType.Call, (:? MethodCallExpression as e) when Type.(=)(e.Method.ReturnType, typeof<ValueOption<bigint>>) -> Some((Expression.Lambda(meth).Compile() :?> Func<ValueOption<bigint>>).Invoke() |> box)
            | ExpressionType.MemberAccess, (:? MemberExpression as me) when Type.(=)(me.Type, typeof<DateTime>) && me.Member.Name = "Now" -> Some(DateTime.Now |> box)
            | ExpressionType.MemberAccess, (:? MemberExpression as me) when Type.(=)(me.Type, typeof<DateTime>) && me.Member.Name = "UtcNow" -> Some(DateTime.UtcNow |> box)
            | _ ->
                // In case user feeds us incomplete lambda, we will not execute: the user might have a context needed for the compilation.
                try
                    Some(Expression.Lambda(meth).Compile().DynamicInvoke())
                with err -> None
        match invokation with
        | None -> None
        | Some invokedResult -> 

            let handleNullCompare() =
                match op with
                | ConditionOperator.Equal -> Some(ti,key,ConditionOperator.IsNull,None)
                | ConditionOperator.NotEqual -> Some(ti,key,ConditionOperator.NotNull,None)
                | _ -> Some(ti,key,op,Some(invokedResult))

            if isNull invokedResult then handleNullCompare()
            else
            let retType = invokedResult.GetType()
            if Common.Utilities.isOpt retType then
                let gotVal = retType.GetProperty("Value") // Option type Some should not be SQL-parameter.
                match gotVal.GetValue(invokedResult, [||]) with
                | null -> handleNullCompare()
                | r -> Some(ti,key,op,Some(r))
            else Some(ti,key,op,Some(invokedResult))
    let swapOp = function // Because we serialize db column first, these have to revert (#553)
        | ConditionOperator.LessThan -> ConditionOperator.GreaterThan
        | ConditionOperator.LessEqual -> ConditionOperator.GreaterEqual
        | ConditionOperator.GreaterThan -> ConditionOperator.LessThan
        | ConditionOperator.GreaterEqual -> ConditionOperator.LessEqual
        | x -> x
    match exp with
    | SqlSpecialOpArr(ti,op,key,value)
    | SqlSpecialNegativeOpArr(ti,op,key,value) ->
        Some(ti,key,op,Some (box value))
    | SqlSpecialOp(ti,op,key,value) ->
        Some(ti,key,op,Some value)
    // if using nullable types
    | OptionIsSome(SqlColumnGet(ti,key,_)) ->
        Some(ti,key,ConditionOperator.NotNull,None)
    | OptionIsNone(SqlColumnGet(ti,key,_))
    | SqlCondOp(ConditionOperator.Equal,(OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_))), (OptionNone | NullConstant)) 
    | SqlNegativeCondOp(ConditionOperator.Equal,(OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_))),(OptionNone | NullConstant)) ->
        Some(ti,key,ConditionOperator.IsNull,None)
    | SqlCondOp(ConditionOperator.NotEqual,(OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_))),(OptionNone | NullConstant)) 
    | SqlNegativeCondOp(ConditionOperator.NotEqual,(OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_))),(OptionNone | NullConstant)) ->
        Some(ti,key,ConditionOperator.NotNull,None)
    // matches column to constant with any operator eg c.name = "john", c.age > 42
    | SqlCondOp(op,(OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_))),OptionalConvertOrTypeAs(OptionalFSharpOptionValue(ConstantOrNullableConstant(c)))) 
    | SqlNegativeCondOp(op,(OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_))),OptionalConvertOrTypeAs(OptionalFSharpOptionValue(ConstantOrNullableConstant(c)))) ->
        Some(ti,key,op,c)
    | SqlCondOp(op,OptionalConvertOrTypeAs(OptionalFSharpOptionValue(ConstantOrNullableConstant(c))),(OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_)))) 
    | SqlNegativeCondOp(op,OptionalConvertOrTypeAs(OptionalFSharpOptionValue(ConstantOrNullableConstant(c))),(OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_)))) ->
        Some(ti,key,(swapOp op),c)
    // matches column to column e.g. c.col1 > c.col2
    | SqlCondOp(op,(OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_))),(OptionalConvertOrTypeAs(SqlColumnGet(ti2,key2,_)))) 
    | SqlNegativeCondOp(op,(OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_))),(OptionalConvertOrTypeAs(SqlColumnGet(ti2,key2,_)))) ->
        Some(ti,key,op,Some((ti2,key2) |> box))
    // matches to another property getter, method call or new expression
    | SqlCondOp(op,OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_)),OptionalConvertOrTypeAs(OptionalFSharpOptionValue((((:? MemberExpression) | (:? MethodCallExpression) | (:? NewExpression)) as meth))))
    | SqlNegativeCondOp(op,OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_)),OptionalConvertOrTypeAs(OptionalFSharpOptionValue((((:? MemberExpression) | (:? MethodCallExpression) | (:? NewExpression)) as meth)))) ->
        extractProperty op meth ti key
    | SqlCondOp(op,OptionalConvertOrTypeAs(OptionalFSharpOptionValue((((:? MemberExpression) | (:? MethodCallExpression) | (:? NewExpression)) as meth))),OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_)))
    | SqlNegativeCondOp(op,OptionalConvertOrTypeAs(OptionalFSharpOptionValue((((:? MemberExpression) | (:? MethodCallExpression) | (:? NewExpression)) as meth))),OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_))) ->
        extractProperty (swapOp op) meth ti key
    | SqlColumnGet(ti,key,ret) when exp.Type.FullName = "System.Boolean" || Type.(=)(ret, typeof<bool>) -> 
        Some(ti,key,ConditionOperator.Equal, Some(true |> box))
    | SqlNegativeBooleanColumn(ti,key,e,v) -> 
        Some(ti,key,e, v)
    | _ -> None

and (|TupleSqlColumnsGet|_|) = function 
    | OptionalFSharpOptionValue(NewExpr(cons, args)) when cons.DeclaringType.Name.StartsWith("Tuple") || cons.DeclaringType.Name.StartsWith("AnonymousObject") ->
        let items = args |> List.choose(function
                            | SqlColumnGet(ti,key,t) -> Some(ti, key, t)
                            | _-> None)
        match items with
        | [] -> None
        | li -> Some li
    | _ -> None

and (|SqlSpecialOpArr|_|) = function
    // for some crazy reason, simply using (|=|) stopped working ??
    | MethodCall(None,MethodWithName("op_BarEqualsBar"), [SqlColumnGet(ti,key,_); SeqValues values]) -> Some(ti, ConditionOperator.In, key, values)
    | MethodCall(None,MethodWithName("op_BarLessGreaterBar"),[SqlColumnGet(ti,key,_); SeqValues values]) -> Some(ti, ConditionOperator.NotIn, key, values)
    | MethodCall(None,MethodWithName("Contains"), [SeqValues values; SqlColumnGet(ti,key,_)]) -> Some(ti, ConditionOperator.In, key, values)
    | MethodCall(Some((SeqValues values) as setVals),MethodWithName("Contains"), [SqlColumnGet(ti,key,_)]) when setVals.Type.IsGenericType -> Some(ti, ConditionOperator.In, key, values)
    | _ -> None

and (|SqlSpecialOpArrQueryable|_|) = function
    // for some crazy reason, simply using (|=|) stopped working ??
    | MethodCall(None,MethodWithName("op_BarEqualsBar"), [SqlColumnGet(ti,key,_); SeqValuesQueryable values]) -> Some(ti, ConditionOperator.NestedIn, key, values)
    | MethodCall(None,MethodWithName("op_BarLessGreaterBar"),[SqlColumnGet(ti,key,_); SeqValuesQueryable values]) -> Some(ti, ConditionOperator.NestedNotIn, key, values)
    | MethodCall(None,MethodWithName("Contains"), [SeqValuesQueryable values; SqlColumnGet(ti,key,_)]) -> Some(ti, ConditionOperator.NestedIn, key, values)
    | _ -> None
    
and (|SqlSpecialOp|_|) e =
    match e with
    | MethodCall(None,MethodWithName("op_EqualsPercent"), [SqlColumnGet(ti,key,_); right]) -> Some(ti,ConditionOperator.Like,   key,getRightFromOp right)
    | MethodCall(None,MethodWithName("op_LessGreaterPercent"),[SqlColumnGet(ti,key,_); right]) -> Some(ti,ConditionOperator.NotLike,key,getRightFromOp right)
    // String  methods
    | MethodCall(Some(OptionalFSharpOptionValue(SqlColumnGet(ti,key,t))), MethodWithName "Contains", [right]) when Type.(=)(t, typeof<string>) || Type.(=)(t, typeof<Option<string>>) || Type.(=)(t, typeof<ValueOption<string>>) -> 
        Some(ti,ConditionOperator.Like,key,box (sprintf "%%%O%%" (getRightFromOp right)))
    | MethodCall(Some(OptionalFSharpOptionValue(SqlColumnGet(ti,key,t))), MethodWithName "StartsWith", [right]) when Type.(=)(t, typeof<string>) || Type.(=)(t, typeof<Option<string>>) || Type.(=)(t, typeof<ValueOption<string>>) -> 
        Some(ti,ConditionOperator.Like,key,box (sprintf "%O%%" (getRightFromOp right)))
    | MethodCall(Some(OptionalFSharpOptionValue(SqlColumnGet(ti,key,t))), MethodWithName "EndsWith", [right]) when Type.(=)(t, typeof<string>) || Type.(=)(t, typeof<Option<string>>) || Type.(=)(t, typeof<ValueOption<string>>) -> 
        Some(ti,ConditionOperator.Like,key,box (sprintf "%%%O" (getRightFromOp right)))
    
    // not (String  methods)
    | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Not -> 
        match ue.Operand with
        | MethodCall(Some(OptionalFSharpOptionValue(SqlColumnGet(ti,key,t))), MethodWithName "Contains", [right]) when Type.(=)(t, typeof<string>) || Type.(=)(t, typeof<Option<string>>) || Type.(=)(t, typeof<ValueOption<string>>) -> 
            Some(ti,ConditionOperator.NotLike,key,box (sprintf "%%%O%%" (getRightFromOp right)))
        | MethodCall(Some(OptionalFSharpOptionValue(SqlColumnGet(ti,key,t))), MethodWithName "StartsWith", [right]) when Type.(=)(t, typeof<string>) || Type.(=)(t, typeof<Option<string>>) || Type.(=)(t, typeof<ValueOption<string>>) -> 
            Some(ti,ConditionOperator.NotLike,key,box (sprintf "%O%%" (getRightFromOp right)))
        | MethodCall(Some(OptionalFSharpOptionValue(SqlColumnGet(ti,key,t))), MethodWithName "EndsWith", [right]) when Type.(=)(t, typeof<string>) || Type.(=)(t, typeof<Option<string>>) || Type.(=)(t, typeof<ValueOption<string>>) -> 
            Some(ti,ConditionOperator.NotLike,key,box (sprintf "%%%O" (getRightFromOp right)))
        | _ -> None
    | _ -> None
                

and (|SqlSpecialNegativeOpArr|_|) (e:Expression) = 
    match e.NodeType, e with
    | ExpressionType.Not, (:? UnaryExpression as ue) ->
        match ue.Operand with
        | MethodCall(None,MethodWithName("Contains"), [SeqValues values; SqlColumnGet(ti,key,_)]) -> Some(ti, ConditionOperator.NotIn, key, values)
        | MethodCall(Some((SeqValues values) as setVals),MethodWithName("Contains"), [SqlColumnGet(ti,key,_)]) when setVals.Type.IsGenericType -> Some(ti, ConditionOperator.NotIn, key, values)
        | _ -> None
    | _ -> None

and (|SqlSpecialNegativeOpArrQueryable|_|) (e:Expression) = 
    match e.NodeType, e with
    | ExpressionType.Not, (:? UnaryExpression as ue) ->
        match ue.Operand with
        | MethodCall(None,MethodWithName("Contains"), [SeqValuesQueryable values; SqlColumnGet(ti,key,_)]) -> Some(ti, ConditionOperator.NestedNotIn, key, values)
        | _ -> None
    | _ -> None

and (|SqlExistsClause|_|) = function 
    | MethodCall(None, (MethodWithName "Any" as meth), [ SeqValuesQueryable src; OptionalQuote qual ]) ->
        Some(meth, ConditionOperator.NestedExists, src, qual)
    | _ -> None

and (|SqlNotExistsClause|_|) (e:Expression) = 
    match e.NodeType, e with
    | ExpressionType.Not, (:? UnaryExpression as ue) ->
        match ue.Operand with
        | MethodCall(None, (MethodWithName "Any" as meth), [ SeqValuesQueryable src; OptionalQuote qual ]) -> Some(meth, ConditionOperator.NestedNotExists, src, qual)
        | _ -> None
    | _ -> None
