module internal FSharp.Data.Sql.Patterns

open System
open System.Linq.Expressions
open System.Reflection
open FSharp.Data.Sql
open FSharp.Data.Sql.Schema

/// Marker interface
type ISqlQueryable = interface end

/// Reflection raises a member's exception wrapped in TargetInvocationException; compiled code raises it as it is.
let private unwrapInvocation (read: unit -> obj) =
    try read ()
    with :? TargetInvocationException as ex when not (isNull ex.InnerException) ->
        Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException).Throw()
        null

/// A Convert that keeps the value as it is: a reference cast, boxing, unboxing to the same type or wrapping in
/// Nullable. A numeric, enum or user-defined conversion computes a new value, so it isn't one.
let private convertsAsIs (u:UnaryExpression) (value:obj) =
    isNull u.Method
    && (if isNull value then not (u.Type.IsValueType && isNull (Nullable.GetUnderlyingType u.Type))
        else u.Type.IsInstanceOfType value)

/// Value types whose methods don't change the value. Reflection calls a method on a boxed copy, which compiled code
/// doesn't, so a method of any other value type is left to the interpreter.
let private unchangedByMethods =
    Collections.Generic.HashSet<Type>([ typeof<DateTime>; typeof<DateTimeOffset>; typeof<TimeSpan>; typeof<Guid>; typeof<Decimal> ])

let private callableByReflection (mc:MethodCallExpression) =
    (mc.Method.IsStatic
     || not mc.Object.Type.IsValueType || mc.Object.Type.IsPrimitive || mc.Object.Type.IsEnum
     || unchangedByMethods.Contains mc.Object.Type)
    && mc.Method.GetParameters() |> Array.forall (fun p -> not p.ParameterType.IsByRef)

/// Finds a parameter that none of the expression's own lambdas, blocks or catch blocks declares.
type private OuterParameterFinder() =
    inherit ExpressionVisitor()
    let declared = Collections.Generic.HashSet<ParameterExpression>()
    let used = Collections.Generic.HashSet<ParameterExpression>()
    member _.Found = not (used.IsSubsetOf declared)
    override _.Visit (e:Expression) =
        match e with
        | :? LambdaExpression as l -> declared.UnionWith l.Parameters
        | :? BlockExpression as b -> declared.UnionWith b.Variables
        | :? TryExpression as t -> for h in t.Handlers do if not (isNull h.Variable) then declared.Add h.Variable |> ignore
        | :? ParameterExpression as p -> used.Add p |> ignore
        | _ -> ()
        base.Visit e

/// Whether readValue reads the expression: a constant, a field or property of a readable value or a static one, a
/// method call on readable values, a Convert or TypeAs of one, and a Quote that uses no outer lambda's parameter.
/// The answer comes from the expression's shape, so nothing has been evaluated when it is false.
let rec private isReadable (e:Expression) =
    match e.NodeType, e with
    | ExpressionType.Constant, _ -> true
    | ExpressionType.MemberAccess, (:? MemberExpression as me) -> isNull me.Expression || isReadable me.Expression
    | ExpressionType.Call, (:? MethodCallExpression as mc) ->
        callableByReflection mc && (isNull mc.Object || isReadable mc.Object) && Seq.forall isReadable mc.Arguments
    | (ExpressionType.Convert | ExpressionType.ConvertChecked | ExpressionType.TypeAs), (:? UnaryExpression as ue) ->
        isReadable ue.Operand
    | ExpressionType.Quote, (:? UnaryExpression as ue) ->
        let finder = OuterParameterFinder()
        finder.Visit ue.Operand |> ignore
        not finder.Found
    | _ -> false

#if NETSTANDARD2_0
/// LambdaExpression.Compile(preferInterpretation), or null where the runtime doesn't have it: the netstandard2.0
/// build also runs on .NET Framework, which has the method only from 4.7.1.
let private compilePreferringInterpretation : Func<LambdaExpression, bool, Delegate> =
    match typeof<LambdaExpression>.GetMethod("Compile", [| typeof<bool> |]) with
    | null -> null
    | compile -> Delegate.CreateDelegate(typeof<Func<LambdaExpression, bool, Delegate>>, compile) :?> Func<LambdaExpression, bool, Delegate>
#endif

/// Runs a parameterless expression interpreted, as compiling it would JIT code that runs once.
let private interpret (e:Expression) : obj =
    let body =
        if Type.(=)(e.Type, typeof<Void>) then Expression.Block(e, Expression.Constant(null)) :> Expression
        else Expression.Convert(e, typeof<obj>) :> Expression
    let lambda = Expression.Lambda<Func<obj>> body
#if NET461 || NET462
    lambda.Compile().Invoke()
#else
#if NETSTANDARD2_0
    if isNull compilePreferringInterpretation then lambda.Compile().Invoke()
    else (compilePreferringInterpretation.Invoke(lambda, true) :?> Func<obj>).Invoke()
#else
    lambda.Compile(preferInterpretation = true).Invoke()
#endif
#endif

/// The value of an expression isReadable accepts, each part of it evaluated once. Where reflection doesn't answer as
/// compiled code does (a member of null or of a Nullable, a conversion that computes a new value), the node is
/// interpreted over its operand's value.
let rec private readValue (e:Expression) : obj =
    match e.NodeType, e with
    | ExpressionType.MemberAccess, (:? MemberExpression as me) ->
        let owner = if isNull me.Expression then null else readValue me.Expression
        if not (isNull me.Expression) && (isNull owner || not (isNull (Nullable.GetUnderlyingType me.Expression.Type))) then
            interpret (me.Update(Expression.Constant(owner, me.Expression.Type)))
        else
            match me.Member with
            | :? FieldInfo as f -> f.GetValue owner
            | m -> unwrapInvocation (fun () -> (m :?> PropertyInfo).GetValue(owner, null))
    | ExpressionType.Call, (:? MethodCallExpression as mc) ->
        let owner = if isNull mc.Object then null else readValue mc.Object
        let args = Array.zeroCreate mc.Arguments.Count
        for i in 0 .. args.Length - 1 do
            args.[i] <- readValue mc.Arguments.[i]
        if isNull owner && not mc.Method.IsStatic then raise (NullReferenceException())
        unwrapInvocation (fun () -> mc.Method.Invoke(owner, args))
    | (ExpressionType.Convert | ExpressionType.ConvertChecked), (:? UnaryExpression as ue) ->
        let value = readValue ue.Operand
        if convertsAsIs ue value then value
        else interpret (ue.Update(Expression.Constant(value, ue.Operand.Type)))
    | ExpressionType.TypeAs, (:? UnaryExpression as ue) ->
        let value = readValue ue.Operand
        if isNull value || ue.Type.IsInstanceOfType value then value else null
    | ExpressionType.Quote, (:? UnaryExpression as ue) -> box ue.Operand
    | _ -> (e :?> ConstantExpression).Value

/// The value of a parameterless expression when isReadable accepts it.
let private tryReadValue (e:Expression) : obj voption =
    if isReadable e then ValueSome (readValue e) else ValueNone

/// The value of a parameterless expression, evaluated once: read by reflection where isReadable accepts it,
/// interpreted otherwise.
let internal evaluateExpression (e:Expression) : obj =
    if isReadable e then readValue e else interpret e

[<return: Struct>]
let inline (|MethodWithName|_|)   (s:string) (m:MethodInfo)   = if String.Equals(m.Name, s, StringComparison.Ordinal) then ValueSome () else ValueNone
[<return: Struct>]
let inline (|PropertyWithName|_|) (s:string) (m:PropertyInfo) = if String.Equals(m.Name, s, StringComparison.Ordinal) then ValueSome () else ValueNone

[<return: Struct>]
let inline (|MemberAccess|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.MemberAccess, ( :? MemberExpression as me) -> ValueSome me
    | _ -> ValueNone

[<return: Struct>]
let inline (|MethodCall|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Call, (:? MethodCallExpression as e) -> 
        ValueSome ((match e.Object with null -> None | obj -> Some obj), e.Method, Seq.toList e.Arguments)
    | _ -> ValueNone

[<return: Struct>]
let inline (|NewExpr|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.New , (:? NewExpression as e) -> 
        ValueSome (e.Constructor, Seq.toList e.Arguments)
    | _ -> ValueNone

[<return: Struct>]
let (|Constant|_|) (exp:Expression) =
    let e = ExpressionOptimizer.doReduction exp
    match e.NodeType, e with 
    | ExpressionType.Constant, (:? ConstantExpression as ce) -> ValueSome (ce.Value, ce.Type)
    | _ -> ValueNone

let rec (|OptionalConvertOrTypeAs|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Convert, (:? UnaryExpression as ue ) 
    | ExpressionType.TypeAs, (:? UnaryExpression as ue ) -> 
        match ue.Operand with OptionalConvertOrTypeAs x -> x
    | ExpressionType.Call, (:? MethodCallExpression as e) when e.Method.Name = "Parse" && e.Arguments.Count = 1 ->
        // Don't do any magic, just: DateTime.Parse('2000-01-01') -> '2000-01-01'
        e.Arguments.[0]
    | ExpressionType.Call, (:? MethodCallExpression as e) when e.Method.Name = "Box" && e.Arguments.Count = 1 && e.Arguments.[0].Type.IsValueType ->
        e.Arguments.[0]
    | ExpressionType.Call, (:? MethodCallExpression as e) when isNull e.Object && e.Method.Name = "ToString" && e.Arguments.Count = 1 ->
        e.Arguments.[0]
    | _ -> e

[<return: Struct>]
let (|SeqValuesQueryable|_|) (e:Expression) =
    let rec isQueryable (ty : Type) = 
        ty.FindInterfaces((fun ty _ -> Type.(=)(ty, typeof<System.Linq.IQueryable>)), null)
        |> (not << Seq.isEmpty)

    match (isQueryable e.Type) with
    | false -> ValueNone
    | true ->
        match e.NodeType, e with
        | ExpressionType.Constant, (:? ConstantExpression as ce) ->
            match ce.Value with
            | :? ISqlQueryable -> ValueSome (ce.Value :?> System.Linq.IQueryable)
            | _ -> ValueNone
        | _ ->
            match evaluateExpression e with
            | :? ISqlQueryable as values -> ValueSome (values :?> System.Linq.IQueryable)
            | _ -> ValueNone

[<return: Struct>]
let (|SeqValues|_|) (e:Expression) =
    if e.Type.FullName = "System.String" then ValueNone // String is char[] but we don't want to hit that!
    else
    let rec isEnumerable (ty : Type) = 
        ty.FindInterfaces((fun ty _ -> Type.(=)(ty, typeof<System.Collections.IEnumerable>)), null)
        |> (not << Seq.isEmpty)

    match (isEnumerable e.Type) with
    | false -> ValueNone
    | true ->
        // Here, if e is SQLQueryable<_>, we could avoid execution and nest the queries like
        // select x from xs where x in (select y from ys)
        // ...but instead we just execute the sub-query. Works when sub-query results are small.

        let values = evaluateExpression e :?> System.Collections.IEnumerable

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
        let array = Array.CreateInstance(typeof<obj>, count)
        let mutable i = 0
        for obj in values do
            array.SetValue(obj, i)
            i <- i + 1
        // Return the array
        ValueSome array


[<return: Struct>]
let (|PropertyGet|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.MemberAccess, ( :? MemberExpression as e) -> 
        match e.Member with 
        | :? PropertyInfo as p -> ValueSome ((match e.Expression with null -> None | obj -> Some obj), p)
        | _ -> ValueNone
    | _ -> ValueNone

[<return: Struct>]
let (|ConvertOrTypeAs|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Convert, (:? UnaryExpression as ue ) 
    | ExpressionType.TypeAs, (:? UnaryExpression as ue ) -> ValueSome ue.Operand
    | _ -> ValueNone

[<return: Struct>]
let inline (|OptionNone|_|) (e: Expression) =
    match e with
    | MethodCall(None,MethodWithName "get_None",[]) ->
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
            | null -> Some(Some ce.Value)
            | optVal -> Some(Some optVal)
        else
            Some(Some ce.Value)
    | ExpressionType.Convert, (:? UnaryExpression as ue ) -> 
        match ue.Operand.NodeType, ue.Operand with
        | ExpressionType.Constant, (:? ConstantExpression as ce) -> if isNull ce.Value then Some None else Some(Some ce.Value)
        | ExpressionType.New, (:? NewExpression as ne) ->
            try Some(Some(evaluateExpression ne))
            with err -> failwith ("unsupported nullable new expression " + ne.ToString() + " err: " + err.Message)
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

[<return: Struct>]
let rec (|Bool|_|) (e:Expression) = 
    match e.NodeType, e with
    | _, BoolStrict b -> ValueSome b
    | ExpressionType.Not, (:? UnaryExpression as ue) -> 
        match ue.Operand with Bool x -> ValueSome(not x) | _ -> ValueNone
    | _ -> ValueNone

[<return: Struct>]
let inline (|ParamName|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Parameter, (:? ParameterExpression as pe) ->  ValueSome pe.Name
    | _ -> ValueNone    

[<return: Struct>]
let inline (|ParamWithName|_|) (s:String) (e:Expression) = 
    match e with 
    | ParamName n when s = n -> ValueSome ()
    | _ -> ValueNone    
    
[<return: Struct>]
let (|Lambda|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Lambda, (:? LambdaExpression as ce) ->  ValueSome (Seq.toList ce.Parameters, ce.Body)
    | _ -> ValueNone

let inline (|OptionalQuote|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Quote, (:? UnaryExpression as ce) ->  ce.Operand
    | _ -> e

let inline (|OptionalCopyOfStruct|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Call, MethodCall(Some (Lambda([ParamName para], (:? MemberExpression as me))),MethodWithName "Invoke",[inner]) when para = "copyOfStruct" && me.Member.Name = "Value" -> inner
    | _ -> e

[<return: Struct>]
let inline (|CopyOfStruct|_|) (membername:String) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Call, MethodCall(Some (Lambda([ParamName para], (:? MemberExpression as me))),MethodWithName "Invoke",[inner]) when para = "copyOfStruct" && me.Member.Name = membername -> ValueSome inner
    | _ -> ValueNone

let (|OptionalFSharpOptionValue|) (e:Expression) = 
    match e.NodeType, e with
    | ExpressionType.MemberAccess, ( :? MemberExpression as e) -> 
        match e.Member with 
        | :? PropertyInfo as p when p.Name = "Value" && Common.Utilities.isOpt e.Member.DeclaringType -> e.Expression
        | _ -> upcast e
    | ExpressionType.Call, OptionalCopyOfStruct ( :? MethodCallExpression as e)
        when e.Method.Name = "Some" && Common.Utilities.isOpt e.Method.DeclaringType -> e.Arguments.[0]
    | _, OptionalCopyOfStruct n -> n

[<return: Struct>]
let inline (|AndAlso|_|) (e:Expression) =
    match e.NodeType, e with
    | ExpressionType.AndAlso, ( :? BinaryExpression as be) -> ValueSome(be.Left,be.Right)
    | _ -> ValueNone
    
[<return: Struct>]
let inline (|OrElse|_|) (e:Expression) =
    match e.NodeType, e with
    | ExpressionType.OrElse, ( :? BinaryExpression as be) -> ValueSome(be.Left,be.Right)
    | _ -> ValueNone
    
[<return: Struct>]
let inline (|AndAlsoOrElse|_|) (e:Expression) =
    match e.NodeType, e with
    | ExpressionType.OrElse,  ( :? BinaryExpression as be) 
    | ExpressionType.AndAlso, ( :? BinaryExpression as be)  -> ValueSome(be.Left,be.Right)
    | _ -> ValueNone

[<return: Struct>]
let (|FSharpIsNullMethod|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Call, (:? MethodCallExpression as e) ->
        if isNull e.Object && e.Method.Name = "IsNull" && e.Arguments.Count = 1 && e.Method.DeclaringType.FullName = "Microsoft.FSharp.Core.Operators" then
            ValueSome (e.Arguments.[0])
        else
            ValueNone
    | _ -> ValueNone

let (|OptionIsSome|_|) : Expression -> _ = function    
    | MethodCall(None,MethodWithName "get_IsSome", [e] ) -> Some e
    | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Not ->
        match ue.Operand with
        | MethodCall(None,MethodWithName "get_IsNone", [e] ) -> Some e
        | MemberAccess me when me.Member.Name = "IsNone" -> Some me.Expression
        | MethodCall(Some (Lambda([ParamName para], (:? MemberExpression as me))),MethodWithName "Invoke",[e]) when para = "copyOfStruct" && me.Member.Name = "IsNone" -> Some e
        | CopyOfStruct "IsNone" exp -> Some exp
        | FSharpIsNullMethod exp -> Some exp
        | _ -> None
    | MemberAccess me when me.Member.Name = "IsSome" -> Some me.Expression
    | CopyOfStruct "IsSome" exp -> Some exp
    | _ -> None

let (|OptionIsNone|_|) : Expression -> _ = function    
    | MethodCall(None,MethodWithName "get_IsNone", [e] ) -> Some e
    | :? UnaryExpression as ue when ue.NodeType = ExpressionType.Not ->
        match ue.Operand with
        | MethodCall(None,MethodWithName "get_IsSome", [e] ) -> Some e
        | MemberAccess me when me.Member.Name = "IsSome" -> Some me.Expression
        | CopyOfStruct "IsSome" exp -> Some exp
        | _ -> None
    | MemberAccess me when me.Member.Name = "IsNone" -> Some me.Expression
    | CopyOfStruct "IsNone" exp -> Some exp
    | FSharpIsNullMethod exp -> Some exp
    | _ -> None

[<return: Struct>]
let (|SqlCondOp|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Equal,              (:? BinaryExpression as ce) -> ValueSome (ConditionOperator.Equal,        ce.Left,ce.Right)
    | ExpressionType.LessThan,           (:? BinaryExpression as ce) -> ValueSome (ConditionOperator.LessThan,     ce.Left,ce.Right)
    | ExpressionType.LessThanOrEqual,    (:? BinaryExpression as ce) -> ValueSome (ConditionOperator.LessEqual,    ce.Left,ce.Right)
    | ExpressionType.GreaterThan,        (:? BinaryExpression as ce) -> ValueSome (ConditionOperator.GreaterThan,  ce.Left,ce.Right)
    | ExpressionType.GreaterThanOrEqual, (:? BinaryExpression as ce) -> ValueSome (ConditionOperator.GreaterEqual, ce.Left,ce.Right)
    | ExpressionType.NotEqual,           (:? BinaryExpression as ce) -> ValueSome (ConditionOperator.NotEqual,     ce.Left,ce.Right)
    | _ -> ValueNone

[<return: Struct>]
let (|SqlNegativeCondOp|_|) (e:Expression) = 
    match e.NodeType, e with
    | ExpressionType.Not, (:? UnaryExpression as ue) ->
        match ue.Operand.NodeType, ue.Operand with
        | ExpressionType.NotEqual,           (:? BinaryExpression as ce) -> ValueSome (ConditionOperator.Equal,        ce.Left,ce.Right)
        | ExpressionType.GreaterThanOrEqual, (:? BinaryExpression as ce) -> ValueSome (ConditionOperator.LessThan,     ce.Left,ce.Right)
        | ExpressionType.GreaterThan,        (:? BinaryExpression as ce) -> ValueSome (ConditionOperator.LessEqual,    ce.Left,ce.Right)
        | ExpressionType.LessThanOrEqual,    (:? BinaryExpression as ce) -> ValueSome (ConditionOperator.GreaterThan,  ce.Left,ce.Right)
        | ExpressionType.LessThan,           (:? BinaryExpression as ce) -> ValueSome (ConditionOperator.GreaterEqual, ce.Left,ce.Right)
        | ExpressionType.Equal,              (:? BinaryExpression as ce) -> ValueSome (ConditionOperator.NotEqual,     ce.Left,ce.Right)
        | _ -> ValueNone
    | _ -> ValueNone

// Unwrap Microsoft.FSharp.Core.Operators.Abs$W(ToFSharpFunc(arg0_0 => Abs(arg0_0)), x) to Abs(x)
[<return: Struct>]
let (|MethodCallOrFSharpWrap|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Call, (:? MethodCallExpression as e) ->
#if NETSTANDARD21
        if isNull e.Object && e.Method.Name.Contains('$') && e.Arguments.Count = 2 && e.Method.DeclaringType.FullName = "Microsoft.FSharp.Core.Operators" then
#else
        if isNull e.Object && e.Method.Name.Contains("$") && e.Arguments.Count = 2 && e.Method.DeclaringType.FullName = "Microsoft.FSharp.Core.Operators" then
#endif
            match e.Arguments.[0].NodeType, e.Arguments.[0] with
            | ExpressionType.Call, (:? MethodCallExpression as ec) when ec.Method.Name = "ToFSharpFunc" && ec.Arguments.Count = 1 ->
                match ec.Arguments.[0] with
                | Lambda(arg, body) ->
                    match body.NodeType, body with
                    | ExpressionType.Call, (:? MethodCallExpression as eop) ->
                        ValueSome (None, eop.Method, [e.Arguments.[1]])
                    | _ -> ValueSome ((match e.Object with null -> None | obj -> Some obj), e.Method, Seq.toList e.Arguments)
                | _ -> ValueSome ((match e.Object with null -> None | obj -> Some obj), e.Method, Seq.toList e.Arguments)
            | _ -> ValueSome ((match e.Object with null -> None | obj -> Some obj), e.Method, Seq.toList e.Arguments)
        else
            ValueSome ((match e.Object with null -> None | obj -> Some obj), e.Method, Seq.toList e.Arguments)
    | _ -> ValueNone

let (|SqlPlainColumnGet|_|) = function
    | OptionalFSharpOptionValue(MethodCall(Some o,((MethodWithName "GetColumn" as meth) | (MethodWithName "GetColumnOption" as meth)| (MethodWithName "GetColumnValueOption" as meth)),[String key])) when o.Type.Name = "SqlEntity" -> 
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
    | OptionalFSharpOptionValue(MethodCall(Some o,((MethodWithName "GetColumn" as meth) | (MethodWithName "GetColumnOption" as meth) | (MethodWithName "GetColumnValueOption" as meth)),[String key])) when o.Type.Name = "SqlEntity" -> 
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

/// Return types whose evaluation errors propagate; for other types evaluateMethodCall gives None on an error.
let private propagatingReturnTypes =
    System.Collections.Generic.HashSet<Type>(
        [ typeof<Int32>; typeof<Int64>; typeof<Decimal>; typeof<DateTime>; typeof<Guid>; typeof<Boolean>; typeof<Single>
          typeof<Int16>; typeof<UInt32>; typeof<UInt16>; typeof<UInt64>; typeof<Byte>; typeof<Char>; typeof<DateTimeOffset>
          typeof<TimeSpan>; typeof<bigint>
          typeof<ValueOption<Int32>>; typeof<ValueOption<Int64>>; typeof<ValueOption<Decimal>>; typeof<ValueOption<DateTime>>
          typeof<ValueOption<Guid>>; typeof<ValueOption<Boolean>>; typeof<ValueOption<Single>>; typeof<ValueOption<Int16>>
          typeof<ValueOption<UInt32>>; typeof<ValueOption<UInt16>>; typeof<ValueOption<UInt64>>; typeof<ValueOption<Byte>>
          typeof<ValueOption<Char>>; typeof<ValueOption<DateTimeOffset>>; typeof<ValueOption<TimeSpan>>; typeof<ValueOption<bigint>> ])

/// Evaluates a parameterless method call once
let internal evaluateMethodCall (meth: MethodCallExpression) : obj option =
    let returnType = meth.Method.ReturnType
    if not returnType.IsClass && propagatingReturnTypes.Contains returnType then Some(evaluateExpression meth)
    else
        try Some(evaluateExpression meth)
        with _ -> None

let internal getRightFromOp (right:Expression) = evaluateExpression right

let (|SqlExistsClause|_|) = function 
    | MethodCall(None, (MethodWithName "Any" as meth), [ SeqValuesQueryable src; OptionalQuote qual ]) ->
        Some(meth, ConditionOperator.NestedExists, src, qual)
    | _ -> None

[<return: Struct>]
let (|SqlNotExistsClause|_|) (e:Expression) = 
    match e.NodeType, e with
    | ExpressionType.Not, (:? UnaryExpression as ue) ->
        match ue.Operand with
        | MethodCall(None, (MethodWithName "Any" as meth), [ SeqValuesQueryable src; OptionalQuote qual ]) -> ValueSome(meth, ConditionOperator.NestedNotExists, src, qual)
        | _ -> ValueNone
    | _ -> ValueNone

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
            | t when Type.(=)(t, typeof<string>) || Type.(=)(t, typeof<Option<string>>) || Type.(=)(t, typeof<ValueOption<string>>) -> // String functions
                match meth.Name, par with
                | "Substring", [Int startPos] -> Some(alias, CanonicalOperation(CanonicalOp.Substring(SqlConstant startPos), col), typ)
                | "Substring", [SqlColumnGet(al2,col2,typ2) as pe] when integerTypes |> Array.exists(fun t -> Type.(=)(pe.Type, t)) -> Some(alias, CanonicalOperation(CanonicalOp.Substring(SqlCol(al2,col2)), col), typ)
                | "Substring", [Int startPos; Int strLen] -> Some(alias, CanonicalOperation(CanonicalOp.SubstringWithLength(SqlConstant startPos,SqlConstant strLen), col), typ)
                | "Substring", [SqlColumnGet(al2,col2,typ2) as pe; Int strLen] when integerTypes |> Array.exists(fun t -> Type.(=)(pe.Type, t)) -> Some(alias, CanonicalOperation(CanonicalOp.SubstringWithLength(SqlCol(al2,col2),SqlConstant strLen), col), typ)
                | "Substring", [Int startPos; SqlColumnGet(al2,col2,typ2) as pe] when integerTypes |> Array.exists(fun t -> Type.(=)(pe.Type, t)) -> Some(alias, CanonicalOperation(CanonicalOp.SubstringWithLength(SqlConstant startPos,SqlCol(al2,col2)), col), typ)
                | "Substring", [SqlColumnGet(al2,col2,ty2) as pe1; SqlColumnGet(al3,col3,ty3) as pe2]  when integerTypes |> Array.exists((=) pe1.Type) && integerTypes |> Array.exists((=) pe2.Type) -> Some(alias, CanonicalOperation(CanonicalOp.SubstringWithLength(SqlCol(al2,col2),SqlCol(al3,col3)), col), typ)
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
                | "IndexOf", [String search; SqlColumnGet(al2,col2,typ2) as pe] when integerTypes |> Array.exists(fun t -> t = pe.Type) -> Some(alias, CanonicalOperation(CanonicalOp.IndexOfStart(SqlConstant search, SqlCol(al2,col2)), col), intType typ)
                | "IndexOf", [SqlColumnGet(al2,col2,_); SqlColumnGet(al3,col3,typ2) as pe] when integerTypes |> Array.exists(fun t -> Type.(=)(pe.Type, t)) -> Some(alias, CanonicalOperation(CanonicalOp.IndexOfStart(SqlCol(al2,col2), SqlCol(al3,col3)), col), intType typ)
                | _ -> None
            | t when Type.(=)(t, typeof<DateTime>) || Type.(=)(t, typeof<Option<DateTime>>) || Type.(=)(t, typeof<ValueOption<DateTime>>) || // DateTime functions
                     Type.(=)(t, typeof<DateTimeOffset>) || Type.(=)(t, typeof<Option<DateTimeOffset>>) || Type.(=)(t, typeof<ValueOption<DateTimeOffset>>) ->
                match meth.Name, par with
                | "AddYears", [Int x] -> Some(alias, CanonicalOperation(CanonicalOp.AddYears(SqlConstant(box x)), col), typ)
                | "AddYears", [SqlColumnGet(al2,col2,typ2) as pe] when integerTypes |> Array.exists(fun t -> Type.(=)(pe.Type, t)) -> Some(alias, CanonicalOperation(CanonicalOp.AddYears(SqlCol(al2,col2)), col), typ)
                | "AddMonths", [Int x] -> Some(alias, CanonicalOperation(CanonicalOp.AddMonths(x), col), typ)
                | "AddDays", [Float x] -> Some(alias, CanonicalOperation(CanonicalOp.AddDays(SqlConstant(box x)), col), typ)
                | "AddDays", [OptionalConvertOrTypeAs(Int x)] -> Some(alias, CanonicalOperation(CanonicalOp.AddDays(SqlConstant(box x)), col), typ)
                | "AddDays", [OptionalConvertOrTypeAs(SqlColumnGet(al2,col2,typ2)) as pe] when integerTypes |> Array.exists(fun t -> Type.(=)(pe.Type, t)) || decimalTypes |> Array.exists(fun t -> Type.(=)(pe.Type, t))  -> Some(alias, CanonicalOperation(CanonicalOp.AddDays(SqlCol(al2,col2)), col), typ)
                | "AddHours", [Float x] -> Some(alias, CanonicalOperation(CanonicalOp.AddHours(x), col), typ)
                | "AddHours", [OptionalConvertOrTypeAs(Int x)] -> Some(alias, CanonicalOperation(CanonicalOp.AddHours(x), col), typ)
                | "AddMinutes", [Float x] -> Some(alias, CanonicalOperation(CanonicalOp.AddMinutes(SqlConstant(box x)), col), typ)
                | "AddMinutes", [OptionalConvertOrTypeAs(Int x)] -> Some(alias, CanonicalOperation(CanonicalOp.AddMinutes(SqlConstant(box x)), col), typ)
                | "AddMinutes", [OptionalConvertOrTypeAs(SqlColumnGet(al2,col2,typ2)) as pe] when integerTypes |> Array.exists(fun t -> Type.(=)(pe.Type, t)) || decimalTypes |> Array.exists(fun t -> Type.(=)(pe.Type, t))  -> Some(alias, CanonicalOperation(CanonicalOp.AddMinutes(SqlCol(al2,col2)), col), typ)
                | "AddSeconds", [Float x] -> Some(alias, CanonicalOperation(CanonicalOp.AddSeconds(x), col), typ)
                | "AddSeconds", [OptionalConvertOrTypeAs(Int x)] -> Some(alias, CanonicalOperation(CanonicalOp.AddSeconds(x), col), typ)
                | _ -> None
            | _ -> None
    // These are canonical properties
    | _, OptionalFSharpOptionValue(PropertyGet(Some(OptionalFSharpOptionValue(SqlColumnGet(alias, col, typ) as p1)), propInfo)) -> 
        match p1.Type with
        | t when Type.(=)(t, typeof<string>) || Type.(=)(t, typeof<Option<string>>) || Type.(=)(t, typeof<ValueOption<string>>)  -> // String functions
            match propInfo.Name with
            | "Length" -> Some(alias, CanonicalOperation(CanonicalOp.Length, col), intType typ)
            | _ -> None
        | t when Type.(=)(t, typeof<DateTime>) || Type.(=)(t, typeof<Option<DateTime>>) || Type.(=)(t, typeof<ValueOption<DateTime>>) ||
                 Type.(=)(t, typeof<DateTimeOffset>) || Type.(=)(t, typeof<Option<DateTimeOffset>>) || Type.(=)(t, typeof<ValueOption<DateTimeOffset>>)
                -> // DateTime functions
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
            when (meth.Name = "Subtract" && (Type.(=)(meth.ReturnType, typeof<TimeSpan>) || Type.(=)(meth.ReturnType, typeof<Option<TimeSpan>>) || Type.(=)(meth.ReturnType, typeof<ValueOption<TimeSpan>>)) && 
                  (Type.(=)(p1.Type, typeof<DateTime>) || Type.(=)(p1.Type, typeof<Option<DateTime>>) || Type.(=)(p1.Type, typeof<ValueOption<DateTime>>) ||
                   Type.(=)(p1.Type, typeof<DateTimeOffset>) || Type.(=)(p1.Type, typeof<Option<DateTimeOffset>>) || Type.(=)(p1.Type, typeof<ValueOption<DateTimeOffset>>)
                        )) -> 
        match propInfo.Name, par with
        | "Days", (SqlColumnGet(al2,col2,typ2) as pe) when (Type.(=)(pe.Type, typeof<DateTime>) || Type.(=)(pe.Type, typeof<Option<DateTime>>) || Type.(=)(pe.Type, typeof<ValueOption<DateTime>>))
                                                            || (Type.(=)(pe.Type, typeof<DateTimeOffset>) || Type.(=)(pe.Type, typeof<Option<DateTimeOffset>>) || Type.(=)(pe.Type, typeof<ValueOption<DateTimeOffset>>)) -> Some(alias, CanonicalOperation(CanonicalOp.DateDiffDays(SqlCol(al2,col2)), col), typ)
        | "Seconds", (SqlColumnGet(al2,col2,typ2) as pe) when (Type.(=)(pe.Type, typeof<DateTime>) || Type.(=)(pe.Type, typeof<Option<DateTime>>) || Type.(=)(pe.Type, typeof<ValueOption<DateTime>>))
                                                               || (Type.(=)(pe.Type, typeof<DateTimeOffset>) || Type.(=)(pe.Type, typeof<Option<DateTimeOffset>>) || Type.(=)(pe.Type, typeof<ValueOption<DateTimeOffset>>)) -> Some(alias, CanonicalOperation(CanonicalOp.DateDiffSecs(SqlCol(al2,col2)), col), typ)
        | "Days", Constant(c,t) when Type.(=)(t, typeof<DateTime>) || Type.(=)(t, typeof<DateTimeOffset>) -> Some(alias, CanonicalOperation(CanonicalOp.DateDiffDays(SqlConstant(box c)), col), typ)
        | "Seconds", Constant(c,t) when Type.(=)(t, typeof<DateTime>) || Type.(=)(t, typeof<DateTimeOffset>) -> Some(alias, CanonicalOperation(CanonicalOp.DateDiffSecs(SqlConstant(box c)), col), typ)
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
            | "Sqrt", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Sqrt, col), typ)
            | "Sin", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Sin, col), typ)
            | "Cos", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Cos, col), typ)
            | "Tan", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Tan, col), typ)
            | "ASin", [_] -> Some(alias, CanonicalOperation(CanonicalOp.ASin, col), typ)
            | "ACos", [_] -> Some(alias, CanonicalOperation(CanonicalOp.ACos, col), typ)
            | "ATan", [_] -> Some(alias, CanonicalOperation(CanonicalOp.ATan, col), typ)
            | "Truncate", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Truncate, col), typ)
            | _ -> failwith "Shouldn't hit"

    // Math.Round(col, decimals): a 2-arg call the single-arg numerical pattern above can't match.
    | _, OptionalFSharpOptionValue(MethodCallOrFSharpWrap(None, meth, [OptionalFSharpOptionValue(OptionalConvertOrTypeAs(SqlColumnGet(alias, col, typ)) as pe); Int decCount]))
        when (meth.Name = "Round" && (decimalTypes |> Array.exists(fun t -> Type.(=)(pe.Type, t)))) ->
            Some(alias, CanonicalOperation(CanonicalOp.RoundDecimals(decCount), col), typ)

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

        if Type.(=)(be.Left.Type, typeof<DateTime>) || Type.(=)(be.Right.Type, typeof<DateTime>) || Type.(=)(be.Left.Type, typeof<Option<DateTime>>) || Type.(=)(be.Right.Type, typeof<Option<DateTime>>)
                || Type.(=)(be.Left.Type, typeof<ValueOption<DateTime>>) || Type.(=)(be.Right.Type, typeof<ValueOption<DateTime>>) ||
                Type.(=)(be.Left.Type, typeof<DateTimeOffset>) || Type.(=)(be.Right.Type, typeof<DateTimeOffset>) || Type.(=)(be.Left.Type, typeof<Option<DateTimeOffset>>) || Type.(=)(be.Right.Type, typeof<Option<DateTimeOffset>>)
                    || Type.(=)(be.Left.Type, typeof<ValueOption<DateTimeOffset>>) || Type.(=)(be.Right.Type, typeof<ValueOption<DateTimeOffset>>)
                then
            // DateTime math operations are not supported directly as they return .NET TimeSpan which is not the clear translation of SQL.
            // You can use functions like .AddHours(), .AddDays(), .Subtract().Days and comparison.
            None
        else

        match be.Left, be.Right with
        | OptionalConvertOrTypeAs(OptionalFSharpOptionValue(SqlColumnGet(alias, col, typ))) as p1, OptionalConvertOrTypeAs(Constant(constVal,constTyp)) 
            when (Type.(=)(p1.Type, constTyp) || (Common.Utilities.isOpt p1.Type && p1.Type.GenericTypeArguments.[0] = constTyp )
                 || Type.(=)(be.Left.Type, be.Right.Type)) ->  // Support only numeric and string math
                match p1.Type with
                | t when (operation = "+" && (Type.(=)(t, typeof<string>) || Type.(=)(t, typeof<char>) || Type.(=)(t, typeof<Option<string>>) || Type.(=)(t, typeof<Option<char>>) || Type.(=)(t, typeof<ValueOption<char>>))) -> 
                    // Standard SQL string concatenation is ||
                    Some(alias, CanonicalOperation(CanonicalOp.BasicMath("||", constVal), col), typ)
                | t when (decimalTypes |> Array.exists(fun tt -> Type.(=)(t, tt)) || integerTypes |> Array.exists(fun tt -> Type.(=)(t, tt))) ->
                        Some(alias, CanonicalOperation(CanonicalOp.BasicMath(operation, constVal), col), typ)
                | _ -> None
        | OptionalConvertOrTypeAs(Constant(constVal,constTyp)), (OptionalConvertOrTypeAs(OptionalFSharpOptionValue(SqlColumnGet(alias, col, typ))) as p1)
            when (Type.(=)(p1.Type, constTyp) || (Common.Utilities.isOpt p1.Type && p1.Type.GenericTypeArguments.[0] = constTyp )
                 || Type.(=)(be.Left.Type, be.Right.Type)) ->  // Support only numeric and string math
                match p1.Type with
                | t when (operation = "+" && (Type.(=)(t, typeof<string>) || Type.(=)(t, typeof<char>) || Type.(=)(t, typeof<Option<string>>) || Type.(=)(t, typeof<Option<char>>) || Type.(=)(t, typeof<ValueOption<string>>) || Type.(=)(t, typeof<ValueOption<char>>))) -> 
                    // Standard SQL string concatenation is ||
                    Some(alias, CanonicalOperation(CanonicalOp.BasicMath("||", constVal), col), typ)
                | t when (decimalTypes |> Array.exists(fun tt -> Type.(=)(t, tt)) || integerTypes |> Array.exists(fun tt -> Type.(=)(t, tt))) ->
                        Some(alias, CanonicalOperation(CanonicalOp.BasicMathLeft(operation, constVal), col), typ)
                | _ -> None
        | OptionalConvertOrTypeAs(OptionalFSharpOptionValue(SqlColumnGet(aliasLeft, colLeft, typLeft))) as p1, (OptionalConvertOrTypeAs(OptionalFSharpOptionValue(SqlColumnGet(aliasRight, colRight, typRight))) as p2) 
            when (Type.(=)(p1.Type, p2.Type) ||
                     (Common.Utilities.isOpt p1.Type && p1.Type.GenericTypeArguments.[0] = p2.Type ) ||
                     (Common.Utilities.isOpt p2.Type && p2.Type.GenericTypeArguments.[0] = p1.Type ) ||
                         Type.(=)(be.Left.Type, be.Right.Type)) -> 
                let opFix = 
                    match p1.Type with
                    | t when ((Type.(=)(t, typeof<string>) || Type.(=)(t, typeof<char>) || Type.(=)(t, typeof<Option<string>>) || Type.(=)(t, typeof<Option<char>>) || Type.(=)(t, typeof<ValueOption<string>>) || Type.(=)(t, typeof<ValueOption<char>>)) && operation = "+") -> "||"
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
                extendFilter [] (Some [filterExpression left; filterExpression right])
            | AndAlsoOrElse(AndAlsoOrElse(_) as left,SimpleCondition c)  ->
                extendFilter [c] (Some [filterExpression left])
            | AndAlsoOrElse(SimpleCondition c,(AndAlsoOrElse(_) as right))  ->
                extendFilter [c] (Some [filterExpression right])
            | AndAlsoOrElse(SimpleCondition c1 as cc1 ,SimpleCondition c2) as cc2 ->
                if cc1 = cc2 then extendFilter [c1] None
                else extendFilter [c1;c2] None
            | SimpleCondition cond ->
                Condition.And([cond],None)

            // Support for simple boolean expressions:
            | AndAlso(Bool b, x) | AndAlso(x, Bool b) when b -> filterExpression x
            | OrElse(Bool b, x) | OrElse(x, Bool b) when not b -> filterExpression x
            | Bool b when b -> Condition.ConstantTrue
            | Bool b when not b -> Condition.ConstantFalse
            | _ -> Condition.NotSupported exp

        let filter = filterExpression (ExpressionOptimizer.visit ce.Test)

        match filter, ce.IfTrue, ce.IfFalse with
        | Condition.NotSupported x, _, _ -> None
        | Condition.ConstantTrue, OptionalConvertOrTypeAs(SqlColumnGet(alias,col,typ)), _ -> Some(alias, col, typ)
        | Condition.ConstantFalse, _, OptionalConvertOrTypeAs(SqlColumnGet(alias,col,typ)) -> Some(alias, col, typ)
        | _, OptionalConvertOrTypeAs(SqlColumnGet(al2,col2,typ2)), Constant(c, ct)  -> Some(al2, CanonicalOperation(CanonicalOp.CaseSql(filter, SqlConstant(c)), col2), typ2)
        | _, OptionalConvertOrTypeAs(SqlColumnGet(al2,col2,typ2)), OptionalConvertOrTypeAs(SqlColumnGet(al3,col3,typ3)) -> Some(al2, CanonicalOperation(CanonicalOp.CaseSql(filter, SqlCol(al3,col3)), col2), typ2)
        | _, Constant(c, ct), OptionalConvertOrTypeAs(SqlColumnGet(al2,col2,typ2)) -> Some(al2, CanonicalOperation(CanonicalOp.CaseNotSql(filter, SqlConstant(c)), col2), typ2)

        | _, Constant(c1, ct1), Constant(c2, ct2) when ct1=ct2 -> Some("", CanonicalOperation(CanonicalOp.CaseSqlPlain(filter, c1, c2), SqlColumnType.KeyColumn("constant")), ct1)

        | _ -> None

    | ExpressionType.Call, (:? MethodCallExpression as e) when e.Method.Name = "Parse" && e.Arguments.Count = 1 && 
                           (Type.(=)(e.Type, typeof<DateTime>) || Type.(=)(e.Type, typeof<Option<DateTime>>) || Type.(=)(e.Type, typeof<ValueOption<DateTime>>)
                           || Type.(=)(e.Type, typeof<DateTimeOffset>) || Type.(=)(e.Type, typeof<Option<DateTimeOffset>>) || Type.(=)(e.Type, typeof<ValueOption<DateTimeOffset>>)) ->
        // Don't do any magic, just: DateTime.Parse('2000-01-01') -> '2000-01-01'
        match e.Arguments.[0] with
        | SqlColumnGet(alias, col, typ) when Type.(=)(typ, typeof<String>) || Type.(=)(typ, typeof<Option<String>>) || Type.(=)(typ, typeof<ValueOption<String>>) 
            -> Some(alias, col, e.Type)
        | _ -> None
    | ExpressionType.Call, (:? MethodCallExpression as e) when e.Method.Name = "Parse" && e.Arguments.Count = 1 && 
                           (Type.(=)(e.Type, typeof<int>) || Type.(=)(e.Type, typeof<Option<int>>) || Type.(=)(e.Type, typeof<ValueOption<int>>)) ->
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
                Some(evaluateExpression me)
            | ExpressionType.Call, (:? MethodCallExpression as e) ->

                evaluateMethodCall e

            | ExpressionType.MemberAccess, (:? MemberExpression as me) when Type.(=)(me.Type, typeof<DateTime>) && me.Member.Name = "Now" -> Some(DateTime.Now |> box)
            | ExpressionType.MemberAccess, (:? MemberExpression as me) when Type.(=)(me.Type, typeof<DateTime>) && me.Member.Name = "UtcNow" -> Some(DateTime.UtcNow |> box)
            | ExpressionType.MemberAccess, (:? MemberExpression as me) when Type.(=)(me.Type, typeof<DateTime>) && me.Member.Name = "Today" -> Some(DateTime.Today |> box)
            | ExpressionType.MemberAccess, (:? MemberExpression as me) when Type.(=)(me.Type, typeof<DateTimeOffset>) && me.Member.Name = "Now" -> Some(DateTimeOffset.Now |> box)
            | ExpressionType.MemberAccess, (:? MemberExpression as me) when Type.(=)(me.Type, typeof<DateTimeOffset>) && me.Member.Name = "UtcNow" -> Some(DateTimeOffset.UtcNow |> box)
            | _ ->
                // In case user feeds us incomplete lambda, we will not execute: the user might have a context needed for the compilation.
                try Some(evaluateExpression meth)
                with _ -> None
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
                let gotVal = retType.GetProperty "Value" // Option type Some should not be SQL-parameter.
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
    | SqlCondOp(op,(OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_))),OptionalConvertOrTypeAs(OptionalFSharpOptionValue(ConstantOrNullableConstant c))) 
    | SqlNegativeCondOp(op,(OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_))),OptionalConvertOrTypeAs(OptionalFSharpOptionValue(ConstantOrNullableConstant c))) ->
        Some(ti,key,op,c)
    | SqlCondOp(op,OptionalConvertOrTypeAs(OptionalFSharpOptionValue(ConstantOrNullableConstant c)),(OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_)))) 
    | SqlNegativeCondOp(op,OptionalConvertOrTypeAs(OptionalFSharpOptionValue(ConstantOrNullableConstant c)),(OptionalConvertOrTypeAs(SqlColumnGet(ti,key,_)))) ->
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
    | MethodCall(None,MethodWithName "op_BarEqualsBar", [SqlColumnGet(ti,key,_); SeqValues values]) -> Some(ti, ConditionOperator.In, key, values)
    | MethodCall(None,MethodWithName "op_BarLessGreaterBar",[SqlColumnGet(ti,key,_); SeqValues values]) -> Some(ti, ConditionOperator.NotIn, key, values)
    | MethodCall(None,MethodWithName "Contains", [SeqValues values; SqlColumnGet(ti,key,_)]) -> Some(ti, ConditionOperator.In, key, values)
    | MethodCall(Some((SeqValues values) as setVals),MethodWithName "Contains", [SqlColumnGet(ti,key,_)]) when setVals.Type.IsGenericType -> Some(ti, ConditionOperator.In, key, values)
    | _ -> None

and (|SqlSpecialOpArrQueryable|_|) = function
    // for some crazy reason, simply using (|=|) stopped working ??
    | MethodCall(None,MethodWithName "op_BarEqualsBar", [SqlColumnGet(ti,key,_); SeqValuesQueryable values]) -> Some(ti, ConditionOperator.NestedIn, key, values)
    | MethodCall(None,MethodWithName "op_BarLessGreaterBar",[SqlColumnGet(ti,key,_); SeqValuesQueryable values]) -> Some(ti, ConditionOperator.NestedNotIn, key, values)
    | MethodCall(None,MethodWithName "Contains", [SeqValuesQueryable values; SqlColumnGet(ti,key,_)]) -> Some(ti, ConditionOperator.NestedIn, key, values)
    | _ -> None
    
and (|SqlSpecialOp|_|) e =
    match e with
    | MethodCall(None,MethodWithName "op_EqualsPercent", [SqlColumnGet(ti,key,_); right]) -> Some(ti,ConditionOperator.Like,   key,getRightFromOp right)
    | MethodCall(None,MethodWithName "op_LessGreaterPercent",[SqlColumnGet(ti,key,_); right]) -> Some(ti,ConditionOperator.NotLike,key,getRightFromOp right)
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
        | MethodCall(None,MethodWithName "Contains", [SeqValues values; SqlColumnGet(ti,key,_)]) -> Some(ti, ConditionOperator.NotIn, key, values)
        | MethodCall(Some((SeqValues values) as setVals),MethodWithName "Contains", [SqlColumnGet(ti,key,_)]) when setVals.Type.IsGenericType -> Some(ti, ConditionOperator.NotIn, key, values)
        | _ -> None
    | _ -> None

and (|SqlSpecialNegativeOpArrQueryable|_|) (e:Expression) = 
    match e.NodeType, e with
    | ExpressionType.Not, (:? UnaryExpression as ue) ->
        match ue.Operand with
        | MethodCall(None,MethodWithName "Contains", [SeqValuesQueryable values; SqlColumnGet(ti,key,_)]) -> Some(ti, ConditionOperator.NestedNotIn, key, values)
        | _ -> None
    | _ -> None
