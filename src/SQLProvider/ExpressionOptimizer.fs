/// This is just a light-weight expression optimizer.
/// It won't Compile() or do any heavy stuff.
module ExpressionOptimizer

open System.Linq.Expressions
open System
open System.Reflection

/// Purpose of this is optimize away already known constant=constant style expressions.
///   7 > 8      -->   False
/// "G" = "G"    -->   True
let ``replace constant comparison`` (e:Expression) =
    match e with
    | (:? BinaryExpression as ce) -> 
        let (|Constant|_|) (e:Expression) = 
            match e.NodeType, e with 
            | ExpressionType.Constant, (:? ConstantExpression as ce) when (ce.Value :? IComparable) -> Some (ce.Value :?> IComparable)
            | _ -> None
        let createbool b = Expression.Constant(b,  typeof<bool>) :> Expression
        match ce.Left, ce.Right with
        | Constant l, Constant r -> 
            match e.NodeType with
            | ExpressionType.Equal              -> createbool (l=r)
            | ExpressionType.LessThan           -> createbool (l<r)
            | ExpressionType.LessThanOrEqual    -> createbool (l<=r)
            | ExpressionType.GreaterThan        -> createbool (l>r)
            | ExpressionType.GreaterThanOrEqual -> createbool (l>=r)
            | ExpressionType.NotEqual           -> createbool (l<>r)
            | _ -> e
        | _ -> e
    | _ -> e


/// Purpose of this is to replace non-used anonymous types:
/// new AnonymousObject(Item1 = x, Item2 = "").Item1    -->   x
let ``remove AnonymousType`` (e:Expression) =
    if e = Unchecked.defaultof<Expression> then e else 
    match e.NodeType, e with
    | ExpressionType.MemberAccess, ( :? MemberExpression as me)
        when me.Member.DeclaringType.Name.ToUpper().StartsWith("ANONYMOUSOBJECT") || me.Member.DeclaringType.Name.ToUpper().StartsWith("TUPLE") ->
            let memberIndex = 
                if me.Member.Name.StartsWith("Item") && me.Member.Name.Length > 4 then
                    let ok, i = Int32.TryParse(me.Member.Name.Substring(4))
                    if ok then Some i else None
                else None
            match memberIndex, me.Expression.NodeType, me.Expression, me.Member with 
            | Some idx, ExpressionType.New, (:? NewExpression as ne), (:? PropertyInfo as p) when ne <> null && p <> null -> 
                if ne.Arguments.Count > idx - 1 && ne.Arguments.[idx-1].Type = p.PropertyType then 
                    ne.Arguments.[idx-1] // We found it!
                else e
            | _ -> e
    | _ -> e

//
let ``cut not used condition`` (e:Expression) =
    match e.NodeType, e with
    | ExpressionType.Conditional,        (:? ConditionalExpression as ce) -> 
        match ce.Test with // For now, only direct booleans conditions are optimized to select query:
        | :? ConstantExpression as c when c.Value = box(true) -> ce.IfTrue
        | :? ConstantExpression as c when c.Value = box(false) -> ce.IfFalse
        | _ -> e
    | _ -> e

let ``not false is true``(e:Expression) =
    match e.NodeType, e with
    | ExpressionType.Not, (:? UnaryExpression as ue) -> 
        match ue.Operand with
        | :? ConstantExpression as c when c.Value = box(false) -> Expression.Constant(true, typeof<bool>) :> Expression
        | :? ConstantExpression as c when c.Value = box(true) -> Expression.Constant(false, typeof<bool>) :> Expression
        | _ -> e
    | _ -> e



// --------------- SOME BOOLEAN ALGEBRA ----------------------/
// Idea from https://github.com/mavnn/Algebra.Boolean/blob/6b2099420ef605e3b3f818883db957154afa836a/Algebra.Boolean/Transforms.fs
// But System.Linq.Expressions, not Microsoft.FSharp.Quotations

// Reductions:
// [associate; commute; distribute; gather; identity; annihilate; absorb; idempotence; complement; doubleNegation; deMorgan]

open Microsoft.FSharp.Quotations
open System.Linq.Expressions

let (|Value|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Constant, (:? ConstantExpression as ce) -> Some (ce.Value, ce.Type)
    | _ -> None

let (|IfThenElse|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Conditional, (:? ConditionalExpression as ce) -> Some (ce.Test, ce.IfTrue, ce.IfFalse)
    | _ -> None

let (|Not'|_|) (e:Expression) =
    match e.NodeType, e with
    | ExpressionType.Not, (:? UnaryExpression as ue) -> Some(ue.Operand)
    | _ -> None

let (|True'|_|) expr =
    match expr with
    | Value (o, t) when t = typeof<bool> && (o :?> bool) = true ->
        Some expr
    | _ -> None

let (|False'|_|) expr =
    match expr with
    | Value (o, t) when t = typeof<bool> && (o :?> bool) = false ->
        Some expr
    | _ -> None

let (|Or'|_|) (e:Expression) =
    match e.NodeType, e with
    | _, IfThenElse (left, True' _, right) ->
        Some (left, right)
    | ExpressionType.OrElse, ( :? BinaryExpression as be) -> Some(be.Left,be.Right)
    //| ExpressionType.Or, ( :? BinaryExpression as be) -> Some(be.Left,be.Right)
    | _ -> None

let (|And'|_|) (e:Expression) =
    match e.NodeType, e with
    | _, IfThenElse (left, right, False' _) ->
        Some (left, right)
    | ExpressionType.AndAlso, ( :? BinaryExpression as be)  -> Some(be.Left,be.Right)
    //| ExpressionType.And, ( :? BinaryExpression as be)  -> Some(be.Left,be.Right)
    | _ -> None

// This would just cause looping...
//let associate = function
//    | Or' (Or' (l, r), r') -> Expression.OrElse(Expression.OrElse(l, r), r') :> Expression
//    | Or' (l, Or' (l', r)) -> Expression.OrElse(l, Expression.OrElse(l', r)) :> Expression
//    | And' (And' (l, r), r') -> Expression.AndAlso(Expression.AndAlso(l, r), r') :> Expression
//    | And' (l, And' (l', r)) -> Expression.AndAlso(l, Expression.AndAlso(l', r)) :> Expression
//    | noHit -> noHit

// We commute to AndAlso and OrElse, if not already in that format
let commute = function
    | Or' (left, right) as comex when comex.NodeType <> ExpressionType.OrElse -> Expression.OrElse(right, left) :> Expression
    | And' (left, right) as comex when comex.NodeType <> ExpressionType.AndAlso -> Expression.AndAlso(right, left) :> Expression
    | noHit -> noHit

// This would just cause looping...
//let distribute = function
//    | And' (p, Or' (p', p'')) -> Expression.OrElse(Expression.AndAlso(p, p'), Expression.AndAlso(p, p'')) :> Expression
//    | Or' (p, And' (p', p'')) -> Expression.AndAlso(Expression.OrElse(p, p'), Expression.OrElse(p, p'')) :> Expression
//    | noHit -> noHit

let gather = function
    | And' (Or'(p, p'), Or'(p'', p''')) when p = p'' -> Expression.OrElse(p, Expression.AndAlso(p', p''')) :> Expression
    | Or' (And'(p, p'), And'(p'', p''')) when p = p'' -> Expression.AndAlso(p, Expression.OrElse(p', p''')) :> Expression
    | noHit -> noHit

let identity = function
    | And' (True' _, p)
    | And' (p, True' _)
    | Or' (False' _, p) 
    | Or' (p, False' _)
        -> p
    | noHit -> noHit

let annihilate = function
    | And' (False' f, _)
    | And' (_, False' f) -> f
    | Or' (True' t, _) 
    | Or' (_, True' t) -> t
    | noHit -> noHit

let absorb = function
    | And' (p, Or' (p', _)) 
    | And' (p, Or' (_, p')) 
    | And' (Or' (p', _), p)
    | And' (Or' (_, p'), p)
    | Or' (p, And' (p', _))
    | Or' (p, And' (_, p'))
    | Or' (And' (p', _), p)
    | Or' (And' (_, p'), p) when p = p' -> p
    | noHit -> noHit

let idempotence = function
    | And' (p, p') when p = p' -> p
    | Or' (p, p')  when p = p' -> p
    | noHit -> noHit

let complement = function
    | And' (p, Not' p')
    | And' (Not' p, p') when p = p' -> Expression.Constant(false, typeof<bool>) :> Expression
    | Or' (p, Not' p')
    | Or' (Not' p, p') when p = p' -> Expression.Constant(true, typeof<bool>) :> Expression
    | noHit -> noHit

let doubleNegation = function
    | Not' (Not' p) -> p
    | noHit -> noHit

let deMorgan = function
    | Or' (Not' p, Not' p') -> Expression.Not(Expression.AndAlso(p, p')) :> Expression
    | And' (Not' p, Not' p') -> Expression.Not(Expression.OrElse(p, p')) :> Expression
    | noHit -> noHit

/// ------------------------------------- ///

/// This is just helping to work with FSI:
let ``evaluate FSI constants`` (e:Expression) =
    match e.NodeType, e with
    // Also we don't want FSharp interactive debugging to mess our expressions:
    | ExpressionType.MemberAccess, ( :? MemberExpression as me) 
        when me <> null && me.Expression=null && me.Member.DeclaringType.Name.ToUpper().StartsWith("FSI_") -> 
            match me.Member with 
            | :? PropertyInfo as p when p.PropertyType.IsValueType -> Expression.Constant(p.GetValue(p) :?> IComparable, p.PropertyType) :> Expression
            | _ -> e
    | _ -> e

/// ------------------------------------- ///

let reductionMethods = 
    [``evaluate FSI constants``; 
     ``replace constant comparison``; ``remove AnonymousType``; ``cut not used condition``; ``not false is true``;
     (*associate;*) commute; (*distribute;*) gather; identity; annihilate; absorb; idempotence; complement; doubleNegation; deMorgan]

/// Does reductions just for a current node.
let rec doReduction (exp:Expression) =
    if exp = Unchecked.defaultof<Expression> then exp else 
    let opt = reductionMethods |> Seq.fold(fun acc f -> f(acc)) exp
    match opt = exp with
    | true -> exp
    | false -> doReduction opt

 /// ------------------------------------- ///

/// Expression tree visitor: go through the whole expression tree.
let rec visit (exp:Expression): Expression =
    //bottom up:
    if exp = null then null else
    let e1 = visitchilds exp
    let e2 = doReduction e1
    e2

and visitchilds (e:Expression): Expression =
    if e = null then null else
    match e with
    | (:? UnaryExpression as e)       -> upcast Expression.MakeUnary(e.NodeType, visit e.Operand,e.Type,e.Method)
    | (:? BinaryExpression as e)      -> upcast Expression.MakeBinary(e.NodeType, visit e.Left, visit e.Right)
    | (:? TypeBinaryExpression as e)  -> upcast Expression.TypeIs(visit e.Expression, e.TypeOperand)
    | (:? ConditionalExpression as e) -> upcast Expression.Condition(visit e.Test, visit e.IfTrue, visit e.IfFalse)
    | (:? ConstantExpression as e)    -> upcast e
    | (:? ParameterExpression as e)   -> upcast e
    | (:? MemberExpression as e)      -> upcast Expression.MakeMemberAccess(visit e.Expression, e.Member)
    | (:? MethodCallExpression as e)  -> upcast Expression.Call(visit e.Object, e.Method, e.Arguments |> Seq.map(fun a -> visit a))
    | (:? LambdaExpression as e)      -> upcast Expression.Lambda(e.Type, visit e.Body, e.Parameters)
    | (:? NewExpression as e) when e.Members = null -> 
                                         upcast Expression.New(e.Constructor, e.Arguments |> Seq.map(fun a -> visit a))
    | (:? NewExpression as e) when e.Members <> null -> 
                                         upcast Expression.New(e.Constructor, e.Arguments |> Seq.map(fun a -> visit a), e.Members)
    | (:? NewArrayExpression as e) when e.NodeType = ExpressionType.NewArrayInit ->
                                         upcast Expression.NewArrayBounds(e.Type.GetElementType(), e.Expressions |> Seq.map(fun e -> visit e))
    | (:? NewArrayExpression as e)    -> upcast Expression.NewArrayInit(e.Type.GetElementType(), e.Expressions |> Seq.map(fun e -> visit e))
    | (:? InvocationExpression as e)  -> upcast Expression.Invoke(visit e.Expression, e.Arguments |> Seq.map(fun a -> visit a))
    | (:? MemberInitExpression as e)  -> upcast Expression.MemberInit( (visit e.NewExpression) :?> NewExpression , e.Bindings) //probably shoud visit also bindings
    | (:? ListInitExpression as e)    -> upcast Expression.ListInit( (visit e.NewExpression) :?> NewExpression, e.Initializers) //probably shoud visit also initialixers
    | _ -> failwith ("encountered unknown LINQ expression: " + e.NodeType.ToString() + " " + e.ToString())
