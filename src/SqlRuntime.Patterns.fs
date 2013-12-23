module internal FSharp.Data.Sql.Patterns

open System
open System.Linq.Expressions
open System.Reflection
open FSharp.Data.Sql

let (|MethodWithName|_|)   (s:string) (m:MethodInfo)   = if s = m.Name then Some () else None
let (|PropertyWithName|_|) (s:string) (m:PropertyInfo) = if s = m.Name then Some () else None

let (|MethodCall|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Call, (:? MethodCallExpression as e) -> 
        Some ((match e.Object with null -> None | obj -> Some obj), e.Method, Seq.toList e.Arguments)
    | _ -> None

let (|NewArrayValues|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.NewArrayInit, (:? NewArrayExpression as e) ->  Some(Expression.Lambda(e).Compile().DynamicInvoke() :?> Array)
    | _ -> None

let (|PropertyGet|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.MemberAccess, ( :? MemberExpression as e) -> 
        match e.Member with 
        | :? PropertyInfo as p -> Some ((match e.Expression with null -> None | obj -> Some obj), p)
        | _ -> None
    | _ -> None

let (|Convert|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Convert, (:? UnaryExpression as ue ) -> Some ue.Operand
    | _ -> None

let (|Constant|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Constant, (:? ConstantExpression as ce) -> Some (ce.Value, ce.Type)
    | _ -> None

let (|ConstantOrNullableConstant|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Constant, (:? ConstantExpression as ce) -> Some(Some(ce.Value))
    | ExpressionType.Convert, (:? UnaryExpression as ue ) -> 
        match ue.Operand with
        | :? ConstantExpression as ce -> if ce.Value = null then Some(None) else Some(Some(ce.Value))
        | :? NewExpression as ne -> Some(Some(Expression.Lambda(ne).Compile().DynamicInvoke()))
        | _ -> failwith "unsupported nullable expression"
    | _ -> None

let (|Bool|_|)   = function Constant((:? bool   as b),_) -> Some b | _ -> None
let (|String|_|) = function Constant((:? string as s),_) -> Some s | _ -> None
let (|Int|_|)    = function Constant((:? int    as i),_) -> Some i | _ -> None
    
let (|ParamName|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Parameter, (:? ParameterExpression as pe) ->  Some pe.Name
    | _ -> None    
    
let (|Lambda|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Lambda, (:? LambdaExpression as ce) ->  Some (Seq.toList ce.Parameters, ce.Body)
    | _ -> None

let (|OptionalQuote|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Quote, (:? UnaryExpression as ce) ->  ce.Operand
    | _ -> e

let (|OptionalFSharpOptionValue|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.MemberAccess, ( :? MemberExpression as e) -> 
        match e.Member with 
        | :? PropertyInfo as p when p.Name = "Value" && e.Member.DeclaringType.FullName.StartsWith("Microsoft.FSharp.Core.FSharpOption`1") -> e.Expression
        | _ -> upcast e 
    | _ -> e

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

let (|SqlColumnGet|_|) = function 
    | OptionalFSharpOptionValue(MethodCall(Some(o),((MethodWithName "GetColumn" as meth) | (MethodWithName "GetColumnOption" as meth)),[String key])) -> 
        match o with
        | :? MemberExpression as m  -> Some(m.Member.Name,key,meth.ReturnType) 
        | _ -> Some(String.Empty,key,meth.ReturnType) 
    | _ -> None

let (|OptionIsSome|_|) = function    
    | MethodCall(None,MethodWithName("get_IsSome"), [e] ) -> Some e
    | _ -> None

let (|OptionIsNone|_|) = function    
    | MethodCall(None,MethodWithName("get_IsNone"), [e] ) -> Some e
    | _ -> None

let (|SqlSpecialOpArr|_|) = function
    // for some crazy reason, simply using (|=|) stopped working ??
    | MethodCall(None,MethodWithName("op_BarEqualsBar"), [SqlColumnGet(ti,key,_); NewArrayValues values] ) -> Some(ti,ConditionOperator.In,   key,values)
    | MethodCall(None,MethodWithName("op_BarLessGreaterBar"),[SqlColumnGet(ti,key,_); NewArrayValues values] ) -> Some(ti,ConditionOperator.NotIn,key,values)
    | _ -> None
    
let (|SqlSpecialOp|_|) = function
    | MethodCall(None,MethodWithName("op_EqualsPercent"), [SqlColumnGet(ti,key,_); right]) -> Some(ti,ConditionOperator.Like,   key,Expression.Lambda(right).Compile().DynamicInvoke())
    | MethodCall(None,MethodWithName("op_LessGreaterPercent"),[SqlColumnGet(ti,key,_); right]) -> Some(ti,ConditionOperator.NotLike,key,Expression.Lambda(right).Compile().DynamicInvoke())
    // String  methods
    | MethodCall(Some(SqlColumnGet(ti,key,t)), MethodWithName "Contains", [right]) when t = typeof<string> -> 
        Some(ti,ConditionOperator.Like,key,box (sprintf "%%%O%%" (Expression.Lambda(right).Compile().DynamicInvoke())))
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
