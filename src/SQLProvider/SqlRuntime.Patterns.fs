module internal FSharp.Data.Sql.Patterns

open System
open System.Linq.Expressions
open System.Reflection
open FSharp.Data.Sql
open FSharp.Data.Sql.Schema

let (|MethodWithName|_|)   (s:string) (m:MethodInfo)   = if s = m.Name then Some () else None
let (|PropertyWithName|_|) (s:string) (m:PropertyInfo) = if s = m.Name then Some () else None

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

let (|SeqValuesQueryable|_|) (e:Expression) =
    let rec isQueryable (ty : Type) = 
        ty.FindInterfaces((fun ty _ -> ty = typeof<System.Linq.IQueryable>), null)
        |> (not << Seq.isEmpty)

    match (isQueryable e.Type) with
    | false -> None
    | true -> 
        let values = Expression.Lambda(e).Compile().DynamicInvoke() :?> System.Linq.IQueryable
        match values.GetType().Name = "SqlQueryable`1" with
        | true -> Some values
        | false -> None

let (|SeqValues|_|) (e:Expression) =
    let rec isEnumerable (ty : Type) = 
        ty.FindInterfaces((fun ty _ -> ty = typeof<System.Collections.IEnumerable>), null)
        |> (not << Seq.isEmpty)

    match (isEnumerable e.Type) with
    | false -> None
    | true ->
        // Here, if e is SQLQueryable<_>, we could avoid execution and nest the queries like
        // select x from xs where x in (select y from ys)
        // ...but instead we just execute the sub-query. Works when sub-query results are small.

        let values = Expression.Lambda(e).Compile().DynamicInvoke() :?> System.Collections.IEnumerable
        // Working with untyped IEnumerable so need to do a lot manually instead of using Seq
        // Work out the size the sequence
        let mutable count = 0
        for obj in values do
            count <- count + 1
        // Create and populate the array
        let array = Array.CreateInstance(typeof<System.Object>, count)
        let mutable i = 0
        for obj in values do
            array.SetValue(obj, i)
            i <- i + 1
        // Return the array
        Some(array)


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


let (|Constant|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Constant, (:? ConstantExpression as ce) -> Some (ce.Value, ce.Type)
    | _ -> None

let (|OptionNone|_|) (e: Expression) =
    match e with
    | MethodCall(None,MethodWithName("get_None"),[]) ->
        match e with
        | :? MethodCallExpression as e when e.Method.DeclaringType.FullName.StartsWith("Microsoft.FSharp.Core.FSharpOption`1") -> Some()
        | _ -> None
    | _ -> None

let (|NullConstant|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Constant, (:? ConstantExpression as ce) when ce.Value = null -> Some()
    | _ -> None

let (|ConstantOrNullableConstant|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Constant, (:? ConstantExpression as ce) -> Some(Some(ce.Value))
    | ExpressionType.Convert, (:? UnaryExpression as ue ) -> 
        match ue.Operand with
        | :? ConstantExpression as ce -> if ce.Value = null then Some(None) else Some(Some(ce.Value))
        | :? NewExpression as ne -> Some(Some(Expression.Lambda(ne).Compile().DynamicInvoke()))
        | _ -> failwith ("unsupported nullable expression " + e.ToString())
    | _ -> None

let (|BoolStrict|_|)   = function Constant((:? bool   as b),_) -> Some b | _ -> None
let (|String|_|) = function Constant((:? string as s),_) -> Some s | _ -> None
let (|Int|_|)    = function Constant((:? int    as i),_) -> Some i | _ -> None
let (|Float|_|)    = function Constant((:? float    as i),_) -> Some i | _ -> None
    
let rec (|Bool|_|) (e:Expression) = 
    match e.NodeType, e with
    | _, BoolStrict(b) -> Some b
    | ExpressionType.Not, (:? UnaryExpression as ue) -> 
        match ue.Operand with Bool x -> Some(not x) | _ -> None
    | _ -> None

let (|ParamName|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Parameter, (:? ParameterExpression as pe) ->  Some pe.Name
    | _ -> None    

let (|ParamWithName|_|) (s:String) (e:Expression) = 
    match e with 
    | ParamName(n) when s = n -> Some ()
    | _ -> None    
    
let (|Lambda|_|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Lambda, (:? LambdaExpression as ce) ->  Some (Seq.toList ce.Parameters, ce.Body)
    | _ -> None

let (|OptionalQuote|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Quote, (:? UnaryExpression as ce) ->  ce.Operand
    | _ -> e

let (|OptionalConvertOrTypeAs|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.Convert, (:? UnaryExpression as ue ) 
    | ExpressionType.TypeAs, (:? UnaryExpression as ue ) -> ue.Operand
    | ExpressionType.Call, (:? MethodCallExpression as e) when e.Method.Name = "Parse" && e.Arguments.Count = 1 ->
        // Don't do any magic, just: DateTime.Parse('2000-01-01') -> '2000-01-01'
        e.Arguments.[0]
    | _ -> e

let (|OptionalFSharpOptionValue|) (e:Expression) = 
    match e.NodeType, e with 
    | ExpressionType.MemberAccess, ( :? MemberExpression as e) -> 
        match e.Member with 
        | :? PropertyInfo as p when p.Name = "Value" && e.Member.DeclaringType.FullName.StartsWith("Microsoft.FSharp.Core.FSharpOption`1") -> e.Expression
        | _ -> upcast e
    | ExpressionType.Call, ( :? MethodCallExpression as e) ->
        if e.Method.Name = "Some" && e.Method.DeclaringType.FullName.StartsWith("Microsoft.FSharp.Core.FSharpOption`1")
        then e.Arguments.[0]
        else upcast e
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

let (|SqlPlainColumnGet|_|) = function
    | OptionalFSharpOptionValue(MethodCall(Some(o),((MethodWithName "GetColumn" as meth) | (MethodWithName "GetColumnOption" as meth)),[String key])) -> 
        match o with
        | :? MemberExpression as m  -> Some(m.Member.Name,KeyColumn key,meth.ReturnType) 
        | p when p.NodeType = ExpressionType.Parameter -> Some(String.Empty,KeyColumn key,meth.ReturnType) 
        | _ -> None
    | _ -> None

let decimalTypes = [| typeof<decimal>; typeof<float32>; typeof<double>; typeof<float>;
                      typeof<Option<decimal>>; typeof<Option<float32>>; typeof<Option<double>>; typeof<Option<float>> |]
let integerTypes = [| typeof<Int32>; typeof<Int64>; typeof<Int16>; typeof<int8>;typeof<UInt32>; typeof<UInt64>; typeof<UInt16>; typeof<uint8>;
                      typeof<Option<Int32>>; typeof<Option<Int64>>; typeof<Option<Int16>>; typeof<Option<int8>>; typeof<Option<UInt32>>; typeof<Option<UInt64>>; typeof<Option<UInt16>>; typeof<Option<uint8>> |]

let intType (typ:Type) = 
    if typ <> null && typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<Option<_>> then typeof<Option<int>>
    else typeof<int>

let rec (|SqlColumnGet|_|) (e:Expression) =  
    match e.NodeType, e with 
    | _, SqlPlainColumnGet(m, k, t) -> Some(m,k,t)

    // These are aggregations, e.g. GROUPBY, HAVING-clause
    | ExpressionType.MemberAccess, ( :? MemberExpression as me) when 
            me.Expression <> null && me.Expression.Type <> null && me.Expression.Type.Name <> null &&
            me.Expression.Type.Name.StartsWith("IGrouping")  -> 
        match me.Member with 
        | :? PropertyInfo as p when p.Name = "Key" -> Some(String.Empty, GroupColumn (KeyOp "",SqlColumnType.KeyColumn("Key")), p.DeclaringType) 
        | _ -> None
    | ExpressionType.Call, ( :? MethodCallExpression as e) when e.Arguments <> null && e.Arguments.Count = 1 && 
            e.Arguments.[0] <> null && e.Arguments.[0].Type <> null && e.Arguments.[0].Type.Name <> null &&
            e.Arguments.[0].Type.Name.StartsWith("IGrouping") ->
        if e.Arguments.[0].NodeType = ExpressionType.Parameter then
            let pn = match e.Arguments.[0] with :? ParameterExpression as p -> p.Name | _ -> e.Method.Name
            match e.Method.Name with
            | "Count" -> Some(String.Empty, GroupColumn (CountOp "",SqlColumnType.KeyColumn(pn)), e.Method.DeclaringType)
            | "Average" -> Some(String.Empty, GroupColumn (AvgOp "",SqlColumnType.KeyColumn(pn)), e.Method.DeclaringType)
            | "Min" -> Some(String.Empty, GroupColumn (MinOp "",SqlColumnType.KeyColumn(pn)), e.Method.DeclaringType)
            | "Max" -> Some(String.Empty, GroupColumn (MaxOp "",SqlColumnType.KeyColumn(pn)), e.Method.DeclaringType)
            | "Sum" -> Some(String.Empty, GroupColumn (SumOp "",SqlColumnType.KeyColumn(pn)), e.Method.DeclaringType)
            | _ -> None
        else 
            match e.Arguments.[0], e.Method.Name with
            | :? MemberExpression as m, "Count" -> Some(m.Member.Name, GroupColumn (CountOp "",SqlColumnType.KeyColumn(m.Member.Name)), e.Method.DeclaringType)
            | :? MemberExpression as m, "Average" -> Some(m.Member.Name, GroupColumn (AvgOp "",SqlColumnType.KeyColumn(m.Member.Name)), e.Method.DeclaringType)
            | :? MemberExpression as m, "Min" -> Some(m.Member.Name, GroupColumn (MinOp "",SqlColumnType.KeyColumn(m.Member.Name)), e.Method.DeclaringType)
            | :? MemberExpression as m, "Max" -> Some(m.Member.Name, GroupColumn (MaxOp "",SqlColumnType.KeyColumn(m.Member.Name)), e.Method.DeclaringType)
            | :? MemberExpression as m, "Sum" -> Some(m.Member.Name, GroupColumn (SumOp "",SqlColumnType.KeyColumn(m.Member.Name)), e.Method.DeclaringType)
            | _ -> None

    // These are canonical functions
    | _, OptionalFSharpOptionValue(MethodCall(Some(OptionalFSharpOptionValue(SqlColumnGet(alias, col, typ))), meth, par)) ->
        match typ with
        | t when t = typeof<System.String> || t= typeof<Option<System.String>> -> // String functions
            match meth.Name, par with
            | "Substring", [Int startPos] -> Some(alias, CanonicalOperation(CanonicalOp.Substring(SqlInt(startPos)), col), typ)
            | "Substring", [SqlColumnGet(al2,col2,typ2)] when integerTypes |> Seq.exists(fun t -> t = typ2) -> Some(alias, CanonicalOperation(CanonicalOp.Substring(SqlIntCol(al2,col2)), col), typ)
            | "Substring", [Int startPos; Int strLen] -> Some(alias, CanonicalOperation(CanonicalOp.SubstringWithLength(SqlInt(startPos),SqlInt(strLen)), col), typ)
            | "Substring", [SqlColumnGet(al2,col2,typ2); Int strLen] when integerTypes |> Seq.exists(fun t -> t = typ2) -> Some(alias, CanonicalOperation(CanonicalOp.SubstringWithLength(SqlIntCol(al2,col2),SqlInt(strLen)), col), typ)
            | "Substring", [Int startPos; SqlColumnGet(al2,col2,typ2)] when integerTypes |> Seq.exists(fun t -> t = typ2) -> Some(alias, CanonicalOperation(CanonicalOp.SubstringWithLength(SqlInt(startPos),SqlIntCol(al2,col2)), col), typ)
            | "Substring", [SqlColumnGet(al2,col2,ty2); SqlColumnGet(al3,col3,ty3)]  when integerTypes |> Seq.exists(fun t -> t = ty2) && integerTypes |> Seq.exists(fun t -> t = ty3) -> Some(alias, CanonicalOperation(CanonicalOp.SubstringWithLength(SqlIntCol(al2,col2),SqlIntCol(al3,col3)), col), typ)
            | "ToUpper", []
            | "ToUpperInvariant", [] -> Some(alias, CanonicalOperation(CanonicalOp.ToUpper, col), typ)
            | "ToLower", [] 
            | "ToLowerInvariant", [] -> Some(alias, CanonicalOperation(CanonicalOp.ToLower, col), typ)
            | "Trim", [] -> Some(alias, CanonicalOperation(CanonicalOp.Trim, col), typ)
            | "Length", [] -> Some(alias, CanonicalOperation(CanonicalOp.Length, col), intType typ)
            | "Replace", [String itm1; String itm2] when not(itm1.Contains("'") || itm2.Contains("'"))  -> Some(alias, CanonicalOperation(CanonicalOp.Replace(SqlStr(itm1), SqlStr(itm2)), col), typ)
            | "Replace", [SqlColumnGet(al2,col2,_); String itm2] when not(itm2.Contains("'"))  -> Some(alias, CanonicalOperation(CanonicalOp.Replace(SqlStrCol(al2,col2), SqlStr(itm2)), col), typ)
            | "Replace", [String itm1; SqlColumnGet(al2,col2,_)] when not(itm1.Contains("'"))  -> Some(alias, CanonicalOperation(CanonicalOp.Replace(SqlStr(itm1), SqlStrCol(al2,col2)), col), typ)
            | "Replace", [SqlColumnGet(al2,col2,_); SqlColumnGet(al3,col3,_)] -> Some(alias, CanonicalOperation(CanonicalOp.Replace(SqlStrCol(al2,col2), SqlStrCol(al3,col3)), col), typ)
            | "IndexOf", [String search] when not(search.Contains("'")) -> Some(alias, CanonicalOperation(CanonicalOp.IndexOf(SqlStr(search)), col), intType typ)
            | "IndexOf", [SqlColumnGet(al2,col2,_)] -> Some(alias, CanonicalOperation(CanonicalOp.IndexOf(SqlStrCol(al2,col2)), col), intType typ)
            | "IndexOf", [String search; Int startPos] when not(search.Contains("'")) -> Some(alias, CanonicalOperation(CanonicalOp.IndexOfStart(SqlStr(search), SqlInt(startPos)), col), intType typ)
            | "IndexOf", [SqlColumnGet(al2,col2,_); Int startPos] -> Some(alias, CanonicalOperation(CanonicalOp.IndexOfStart(SqlStrCol(al2,col2), SqlInt(startPos)), col), intType typ)
            | "IndexOf", [String search; SqlColumnGet(al2,col2,typ2)] when not(search.Contains("'")) && integerTypes |> Seq.exists(fun t -> t = typ2) -> Some(alias, CanonicalOperation(CanonicalOp.IndexOfStart(SqlStr(search), SqlIntCol(al2,col2)), col), intType typ)
            | "IndexOf", [SqlColumnGet(al2,col2,_); SqlColumnGet(al3,col3,typ2)] when integerTypes |> Seq.exists(fun t -> t = typ2) -> Some(alias, CanonicalOperation(CanonicalOp.IndexOfStart(SqlStrCol(al2,col2), SqlIntCol(al3,col3)), col), intType typ)
            | _ -> None
        | t when t = typeof<System.DateTime> || t = typeof<Option<System.DateTime>> -> // DateTime functions
            match meth.Name, par with
            | "AddYears", [Int x] -> Some(alias, CanonicalOperation(CanonicalOp.AddYears(SqlInt(x)), col), typ)
            | "AddYears", [SqlColumnGet(al2,col2,typ2)] when integerTypes |> Seq.exists(fun t -> t = typ2) -> Some(alias, CanonicalOperation(CanonicalOp.AddYears(SqlIntCol(al2,col2)), col), typ)
            | "AddMonths", [Int x] -> Some(alias, CanonicalOperation(CanonicalOp.AddMonths(x), col), typ)
            | "AddDays", [Float x] -> Some(alias, CanonicalOperation(CanonicalOp.AddDays(SqlFloat(x)), col), typ)
            | "AddDays", [OptionalConvertOrTypeAs(SqlColumnGet(al2,col2,typ2))] when integerTypes |> Seq.exists(fun t -> t = typ2) || decimalTypes |> Seq.exists(fun t -> t = typ2)  -> Some(alias, CanonicalOperation(CanonicalOp.AddDays(SqlNumCol(al2,col2)), col), typ)
            | "AddHours", [Float x] -> Some(alias, CanonicalOperation(CanonicalOp.AddHours(x), col), typ)
            | "AddMinutes", [Float x] -> Some(alias, CanonicalOperation(CanonicalOp.AddMinutes(SqlFloat(x)), col), typ)
            | "AddMinutes", [OptionalConvertOrTypeAs(SqlColumnGet(al2,col2,typ2))] when integerTypes |> Seq.exists(fun t -> t = typ2) || decimalTypes |> Seq.exists(fun t -> t = typ2)  -> Some(alias, CanonicalOperation(CanonicalOp.AddMinutes(SqlNumCol(al2,col2)), col), typ)
            | "AddSeconds", [Float x] -> Some(alias, CanonicalOperation(CanonicalOp.AddSeconds(x), col), typ)
            | _ -> None
        | _ -> None
    // These are canonical properties
    | _, OptionalFSharpOptionValue(PropertyGet(Some(OptionalFSharpOptionValue(SqlColumnGet(alias, col, typ))), meth)) -> 
        match typ with
        | t when t = typeof<System.String> || t = typeof<Option<System.String>> -> // String functions
            match meth.Name with
            | "Length" -> Some(alias, CanonicalOperation(CanonicalOp.Length, col), intType typ)
            | _ -> None
        | t when t = typeof<System.DateTime> || t = typeof<Option<System.DateTime>> -> // DateTime functions
            match meth.Name with
            | "Date" -> Some(alias, CanonicalOperation(CanonicalOp.Date, col), typ)
            | "Year" -> Some(alias, CanonicalOperation(CanonicalOp.Year, col), intType typ)
            | "Month" -> Some(alias, CanonicalOperation(CanonicalOp.Month, col), intType typ)
            | "Day" -> Some(alias, CanonicalOperation(CanonicalOp.Day, col), intType typ)
            | "Hour" -> Some(alias, CanonicalOperation(CanonicalOp.Hour, col), intType typ)
            | "Minute" -> Some(alias, CanonicalOperation(CanonicalOp.Minute, col), intType typ)
            | "Second" -> Some(alias, CanonicalOperation(CanonicalOp.Second, col), intType typ)
            | _ -> None
        | _ -> None
    // Numerical functions
    | _, OptionalFSharpOptionValue(MethodCall(None, meth, ([OptionalFSharpOptionValue(SqlColumnGet(alias, col, typ))] as par)))
        when ((meth.Name = "Abs" || meth.Name = "Ceil" || meth.Name = "Floor" || meth.Name = "Round" || meth.Name = "Truncate") && (decimalTypes |> Array.exists((=) typ))
            || (meth.Name = "Abs" && integerTypes |> Array.exists((=) typ))) -> 
            
            match meth.Name, par with
            | "Abs", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Abs, col), typ)
            | "Ceil", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Ceil, col), typ)
            | "Floor", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Floor, col), typ)
            | "Round", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Round, col), typ)
            | "Round", [_; Int decCount] -> Some(alias, CanonicalOperation(CanonicalOp.RoundDecimals(decCount), col), typ)
            | "Truncate", [_] -> Some(alias, CanonicalOperation(CanonicalOp.Truncate, col), typ)
            | _ -> failwith "Shouldn't hit"

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
            | _ -> failwith "Shouldn't hit"

        match be.Left, be.Right with
        | OptionalConvertOrTypeAs(OptionalFSharpOptionValue(SqlColumnGet(alias, col, typ))), OptionalConvertOrTypeAs(Constant(constVal,constTyp)) 
        | OptionalConvertOrTypeAs(Constant(constVal,constTyp)), OptionalConvertOrTypeAs(OptionalFSharpOptionValue(SqlColumnGet(alias, col, typ)))
            when (typ = constTyp || (typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<Option<_>> && typ.GenericTypeArguments.[0] = constTyp )
                 || be.Left.Type = be.Right.Type) ->  // Support only numeric and string math
                match typ with
                | t when (operation = "+" && (t = typeof<System.String> || t = typeof<System.Char> || t = typeof<Option<System.String>> || t = typeof<Option<System.Char>>)) -> 
                    let x = constVal :?> String
                    // As these are compiled to SQL-clause we avoid SQL-injection risk by this.
                    // These are not ment for dynamic data anyways...
                    if not(x.Contains "'") then 
                        // Standard SQL string concatenation is ||
                        Some(alias, CanonicalOperation(CanonicalOp.BasicMath("||", constVal), col), typ)
                    else None
                | t when (decimalTypes |> Seq.exists((=) t) || integerTypes |> Seq.exists((=) t)) ->
                        Some(alias, CanonicalOperation(CanonicalOp.BasicMath(operation, constVal), col), typ)
                | _ -> None
        | OptionalConvertOrTypeAs(OptionalFSharpOptionValue(SqlColumnGet(aliasLeft, colLeft, typLeft))), OptionalConvertOrTypeAs(OptionalFSharpOptionValue(SqlColumnGet(aliasRight, colRight, typRight))) 
            when (typLeft = typRight ||
                     (typLeft.IsGenericType && typLeft.GetGenericTypeDefinition() = typedefof<Option<_>> && typLeft.GenericTypeArguments.[0] = typRight ) ||
                     (typRight.IsGenericType && typRight.GetGenericTypeDefinition() = typedefof<Option<_>> && typRight.GenericTypeArguments.[0] = typLeft ) ||
                         be.Left.Type = be.Right.Type) -> 
                let opFix = 
                    match typLeft with
                    | t when ((t = typeof<System.String> || t = typeof<System.Char> || t = typeof<Option<System.String>> || t = typeof<Option<System.Char>>) && operation = "+") -> "||"
                    | _ -> operation
                Some(aliasLeft, CanonicalOperation(CanonicalOp.BasicMathOfColumns(opFix, aliasRight, colRight), colLeft), typLeft)
        | _ -> None
    //
    | ExpressionType.Call, (:? MethodCallExpression as e) when e.Method.Name = "Parse" && e.Arguments.Count = 1 && 
                           (e.Type = typeof<System.DateTime> || e.Type = typeof<Option<System.DateTime>>) ->
        // Don't do any magic, just: DateTime.Parse('2000-01-01') -> '2000-01-01'
        match e.Arguments.[0] with
        | SqlColumnGet(alias, col, typ) when typ = typeof<String> || typ = typeof<Option<String>> 
            -> Some(alias, col, e.Type)
        | _ -> None
    | _ -> None

let (|TupleSqlColumnsGet|_|) = function 
    | OptionalFSharpOptionValue(NewExpr(cons, args)) when cons.DeclaringType.Name.StartsWith("Tuple") || cons.DeclaringType.Name.StartsWith("AnonymousObject") ->
        let items = args |> List.choose(function
                            | SqlColumnGet(ti,key,t) -> Some(ti, key, t)
                            | _-> None)
        match items with
        | [] -> None
        | li -> Some li
    | _ -> None

let (|OptionIsSome|_|) = function    
    | MethodCall(None,MethodWithName("get_IsSome"), [e] ) -> Some e
    | _ -> None

let (|OptionIsNone|_|) = function    
    | MethodCall(None,MethodWithName("get_IsNone"), [e] ) -> Some e
    | _ -> None

let (|SqlSpecialOpArr|_|) = function
    // for some crazy reason, simply using (|=|) stopped working ??
    | MethodCall(None,MethodWithName("op_BarEqualsBar"), [SqlColumnGet(ti,key,_); SeqValues values]) -> Some(ti, ConditionOperator.In, key, values)
    | MethodCall(None,MethodWithName("op_BarLessGreaterBar"),[SqlColumnGet(ti,key,_); SeqValues values]) -> Some(ti, ConditionOperator.NotIn, key, values)
    | MethodCall(None,MethodWithName("Contains"), [SeqValues values; SqlColumnGet(ti,key,_)]) -> Some(ti, ConditionOperator.In, key, values)
    | _ -> None

let (|SqlSpecialOpArrQueryable|_|) = function
    // for some crazy reason, simply using (|=|) stopped working ??
    | MethodCall(None,MethodWithName("op_BarEqualsBar"), [SqlColumnGet(ti,key,_); SeqValuesQueryable values]) -> Some(ti, ConditionOperator.NestedIn, key, values)
    | MethodCall(None,MethodWithName("op_BarLessGreaterBar"),[SqlColumnGet(ti,key,_); SeqValuesQueryable values]) -> Some(ti, ConditionOperator.NestedNotIn, key, values)
    | MethodCall(None,MethodWithName("Contains"), [SeqValuesQueryable values; SqlColumnGet(ti,key,_)]) -> Some(ti, ConditionOperator.NestedIn, key, values)
    | _ -> None

    
let (|SqlSpecialOp|_|) = function
    | MethodCall(None,MethodWithName("op_EqualsPercent"), [SqlColumnGet(ti,key,_); right]) -> Some(ti,ConditionOperator.Like,   key,Expression.Lambda(right).Compile().DynamicInvoke())
    | MethodCall(None,MethodWithName("op_LessGreaterPercent"),[SqlColumnGet(ti,key,_); right]) -> Some(ti,ConditionOperator.NotLike,key,Expression.Lambda(right).Compile().DynamicInvoke())
    // String  methods
    | MethodCall(Some(OptionalFSharpOptionValue(SqlColumnGet(ti,key,t))), MethodWithName "Contains", [right]) when t = typeof<string> || t = typeof<Option<string>> -> 
        Some(ti,ConditionOperator.Like,key,box (sprintf "%%%O%%" (Expression.Lambda(right).Compile().DynamicInvoke())))
    | MethodCall(Some(OptionalFSharpOptionValue(SqlColumnGet(ti,key,t))), MethodWithName "StartsWith", [right]) when t = typeof<string> || t = typeof<Option<string>> -> 
        Some(ti,ConditionOperator.Like,key,box (sprintf "%O%%" (Expression.Lambda(right).Compile().DynamicInvoke())))
    | MethodCall(Some(OptionalFSharpOptionValue(SqlColumnGet(ti,key,t))), MethodWithName "EndsWith", [right]) when t = typeof<string> || t = typeof<Option<string>> -> 
        Some(ti,ConditionOperator.Like,key,box (sprintf "%%%O" (Expression.Lambda(right).Compile().DynamicInvoke())))
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

let (|SqlSpecialNegativeOpArr|_|) (e:Expression) = 
    match e.NodeType, e with
    | ExpressionType.Not, (:? UnaryExpression as ue) ->
        match ue.Operand with
        | MethodCall(None,MethodWithName("Contains"), [SeqValues values; SqlColumnGet(ti,key,_)]) -> Some(ti, ConditionOperator.NotIn, key, values)
        | _ -> None
    | _ -> None

let (|SqlSpecialNegativeOpArrQueryable|_|) (e:Expression) = 
    match e.NodeType, e with
    | ExpressionType.Not, (:? UnaryExpression as ue) ->
        match ue.Operand with
        | MethodCall(None,MethodWithName("Contains"), [SeqValuesQueryable values; SqlColumnGet(ti,key,_)]) -> Some(ti, ConditionOperator.NestedIn, key, values)
        | _ -> None
    | _ -> None
