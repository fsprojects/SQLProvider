module ExpressionEvaluationTests

// Values of parameterless expressions read without compiling (Patterns.evaluateExpression), and compiled lambdas
// cached by shape (CompiledLambdas.compile). Both are internal, so they are called by reflection.

open System
open System.Linq
open System.Linq.Expressions
open NUnit.Framework

let private flags = Reflection.BindingFlags.Static ||| Reflection.BindingFlags.Public ||| Reflection.BindingFlags.NonPublic
let private commonAssembly = typeof<FSharp.Data.Sql.Common.SqlEntity>.Assembly

let private invokeStatic (typeName: string) (name: string) (arguments: obj array) =
    let methodInfo = commonAssembly.GetType(typeName, true).GetMethod(name, flags)
    try methodInfo.Invoke(null, arguments)
    with :? Reflection.TargetInvocationException as ex ->
        Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException).Throw()
        null

/// The expression's value, read by reflection or interpreted.
let private evaluate (e: Expression) = invokeStatic "FSharp.Data.Sql.Patterns" "evaluateExpression" [| box e |]

/// The expression's value when it is read by reflection alone; ValueNone when it would be interpreted.
let private readByReflection (e: Expression) =
    invokeStatic "FSharp.Data.Sql.Patterns" "tryReadValue" [| box e |] :?> obj voption

let private compile (lambda: LambdaExpression) =
    invokeStatic "FSharp.Data.Sql.QueryExpression.CompiledLambdas" "compile" [| box lambda |] :?> Delegate

let private cachedShapes () =
    invokeStatic "FSharp.Data.Sql.QueryExpression.CompiledLambdas" "cachedShapes" [||] :?> int

[<Struct>]
type Point = { X: int; Y: int }

type Holder(name: string, count: int, point: Point) =
    member _.Name = name
    member _.Count = count
    member _.Point = point
    member _.Failing: string = invalidOp "getter failed"

/// Closure classes as C# generates them: a nested lambda's class holds the outer one in a field.
type OuterClosure() =
    [<DefaultValue>] val mutable Ids: int list

type InnerClosure() =
    [<DefaultValue>] val mutable Outer: OuterClosure
    [<DefaultValue>] val mutable Name: string

type Statics =
    static member Greeting = "hello"
    static member Fail() : string = invalidOp "method failed"

type ToExpression =
    static member Of(e: Expression<Func<'T>>) = e

let private field (owner: Expression) (name: string) = Expression.Field(owner, name) :> Expression
let private property (owner: Expression) (name: string) = Expression.Property(owner, name) :> Expression
let private constant (value: obj) = Expression.Constant value :> Expression

[<Test; SqlCheck.NoSql>]
let ``constants are read as they are``() =
    Assert.AreEqual(ValueSome(box 42), readByReflection (constant 42))
    Assert.AreEqual(ValueSome(box "text"), readByReflection (constant "text"))
    Assert.AreEqual(ValueSome(null: obj), readByReflection (Expression.Constant(null, typeof<string>)))

[<Test; SqlCheck.NoSql>]
let ``properties and fields of a constant are read by reflection``() =
    let holder = Holder("name", 3, { X = 1; Y = 2 })
    Assert.AreEqual(ValueSome(box "name"), readByReflection (property (constant holder) "Name"))
    Assert.AreEqual(ValueSome(box 3), readByReflection (property (constant holder) "Count"))
    let closure = InnerClosure(Name = "closure")
    Assert.AreEqual(ValueSome(box "closure"), readByReflection (field (constant closure) "Name"))

[<Test; SqlCheck.NoSql>]
let ``a member whose value is null reads null``() =
    let holder = Holder(null, 0, { X = 0; Y = 0 })
    Assert.AreEqual(ValueSome(null: obj), readByReflection (property (constant holder) "Name"))
    Assert.AreEqual(ValueSome(null: obj), readByReflection (field (constant (InnerClosure())) "Outer"))

[<Test; SqlCheck.NoSql>]
let ``a member of null throws as compiled code does``() =
    let read = property (Expression.Constant(null, typeof<Holder>)) "Name"
    Assert.Throws<NullReferenceException>(fun () -> evaluate read |> ignore) |> ignore

[<Test; SqlCheck.NoSql>]
let ``members of a Nullable give what compiled code gives``() =
    let none = Expression.Constant(null, typeof<Nullable<int>>)
    Assert.AreEqual(ValueSome(box false), readByReflection (property none "HasValue"))
    Assert.Throws<InvalidOperationException>(fun () -> evaluate (property none "Value") |> ignore) |> ignore
    let some = Expression.Constant(Nullable 5, typeof<Nullable<int>>)
    Assert.AreEqual(ValueSome(box true), readByReflection (property some "HasValue"))
    Assert.AreEqual(ValueSome(box 5), readByReflection (property some "Value"))

[<Test; SqlCheck.NoSql>]
let ``members of value types are read``() =
    let point = { X = 5; Y = 7 }
    Assert.AreEqual(ValueSome(box 5), readByReflection (property (constant point) "X"))
    let holder = Holder("h", 1, point)
    Assert.AreEqual(ValueSome(box 7), readByReflection (property (property (constant holder) "Point") "Y"))

[<Test; SqlCheck.NoSql>]
let ``static members are read``() =
    let staticProperty = Expression.Property(null, typeof<Statics>.GetProperty "Greeting")
    Assert.AreEqual(ValueSome(box "hello"), readByReflection staticProperty)
    let staticField = Expression.Field(null, typeof<String>.GetField "Empty")
    Assert.AreEqual(ValueSome(box ""), readByReflection staticField)
    let literalField = Expression.Field(null, typeof<Int32>.GetField "MaxValue")
    Assert.AreEqual(ValueSome(box Int32.MaxValue), readByReflection literalField)

[<Test; SqlCheck.NoSql>]
let ``nested closures are read through their member chain``() =
    let inner = InnerClosure(Outer = OuterClosure(Ids = [ 1; 2; 3 ]))
    let ids = field (field (constant inner) "Outer") "Ids"
    Assert.AreEqual(ValueSome(box [ 1; 2; 3 ]), readByReflection ids)

[<Test; SqlCheck.NoSql>]
let ``values captured by F# lambdas are evaluated``() =
    let outer (x: int) =
        let y = x + 1
        ToExpression.Of(fun () -> (x, y))
    let lambda = outer 41
    Assert.AreEqual(box (41, 42), evaluate lambda.Body)
    let ids = [ "a"; "b" ]
    Assert.AreEqual(box ids, evaluate (ToExpression.Of(fun () -> ids)).Body)

[<Test; SqlCheck.NoSql>]
let ``conversions are read, and ones that compute a value give what compiled code gives``() =
    Assert.AreEqual(ValueSome(box 5), readByReflection (Expression.Convert(constant 5, typeof<obj>)))
    Assert.AreEqual(ValueSome(box 5), readByReflection (Expression.Convert(constant 5, typeof<Nullable<int>>)))
    Assert.AreEqual(ValueSome(box 5), readByReflection (Expression.Convert(Expression.Constant(box 5, typeof<obj>), typeof<int>)))
    Assert.AreEqual(ValueSome(null: obj), readByReflection (Expression.Convert(Expression.Constant(null, typeof<obj>), typeof<string>)))
    Assert.AreEqual(ValueSome(box 5L), readByReflection (Expression.Convert(constant 5, typeof<int64>)))
    Assert.AreEqual(ValueSome(box 2), readByReflection (Expression.Convert(constant 2.7, typeof<int>)))
    Assert.AreEqual(ValueSome(box DayOfWeek.Friday), readByReflection (Expression.Convert(constant 5, typeof<DayOfWeek>)))
    Assert.Throws<OverflowException>(fun () -> evaluate (Expression.ConvertChecked(constant 300, typeof<byte>)) |> ignore) |> ignore
    Assert.Throws<InvalidCastException>(fun () -> evaluate (Expression.Convert(Expression.Constant(box 5, typeof<obj>), typeof<int64>)) |> ignore) |> ignore
    Assert.Throws<NullReferenceException>(fun () -> evaluate (Expression.Convert(Expression.Constant(null, typeof<obj>), typeof<int>)) |> ignore) |> ignore
    Assert.AreEqual(ValueSome(box "s"), readByReflection (Expression.TypeAs(Expression.Constant(box "s", typeof<obj>), typeof<string>)))
    Assert.AreEqual(ValueSome(null: obj), readByReflection (Expression.TypeAs(Expression.Constant(box 5, typeof<obj>), typeof<string>)))

[<Test; SqlCheck.NoSql>]
let ``a quote evaluates to its expression``() =
    let lambda = Expression.Lambda<Func<int>>(constant 1)
    match readByReflection (Expression.Quote lambda) with
    | ValueSome value -> Assert.AreSame(lambda, value)
    | ValueNone -> Assert.Fail "a quote is read without evaluating"

[<Test; SqlCheck.NoSql>]
let ``a getter's exception is raised as it is``() =
    let holder = Holder("h", 1, { X = 0; Y = 0 })
    Assert.Throws<InvalidOperationException>(fun () -> evaluate (property (constant holder) "Failing") |> ignore) |> ignore

[<Test; SqlCheck.NoSql>]
let ``other expressions are interpreted``() =
    let sum = Expression.Add(constant 1, constant 2)
    Assert.AreEqual(ValueNone, readByReflection sum)
    Assert.AreEqual(box 3, evaluate sum)
    let created = Expression.New(typeof<DateTime>.GetConstructor [| typeof<int>; typeof<int>; typeof<int> |], constant 2000, constant 1, constant 2)
    Assert.AreEqual(ValueNone, readByReflection created)
    Assert.AreEqual(box (DateTime(2000, 1, 2)), evaluate created)

[<Test; SqlCheck.NoSql>]
let ``static and instance method calls on readable values are read by reflection``() =
    let concat = Expression.Call(typeof<String>.GetMethod("Concat", [| typeof<string>; typeof<string> |]), constant "a", constant "b")
    Assert.AreEqual(ValueSome(box "ab"), readByReflection concat)
    let holder = Holder("name", 1, { X = 0; Y = 0 })
    let upper = Expression.Call(property (constant holder) "Name", typeof<string>.GetMethod("ToUpperInvariant"))
    Assert.AreEqual(ValueSome(box "NAME"), readByReflection upper)
    let addDays = Expression.Call(constant (DateTime(2026, 1, 1)), typeof<DateTime>.GetMethod("AddDays"), constant 1.0)
    Assert.AreEqual(ValueSome(box (DateTime(2026, 1, 2))), readByReflection addDays)
    let voidCall = Expression.Call(typeof<GC>.GetMethod("KeepAlive"), Expression.Constant(null, typeof<obj>))
    Assert.AreEqual(ValueSome(null: obj), readByReflection voidCall)

/// Queryable.Where over ints with a quoted predicate comparing the item to the given expression.
let private whereGreaterThan (limit: Expression) =
    let x = Expression.Parameter(typeof<int>, "x")
    let predicate = Expression.Lambda<Func<int, bool>>(Expression.GreaterThan(x, limit), x)
    let where = typeof<Queryable>.GetMethods() |> Array.find (fun m -> m.Name = "Where" && m.GetParameters().[1].ParameterType.GetGenericArguments().[0].GetGenericArguments().Length = 2)
    Expression.Call(where.MakeGenericMethod typeof<int>, constant ([ 1; 2; 3 ].AsQueryable()), Expression.Quote predicate)

[<Test; SqlCheck.NoSql>]
let ``a call with a quoted argument passes the expression``() =
    match readByReflection (whereGreaterThan (constant 1)) with
    | ValueSome (:? IQueryable<int> as filtered) -> Assert.AreEqual([ 2; 3 ], filtered |> Seq.toList)
    | other -> Assert.Fail($"a quoted argument is read without evaluating, got %A{other}")

[<Test; SqlCheck.NoSql>]
let ``a quote that uses an outer lambda's parameter is not evaluated``() =
    let call = whereGreaterThan (Expression.Parameter(typeof<int>, "outer"))
    Assert.AreEqual(ValueNone, readByReflection call)
    Assert.Throws<InvalidOperationException>(fun () -> evaluate call |> ignore) |> ignore

type Counter() =
    let mutable calls = 0
    member _.Calls = calls
    member _.Next() =
        calls <- calls + 1
        calls
    member _.Missing() : string =
        calls <- calls + 1
        null

[<Test; SqlCheck.NoSql>]
let ``a captured method is called once``() =
    let call (counter: Counter) (name: string) = Expression.Call(constant counter, typeof<Counter>.GetMethod name) :> Expression
    let max = typeof<Math>.GetMethod("Max", [| typeof<int>; typeof<int> |])
    let cases: (string * (Counter -> Expression)) list =
        [ "under a conversion that computes a value", fun c -> Expression.Convert(call c "Next", typeof<int64>)
          "beside an argument that is interpreted", fun c -> Expression.Call(max, call c "Next", Expression.Negate(constant 4))
          "before a member of its null result", fun c -> Expression.Property(call c "Missing", "Length") ]
    for name, build in cases do
        let counter = Counter()
        try evaluate (build counter) |> ignore
        with :? NullReferenceException -> ()
        Assert.AreEqual(1, counter.Calls, name)

[<Test; SqlCheck.NoSql>]
let ``calls left to the interpreter``() =
    let point = { X = 1; Y = 2 }
    let structCall = Expression.Call(constant point, typeof<Point>.GetMethod("ToString", Type.EmptyTypes))
    Assert.AreEqual(ValueNone, readByReflection structCall)
    Assert.AreEqual(box (point.ToString()), evaluate structCall)
    let unreadableArgument = Expression.Call(typeof<Math>.GetMethod("Abs", [| typeof<int> |]), Expression.Negate(constant 4))
    Assert.AreEqual(ValueNone, readByReflection unreadableArgument)
    Assert.AreEqual(box 4, evaluate unreadableArgument)

[<Test; SqlCheck.NoSql>]
let ``a method of null throws as compiled code does``() =
    let call = Expression.Call(Expression.Constant(null, typeof<string>), typeof<string>.GetMethod("ToUpperInvariant"))
    Assert.Throws<NullReferenceException>(fun () -> evaluate call |> ignore) |> ignore

[<Test; SqlCheck.NoSql>]
let ``a method's exception is raised as it is``() =
    let call = Expression.Call(typeof<Statics>.GetMethod "Fail")
    Assert.Throws<InvalidOperationException>(fun () -> evaluate call |> ignore) |> ignore

let private addConstant (value: int) =
    let x = Expression.Parameter(typeof<int>, "x")
    Expression.Lambda<Func<int, int>>(Expression.Add(x, Expression.Constant value), x)

[<Test; SqlCheck.NoSql>]
let ``lambdas that differ only in constants share one compiled shape``() =
    let first = compile (addConstant 1000001) :?> Func<int, int>
    let before = cachedShapes ()
    let second = compile (addConstant 1000002) :?> Func<int, int>
    Assert.AreEqual(before, cachedShapes ())
    Assert.AreEqual(1000011, first.Invoke 10)
    Assert.AreEqual(1000012, second.Invoke 10)

[<Test; SqlCheck.NoSql>]
let ``lambdas of other shapes are not mixed up``() =
    let x = Expression.Parameter(typeof<int>, "x")
    let multiply = Expression.Lambda<Func<int, int>>(Expression.Multiply(x, Expression.Constant 3), x)
    Assert.AreEqual(30, (compile multiply :?> Func<int, int>).Invoke 10)
    Assert.AreEqual(13, (compile (addConstant 3) :?> Func<int, int>).Invoke 10)
    let a, b = Expression.Parameter(typeof<int>, "a"), Expression.Parameter(typeof<int>, "b")
    let aMinusB = Expression.Lambda<Func<int, int, int>>(Expression.Subtract(a, b), a, b)
    let bMinusA = Expression.Lambda<Func<int, int, int>>(Expression.Subtract(b, a), a, b)
    Assert.AreEqual(7, (compile aMinusB :?> Func<int, int, int>).Invoke(10, 3))
    Assert.AreEqual(-7, (compile bMinusA :?> Func<int, int, int>).Invoke(10, 3))

[<Test; SqlCheck.NoSql>]
let ``a constant used twice and two constants of one value are each bound``() =
    let x = Expression.Parameter(typeof<int>, "x")
    let shared = Expression.Constant 2000001
    let twice = Expression.Lambda<Func<int, int>>(Expression.Add(x, Expression.Add(shared, shared)), x)
    let distinct = Expression.Lambda<Func<int, int>>(Expression.Add(x, Expression.Add(Expression.Constant 2000001, Expression.Constant 5)), x)
    Assert.AreEqual(4000002, (compile twice :?> Func<int, int>).Invoke 0)
    Assert.AreEqual(2000006, (compile distinct :?> Func<int, int>).Invoke 0)

[<Test; SqlCheck.NoSql>]
let ``compiled lambdas keep reference, null and nullable constants``() =
    let s = Expression.Parameter(typeof<string>, "s")
    let concat = typeof<String>.GetMethod("Concat", [| typeof<string>; typeof<string> |])
    let prefixed (prefix: string) = Expression.Lambda<Func<string, string>>(Expression.Call(concat, Expression.Constant(prefix, typeof<string>), s), s)
    Assert.AreEqual("ab", (compile (prefixed "a") :?> Func<string, string>).Invoke "b")
    Assert.AreEqual("b", (compile (prefixed null) :?> Func<string, string>).Invoke "b")
    let n = Expression.Parameter(typeof<Nullable<int>>, "n")
    let orDefault (value: Nullable<int>) = Expression.Lambda<Func<Nullable<int>, Nullable<int>>>(Expression.Coalesce(n, Expression.Constant(value, typeof<Nullable<int>>)), n)
    Assert.AreEqual(Nullable 4, (compile (orDefault (Nullable 4)) :?> Func<Nullable<int>, Nullable<int>>).Invoke(Nullable()))
    Assert.AreEqual(Nullable(), (compile (orDefault (Nullable())) :?> Func<Nullable<int>, Nullable<int>>).Invoke(Nullable()))

[<Test; SqlCheck.NoSql>]
let ``a lambda holding a quote is compiled without the cache``() =
    let inner = Expression.Lambda<Func<int>>(Expression.Constant 3000001)
    let quoting = Expression.Lambda<Func<Expression>>(Expression.Convert(Expression.Quote inner, typeof<Expression>))
    let before = cachedShapes ()
    let quoted = (compile quoting :?> Func<Expression>).Invoke() :?> Expression<Func<int>>
    Assert.AreEqual(before, cachedShapes ())
    Assert.AreEqual(3000001, quoted.Compile().Invoke())
