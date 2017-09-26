#r @"..\..\bin\net451\FSharp.Data.SqlProvider.dll" 
//#r @"F:\dropbox\SqlProvider\bin\net451\Debug\FSharp.Data.SqlProvider.dll"
module GraphViz  =
//http://www.graphviz.org/doc/info/attrs.html#d:arrowtail
// script for helping to visualise crazy LINQ expression tress with graphviz

   open System
   open System.Text
   open System.Diagnostics
   open System.Linq.Expressions
   open System.Collections.Generic

   open System
   open System.Linq.Expressions
   open System.Reflection
   open FSharp.Data.Sql

   let (|MethodCall|_|) (e:Expression) = 
       match e.NodeType, e with 
       | ExpressionType.Call, (:? MethodCallExpression as e) -> 
           Some ((match e.Object with null -> None | obj -> Some obj), e.Method, Seq.toList e.Arguments)
       | _ -> None

   let (|New|_|) (e:Expression) = 
       match e.NodeType, e with 
       | ExpressionType.New, (:? NewExpression as e) -> 
           Some (Seq.toList e.Arguments, e.Constructor)
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

   let (|Constant|_|) (e:Expression) = 
       match e.NodeType, e with 
       | ExpressionType.Constant, (:? ConstantExpression as ce) -> Some (ce.Value, ce.Type)
       | _ -> None

   let (|Convert|_|)(e:Expression) =
       match e.NodeType, e with
       | ExpressionType.Convert, (:? UnaryExpression as ue) -> Some(ue)
       | _ -> None

   let (|ConstantOrNullableConstant|_|) (e:Expression) = 
       match e.NodeType, e with 
       | ExpressionType.Constant, (:? ConstantExpression as ce) -> Some(ce.Type,Some(ce.Value))
       | ExpressionType.Convert, (:? UnaryExpression as ue ) -> 
           match ue.Operand with
           | :? ConstantExpression as ce -> if ce.Value = null then Some(ce.Type,None) else Some(ce.Type,Some(ce.Value))
           | :? NewExpression as ne -> Some(ne.Constructor.DeclaringType,Some(Expression.Lambda(ne).Compile().DynamicInvoke()))
           | _ -> None
       | _ -> None

   let (|ParamName|_|) (e:Expression) = 
       match e.NodeType, e with 
       | ExpressionType.Parameter, (:? ParameterExpression as pe) ->  Some pe.Name
       | _ -> None    
    
   let (|Lambda|_|) (e:Expression) = 
       match e.NodeType, e with 
       | ExpressionType.Lambda, (:? LambdaExpression as ce) ->  Some (Seq.toList ce.Parameters, ce.Body)
       | _ -> None

   let (|Quote|_|) (e:Expression) = 
       match e.NodeType, e with 
       | ExpressionType.Quote, (:? UnaryExpression as ce) ->  Some ce.Operand
       | _ -> None

   let (|AndAlso|_|) (e:Expression) =
       match e.NodeType, e with
       | ExpressionType.AndAlso, ( :? BinaryExpression as be) -> Some(be.Left,be.Right)
       | _ -> None
    
   let (|OrElse|_|) (e:Expression) =
       match e.NodeType, e with
       | ExpressionType.OrElse, ( :? BinaryExpression as be) -> Some(be.Left,be.Right)
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

   let dotExe = @"C:\Program Files (x86)\Graphviz2.36\bin\dot.exe"
   let generate text file = 
      let temp = System.IO.Path.GetTempFileName()
      let args = sprintf "%s -Tsvg -o%s" temp file
      System.IO.File.WriteAllText(temp,text)
      let p = new Process(StartInfo=ProcessStartInfo(FileName=dotExe,Arguments=args)) 
      p.Start() |> ignore
      p.WaitForExit()
      System.IO.File.Delete temp   
      let newFile = file.Replace(".tmp", ".svg")
      System.IO.File.Move(file,newFile)
      let p = new Process(StartInfo=ProcessStartInfo(FileName=newFile)) 
      p.Start() |> ignore
      System.Threading.Thread.Sleep(500)

   let toGraph (e:Expression) = 
      let sb = StringBuilder("digraph G {\r\nsplines=\"compound\"\r\nnode [shape=record];\r\n")
      let (~~) (text:string) = sb.Append text |> ignore
      let (~~~) (text:string) = sb.AppendLine text |> ignore

      let index = ref 1
      let nextIndex() =
         let res = (!index).ToString()
         incr index
         res
      let rec eval (e:Expression) =
         let processArgs args parentName trailingText =
            args
            |> List.iteri(fun i e ->
               let lName = (sprintf "<%i> arg%i" (i+1) i)
               ~~ (sprintf "|%s" lName))
            ~~~ trailingText
            args
            |> List.map eval
            |> List.iteri(fun i e ->
               let lName = (sprintf "%i" (i+1))
               ~~~ (sprintf "%s:%s -> %s:0;" parentName lName e))
         match e with 
         | Quote(e) ->
            let name = ("Quote" + nextIndex())   
            ~~~ (sprintf "%s %s" name (sprintf "[label=\"<0> Quote\"]"))
            let pName = eval e
            ~~~ (sprintf "%s:0 -> %s:0;" name pName)
            name
         | MethodCall(o,meth,args) ->
            let name = (meth.Name  + nextIndex())            
            ~~ (sprintf "%s %s" name (sprintf "[label=\"{<0> MethodCall&#92;n%s|<f0> %s}" meth.Name (if o.IsSome then "Instance" else "Static")))
            processArgs args name "\"];"
            match o with
            | Some o -> 
               let o = eval o
               ~~~ (sprintf "%s:%s -> %s:0;" name "f0" o)
            | None -> () 
            name
         | Lambda(args,ex) ->
            let name = "Lambda" + nextIndex() 
            ~~ (sprintf "%s %s" name "[label=\"<0> Lambda")
            processArgs args name "|<ex> Exp\"];"       
            let ex = eval ex
            ~~~ (sprintf "%s:ex -> %s:0;" name ex)
            name
         | SqlCondOp(op,left,right) -> 
            let name = ("Op" + nextIndex())    
            ~~~ (sprintf "%s %s" name (sprintf "[label=\"<0> %s\"];" (op.ToString())))
            let l = eval left
            let r = eval right
            ~~~ (sprintf "%s:0 -> %s:0;" name l)
            ~~~ (sprintf "%s:0 -> %s:0;" name r)
            name
         | New(args,ci) ->
            let name = ("New" + nextIndex())            
            ~~ (sprintf "%s %s" name (sprintf "[label=\"<0> New&#92;n%s" ci.DeclaringType.Name))
            processArgs args name "\"];"
            name
         | NewArrayValues(values) -> 
            let name = ("NewArray" + nextIndex())     
            ~~ (sprintf "%s %s" name "[label=\"<0> NewArray")       
            values
            |> Seq.cast<obj>
            |> Seq.iteri(fun i e ->
               let lName = (sprintf "<%i> %s" (i+1) (e.ToString()))
               ~~ (sprintf "|%s" lName))
            ~~~  "\"];"     
            name
         | AndAlso(left,right) ->
            let name = ("And" + nextIndex())    
            ~~~ (sprintf "%s %s" name "[label=\"<0> AND\"];")
            let l = eval left
            let r = eval right
            ~~~ (sprintf "%s:0 -> %s:0;" name l)
            ~~~ (sprintf "%s:0 -> %s:0;" name r)
            name
         | OrElse(left,right) ->
            let name = ("Or" + nextIndex())    
            ~~~ (sprintf "%s %s" name "[label=\"<0> OR\"];")
            let l = eval left
            let r = eval right
            ~~~ (sprintf "%s:0 -> %s:0;" name l)
            ~~~ (sprintf "%s:0 -> %s:0;" name r)
            name
         | PropertyGet(o,pi) ->
            let name = "Prop" + nextIndex() 
            ~~~ (sprintf "%s %s" name (sprintf "[label=\"{<0> PropertyGet|<f0> %s } | <1> %s\"];" (if o.IsSome then "Instance" else "Static" ) (pi.Name+"\r\n"+pi.DeclaringType.Name  ) ))
            match o with
            | Some o -> 
               let o = eval o
               ~~~ (sprintf "%s:%s -> %s:0;" name "f0" o)
            | None -> () 
            name
         | ParamName(n) ->
            let name = "Param" + nextIndex() 
            ~~~ (sprintf "%s %s" name (sprintf "[label=\"<0> Param|<1> %s\"];" n ) )
            name
         | ConstantOrNullableConstant(t,v) ->
            let name = "Const" + nextIndex()
            let v = if v.IsNone then "NULL" else v.Value.ToString()
            let v = v.Replace("<","").Replace(">","")
            let v = v.Replace("FSharp.Data.Sql.Runtime.Common.SqlEntity","SqlEntity")
            let v = v.Replace("FSharp.Data.Sql.Runtime.QueryImplementation+SqlQueryable","SqlQueryable~")
            let v = v.Replace("Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject","Tuple")
            let v = v.Replace("FSharp.Data.Sql.Common.SqlEntity","SqlEntity")
            let v = if v.StartsWith("SqlDataProvider") then "SqlDataProvider" else v
            ~~~ (sprintf "%s %s" name (sprintf "[label=\"<0> Const|{<1> %s| <2> %s}\"];" t.Name v) )
            name
         | Convert(ue) ->
            let name = ("Convert" + nextIndex())   
            ~~~ (sprintf "%s %s" name (sprintf "[label=\"<0> Convert\"]"))
            let pName = eval ue.Operand
            ~~~ (sprintf "%s:0 -> %s:0;" name pName)
            name
         | _ ->  "UnrecognisedNode " + (nextIndex())  + e.NodeType.ToString()

      eval e |> ignore
      ~~ "}"
      let final = sb.ToString().Replace("->}","}")
      let temp = System.IO.Path.GetTempFileName()
      printfn "%s" <| final
      generate final temp
     


