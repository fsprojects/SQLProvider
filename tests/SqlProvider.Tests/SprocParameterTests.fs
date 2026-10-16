module SprocParameterTests

// Issue #877: stored procedures compiled against an offline ContextSchema
// have result-set / return-value columns in their parameter list.

open System.Data
open FSharp.Data.Sql.Schema
open FSharp.Data.Sql.Providers
open NUnit.Framework

let private intType = TypeMapping.Create(typeof<int>.ToString(), DbType.Int32)
let private cursorType = TypeMapping.Create(typeof<obj>.ToString(), DbType.Object, "cursor")

let private param name ordinal typeMapping direction =
    QueryParameter.Create(name, ordinal, typeMapping, direction)

[<Test>]
let ``MSSQL sproc call ignores result set columns from an offline schema``() =
    let resultSet = param "ResultSet" 0 cursorType ParameterDirection.Output
    // Offline the cached parameter list has the result set appended to the real parameters
    let inputParameters =
        [| param "@a" 1 intType ParameterDirection.Input
           param "@b" 2 intType ParameterDirection.InputOutput
           resultSet |]
    let values = [| box 1; box 2 |]

    let allParams, outps = MSSqlServer.executeSprocCommandCommon inputParameters [| resultSet |] values

    let names = allParams |> Array.map (fun (_,name,_) -> name)
    Assert.AreEqual([| "@a"; "@b" |], names)
    let values = allParams |> Array.map (fun (_,_,p) -> p.Value)
    Assert.AreEqual([| box 1; box 2 |], values)
    Assert.AreEqual(1, outps.Length)

[<Test>]
let ``MSSQL function call passes the return value parameter``() =
    let returnValue = param "ReturnValue" 0 intType ParameterDirection.ReturnValue
    let inputParameters =
        [| param "" 0 intType ParameterDirection.ReturnValue
           param "@a" 1 intType ParameterDirection.Input
           returnValue |]

    let allParams, _ = MSSqlServer.executeSprocCommandCommon inputParameters [| returnValue |] [| box 5 |]

    let names = allParams |> Array.map (fun (_,_,p) -> (p :?> IDataParameter).ParameterName)
    Assert.AreEqual([| "@RETURN_VALUE"; "@a" |], names)

[<Test>]
let ``distinctNamed drops repeated named parameters only``() =
    let parameters =
        [ param "" 0 intType ParameterDirection.ReturnValue
          param "@a" 1 intType ParameterDirection.Input
          param "@b" 2 intType ParameterDirection.InputOutput
          param "ResultSet" 0 cursorType ParameterDirection.Output
          param "ReturnValue" 0 intType ParameterDirection.ReturnValue
          param "@b" 2 intType ParameterDirection.InputOutput ]

    let result = SprocParameters.distinctNamed parameters

    Assert.AreEqual(
        [ "", ParameterDirection.ReturnValue
          "@a", ParameterDirection.Input
          "@b", ParameterDirection.InputOutput
          "ResultSet", ParameterDirection.Output
          "ReturnValue", ParameterDirection.ReturnValue ],
        result |> List.map (fun p -> p.Name, p.Direction))
