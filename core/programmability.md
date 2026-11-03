# Programmability

```fsharp
type AdventureWorks = SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER, connStr>
let ctx = AdventureWorks.GetDataContext()
```

Querying views is just like querying tables. But executing a stored procedure or a function is a bit different:

Execute a function in the Adventure Works database

```fsharp
ctx.Functions.UfnGetSalesOrderStatusText.Invoke(0uy)
```

Execute a stored procedure in the Adventure Works database

```fsharp
ctx.Procedures.UspLogError.Invoke(1)
```

Example of executing a procedure with async and reading the results:

```fsharp
let uspGetManagerEmployees =
    async {
        let! res = ctx.Procedures.UspGetManagerEmployees.InvokeAsync 2
        let mapped = res.ResultSet |> Array.map(fun i -> i.ColumnValues |> Map.ofSeq)
        mapped |> Array.iter(fun i ->
            printfn "Name: %O, Level: %O" i.["FirstName"] i.["RecursionLevel"]
        )
    } |> Async.StartAsTask

    // Name: Roberto, Level: 1
    // Name: Rob, Level: 2
    // Name: Gail, Level: 2
    // ...
```
