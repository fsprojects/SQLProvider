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

## What each database gives

Database | Functions | Procedures | Notes
--- | --- | --- | ---
SQL Server | `ctx.Functions` (scalar) | `ctx.Procedures` | Table-valued functions aren't listed.
PostgreSQL | `ctx.Functions` | `ctx.Procedures` (PostgreSQL 11+) | Called by schema and name. See the [PostgreSQL](postgresql.html) page.
MySQL | `ctx.Functions` | `ctx.Procedures` | A routine of another database of `Owner` is called by database and name.
Oracle | `ctx.Functions` | `ctx.Procedures`, `ctx.Packages` | Called by owner and name, so another schema's routines work with a grant.
Firebird | not listed | `ctx.Procedures` | A procedure's output parameters are the one row of `ResultSet`; a selectable procedure gives its rows there.

`Invoke` and `InvokeAsync` take the routine's IN and INOUT parameters in their order. A function's value is
`ReturnValue`; OUT and INOUT parameters are properties of the result, by their names.
