# PostgreSQL Provider

SQLProvider with Oracle is available via both NuGet-Packages:

* NuGet: [SQLProvider.PostgreSql](https://www.nuget.org/packages/SQLProvider.PostgreSql) - Fixed Npgsql. Doesn't need resolution path. TypeProvider class: `FSharp.Data.Sql.PostgreSql.SqlDataProvider`
* NuGet: [SQLProvider](https://www.nuget.org/packages/SQLProvider) - Dynamic version. TypeProvider class: `FSharp.Data.Sql.SqlDataProvider`

## Parameters

### DatabaseVendor

From the `FSharp.Data.Sql.Common.DatabaseProviderTypes` enumeration. For PostgreSQL,
use `Common.DatabaseProviderTypes.POSTGRESQL`.

```fsharp
let [<Literal>] dbVendor = Common.DatabaseProviderTypes.POSTGRESQL
```

### ConnectionString

A basic connection string used to connect to PostgreSQL instance; typical
connection strings for the driver apply here. See
[PostgreSQL Connecting Strings Documentation](http://www.npgsql.org/doc/connection-string-parameters.html)
for a complete list of connection string options.

```fsharp
let [<Literal>] connString = "Host=localhost;Database=test;Username=test;Password=test"
```

### ConnectionStringName

Instead of storing the connection string in the source code / `fsx` script, you
can store values in the `App.config` file.  This is the name of the
connectionString key/value pair stored in App.config (TODO: confirm file name).

```fsharp
// found in App.config (TOOD: confirm)
let [<Literal>] connexStringName = "DefaultConnectionString"
```

### Resolution Path

Path to search for assemblies containing database vendor-specific connections
and custom types. Type the path where `Npgsql.Data.dll` is stored.

```fsharp
let [<Literal>] resPath = @"C:\Projects\Libs\Npgsql"
```

### IndividualsAmount

Sets the count to load for each individual. See [individuals](individuals.html)
for further info.

```fsharp
let [<Literal>] indivAmount = 1000
```

### UseOptionTypes

If true, F# option types will be used in place of nullable database columns.
If false, you will always receive the default value of the column's type, even
if it is null in the database. You can also use VALUE_OPTION.

```fsharp
let [<Literal>] useOptTypes  = FSharp.Data.Sql.Common.NullableColumnType.OPTION
```

### Owner

Indicates the schema or schemas to which SqlProvider will try to provide access to.
Multiple schemas can be indicated and separated by commas or semicolons.
Defaults to "public".

```fsharp
let [<Literal>] owner = "public, admin, references"


type sql =
    SqlDataProvider<
        dbVendor,
        connString,
        "",         //ConnectionNameString can be left empty
        resPath,
        indivAmount,
        useOptTypes,
        owner>
```

## Functions and procedures

Functions are under `ctx.Functions` and procedures (PostgreSQL 11 and newer) under `ctx.Procedures`. Each has
`Invoke` and `InvokeAsync`, which take the routine's IN and INOUT parameters in their order.

```fsharp
let ctx = sql.GetDataContext()

// A function's value
let total = ctx.Functions.OrderTotal.Invoke(orderId).ReturnValue

// A function that returns a table or a refcursor gives its rows
let rows = ctx.Functions.OrdersOf.Invoke(customerId).ReturnValue

// A procedure; its INOUT and OUT parameters are properties of the result
ctx.Procedures.CloseOrder.Invoke orderId
let next = ctx.Procedures.NextNumber.Invoke("invoice", 0).number

```

What | How it is called
--- | ---
**Name** | By schema and name, so the schema needn't be on the connection's search path.
**Function** | `SELECT * FROM schema.function(...)`
**Procedure** | `CALL schema.procedure(...)`; an OUT parameter's place takes a NULL (PostgreSQL 14 and newer).
**Transaction** | Inside a `TransactionScope` the call runs in the scope's transaction. Outside one it runs in its own, so a procedure can't commit or roll back itself.
**Npgsql** | The same calls with every Npgsql version: SQLProvider writes the call's text itself.

A function that returns a set of one column (`RETURNS SETOF integer`, or a table of one column) gives its first row's
value; return a table of two or more columns to read the rows.
