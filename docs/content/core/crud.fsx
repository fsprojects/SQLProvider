(*** hide ***)
#r "../../../bin/lib/netstandard2.0/FSharp.Data.SqlProvider.Common.dll"
#r "../../../bin/lib/netstandard2.0/FSharp.Data.SqlProvider.dll"
(*** hide ***)
let [<Literal>] resolutionPath = __SOURCE_DIRECTORY__ + @"/../../files/sqlite"
(*** hide ***)
let [<Literal>] connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"\..\northwindEF.db;Version=3;Read Only=false;FailIfMissing=True;"

open FSharp.Data.Sql
(*** hide ***)
type sql  = SqlDataProvider<
                Common.DatabaseProviderTypes.SQLITE,
                connectionString,
                SQLiteLibrary=Common.SQLiteLibrary.SystemDataSQLite,
                ResolutionPath = resolutionPath,
                CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL
            >

(**
# CRUD sample
*)

open System


let ctx = sql.GetDataContext()

let orders = ctx.Main.Orders
let employees = ctx.Main.Employees

let customer = ctx.Main.Customers |> Seq.head
let employee = ctx.Main.Employees |> Seq.head
let now = DateTime.Now

(**
Create() has various overloads to make inserting records simple.
*)

(**
Create a new row
*)

let row = orders.Create()
row.CustomerId <- customer.CustomerId
row.EmployeeId <- employee.EmployeeId
row.Freight <- 10M
row.OrderDate <- now.AddDays(-1.0)
row.RequiredDate <- now.AddDays(1.0)
row.ShipAddress <- "10 Downing St"
row.ShipCity <- "London"
row.ShipName <- "Dragons den"
row.ShipPostalCode <- "SW1A 2AA"
row.ShipRegion <- "UK"
row.ShippedDate <- now

(**
Submit updates to the database
*)
ctx.SubmitUpdates()

(**
After updating, your item (row) will have the Id property.



You can also create with the longer \`\`Create(...)\`\`(parameters)-method like this:

*)

let emp = ctx.Main.Employees.``Create(FirstName, LastName)``("Don", "Syme")

(**

Delete the row
*)
row.Delete()

(**
Submit updates to the database
*)
ctx.SubmitUpdates()


(**Insert a list of records:
*)

type Employee = {
    FirstName:string
    LastName:string
}

let mvps1 = [
    {FirstName="Andrew"; LastName="Kennedy"};
    {FirstName="Mads"; LastName="Torgersen"};
    {FirstName="Martin";LastName="Odersky"};
]

mvps1
    |> List.map (fun x ->
                    let row = employees.Create()
                    row.FirstName <- x.FirstName
                    row.LastName <- x.LastName)

ctx.SubmitUpdates()

(**Or directly specify the fields:
*)

let mvps2 = [
    {FirstName="Byron"; LastName="Cook"};
    {FirstName="James"; LastName="Huddleston"};
    {FirstName="Xavier";LastName="Leroy"};
]

mvps2
    |> List.map (fun x ->
                   employees.Create(x.FirstName, x.LastName)
                    )

ctx.SubmitUpdates()

(** update a single row
    assuming Id is unique
*)

type Employee2 = {
    Id:int
    FirstName:string
    LastName:string
}

let updateEmployee (employee: Employee2) =
    let foundEmployeeMaybe = query {
        for p in ctx.Public.Employee2 do
        where (p.Id = employee.Id)
        select (Some p)
        exactlyOneOrDefault
    }
    match foundEmployeeMaybe with
    | Some foundEmployee ->
        foundEmployee.FirstName <- employee.FirstName
        foundEmployee.LastName <- employee.LastName
        ctx.SubmitUpdates()
    | None -> ()

let updateEmployee' (employee: Employee2) =
    query {
        for p in ctx.Public.Employee2 do
        where (p.Id = employee.Id)
    }
    |> Seq.iter( fun e ->
        e.FirstName <- employee.FirstName
        e.LastName <- employee.LastName
    )
    ctx.SubmitUpdates()

let john = {
  Id = 1
  FirstName = "John"
  LastName = "Doe" }

updateEmployee john
updateEmployee' john


(**Finally it is also possible to specify a seq of `string * obj`, which is precisely the
output of .ColumnValues:
*)

employees
    |> Seq.map (fun x ->
                employee.Create(x.ColumnValues)) // create twins
    |>  Seq.toList

let twins = ctx.GetUpdates() // Retrieve the FSharp.Data.Sql.Common.SqlEntity objects

ctx.ClearUpdates() // delete the updates
ctx.GetUpdates() // Get the updates
ctx.SubmitUpdates() // no record is added

(**

### Transactions and isolation level

`SubmitUpdates` / `SubmitUpdatesAsync` wrap the write in a `TransactionScope` created with
`TransactionScopeOption.Required`: it *joins* an ambient transaction if one already exists, otherwise
it starts a new one. Only the submit is wrapped &mdash; your queries/reads are **not** put in a
transaction, so reads carry no locking overhead.

Because of `Required`, the way to make a *read-then-write* atomic (read some rows, decide, then write)
is to open your **own** transaction around both the reads and the `SubmitUpdates`, and that scope's
isolation level is the one that applies:

```fsharp
open System.Transactions

use scope =
    new TransactionScope(
        TransactionScopeOption.Required,
        TransactionOptions(IsolationLevel = IsolationLevel.ReadCommitted),
        TransactionScopeAsyncFlowOption.Enabled)   // needed if you await inside the scope

// Open a FRESH data context *inside* the scope for this unit of work (see note below):
let ctx = sql.GetDataContext connstr

// ... your queries (reads) ...
// ... your Create()/mutations ...
ctx.SubmitUpdates()
scope.Complete()
```

**Create a fresh context per transaction &mdash; don't reuse a shared/long-lived one.** A data context
accumulates *all* pending changes (every `Create()`, `Delete()`, and column edit) until `SubmitUpdates()`
flushes them to the database together. If you'd reuse a long-lived or shared context, a `SubmitUpdates()`
inside your transaction could commit unrelated pending changes that another request or user left sitting on
that context. Opening a new context inside the scope keeps each transaction small and self-contained, and
guarantees the tracked changes are exactly what this transaction should write &mdash; and it keeps the
context's connection enlisted in this transaction.

**Reads are different &mdash; a shared context is fine (and fastest).** The "fresh context per unit of work"
rule above is about *writes*. If you use  `.GetReadOnlyDataContext connstr` you can enforce read-only:
it has no pending-change nor transaction concerns. For read-only operations you can safely **share/cache one** 
database context across requests, which is actually the fastest way to read (no per-call context setup). 
The one thing to add is *resilience*: if the connection dies for reasons outside SQLProvider (driver/network/timeout), 
you need logic to rebuild the shared context &mdash; e.g. hold it in a mutable `Lazy` and recreate 
it when the value is null or a call throws a connection error.

**Isolation level gotcha:** SQLProvider's `TransactionOptions.Default` &mdash; used *only* when
`SubmitUpdates` has to create a standalone write transaction (no ambient scope) &mdash; inherits the
.NET default, which is **`Serializable`**, not `ReadCommitted`. A bare `new TransactionScope()` is the
same: it also defaults to `Serializable`. So if you want `ReadCommitted` (usually the right choice to
avoid over-locking on a larger system), pass it explicitly through `TransactionOptions`, as above. Choosing the isolation
level for a unit of work is intentionally the caller's job, not SQLProvider's (see
[issue #238](https://github.com/fsprojects/SQLProvider/issues/238) for one real-world use-case solution).

If your transaction spans an `await` / async continuation, remember `TransactionScopeAsyncFlowOption.Enabled`
(otherwise the ambient transaction won't flow to the continuation thread &mdash; see the [async](async.html)
page for a full example).

**Read vs write contexts:** SQLProvider can generate a read-only data context via
`GetReadOnlyDataContext()` alongside the writable `GetDataContext()`, giving you compile-time separation
of read and write code paths (the read-only context can't accidentally mutate/submit). They are type-level
different. If you have a method parameter and you want to share a read-only query with a read 
that *must* run inside a write transaction, call `.AsReadOnly()` on the writable context to reuse its
connection.

SQLProvider also supports async database operations:

*)

ctx.SubmitUpdatesAsync() // |> Async.AwaitTask

(**
### OnConflict

The [SQLite](http://sqlite.org/lang_conflict.html), [PostgreSQL 9.5+](https://www.postgresql.org/docs/current/static/sql-insert.html#SQL-ON-CONFLICT) and [MySQL 8.0+](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html) providers support conflict resolution for INSERT statements.

They allow the user to specify if a unique constraint violation should be solved by ignoring the statement (DO NOTHING) or updating existing rows (DO UPDATE).

You can leverage this feature by setting the `OnConflict` property on a row object:
 * Setting it to `DoNothing` will add the DO NOTHING clause (PostgreSQL) or the OR IGNORE clause (SQLite).
 * Setting it to `Update` will add a DO UPDATE clause on the primary key constraint for all columns (PostgreSQL) or a OR REPLACE clause (SQLite).

Sql Server has a similar feature in the form of the MERGE statement. This is not yet supported.
*)

let ctx = sql.GetDataContext()

let emp = ctx.Main.Employees.Create()
emp.Id <- 1
emp.FirstName <- "Jane"
emp.LastName <- "Doe"

emp.OnConflict <- FSharp.Data.Sql.Common.OnConflict.Update

ctx.SubmitUpdates()

(**

### Delete-query for multiple items

To delete many items from a database table, `DELETE FROM [dbo].[EMPLOYEES] WHERE (...)`, there is a way, although we don't recommend deleting items from a database. Instead, you should consider a deletion-flag column. You should also back up your database before trying this. Note that changes are immediately saved to the database even if you don't call `ctx.SubmitUpdates()`.

*)
(*** hide ***)
let conditions = true

query {
    for c in ctx.Main.Employees do
    where (conditions)
} |> Seq.``delete all items from single table`` |> Async.AwaitTask |> Async.RunSynchronously

(**

### Selecting which Create() to use

There are 3 overrides of create.

**The ideal one to use is the long one \`\`Create(...)\`\`(...):**

*)

let emp = ctx.Main.Employees.``Create(FirstName, LastName)``("Don", "Syme")

(**

This is because it will fail if your database structure changes.
So, when your table gets new columns, the code will fail at compile time.
Then you decide what to do with the new columns, and not let a bug to customers.

But you may want to use the plain .Create() if your setup is not optimal.
Try to avoid these conditions:

* If your editor intellisense is not working for backtick-variables.
* You have lot of nullable columns in your database.
* You want to use F# like a dynamic language.

In the last case you'll be maintaining code like this:

*)

let employeeId = 123
// Got some untyped array of data from the client
let createSomeItem (data: seq<string*obj>)  =
    data
    |> Seq.map( // Some parsing and validation:
        function
        // Skip some fields
        | "EmployeeId", x
        | "PermissionLevel", x -> "", x
        // Convert and validate some fields
        | "PostalCode", x ->
            "PostalCode", x.ToString().ToUpper().Replace(" ", "") |> box
        | "BirthDate", x ->
            let bdate = x.ToString() |> DateTime.Parse
            if bdate.AddYears(18) > DateTime.UtcNow then
                failwith "Too young!"
            else
                "BirthDate", bdate.ToString("yyyy-MM-dd") |> box
        | others -> others)
    |> Seq.filter (fun (key,_) -> key <> "")
                  // Add some fields:
    |> Seq.append [|"EmployeeId", employeeId |> box;
                    "Country", "UK" |> box |]
    |> ctx.Main.Employees.Create

(**
### What to do if your creation fails systematically every time

Some underlying database connection libraries have problems with serializing underlying data types.
So, if this fails:

*)

emp.BirthDate <- DateTime.UtcNow
ctx.SubmitUpdates()

(**

Try using `.SetColumn("ColumnName", value |> box)`
for example:

*)

emp.SetColumn("BirthDate", DateTime.UtcNow.ToString("yyyy-MM-dd HH\:mm\:ss") |> box)
ctx.SubmitUpdates()

(**

SetColumn takes an object, giving you more control over the type serialization.

### Identifying columns dynamically

*)

let setIfExists (columnName) =
   if emp.HasColumn(columnName, StringComparison.InvariantCultureIgnoreCase) then
      emp.SetColumn(columnName, "testValue")

