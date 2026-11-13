(*** hide ***)
#r "../../../bin/lib/netstandard2.0/FSharp.Data.SqlProvider.Common.dll"
#r "../../../bin/lib/netstandard2.0/FSharp.Data.SqlProvider.dll"
(*** hide ***)
let [<Literal>] resolutionPath = __SOURCE_DIRECTORY__ + @"/../../files/sqlite"
(*** hide ***)
let [<Literal>] connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"\..\northwindEF.db;Version=3;Read Only=false;FailIfMissing=True;"
(*** hide ***)
open FSharp.Data.Sql
(*** hide ***)
type sql =
    SqlDataProvider<
        ConnectionString = connectionString,
        DatabaseVendor = Common.DatabaseProviderTypes.SQLITE,
        ResolutionPath = resolutionPath,
        CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL,
        SQLiteLibrary=Common.SQLiteLibrary.SystemDataSQLite>

(**

# Locking rows for update

## The problem: lost updates

Most web requests that change data first read it, then decide, then write. When two requests do that to the same row at
the same moment, both read the old value, both decide on it, and the second write silently undoes or doubles the first:

1. Request A reads an account's balance: 50.
2. Request B reads the same balance: 50.
3. A pays out 40 and writes 10.
4. B pays out 40 and writes 10. 80 went out of 50, and the balance says 10.

A transaction alone doesn't stop this. At the isolation level most databases use by default (read committed), both
transactions see the committed 50, and each one's write is valid on its own.

## The fix: lock the rows you read

`Seq.lockForUpdate` asks the database to lock each row the query returns until your transaction ends. Another transaction
that wants to lock, change or delete one of those rows waits until yours commits or rolls back, and then reads the new
value. In the example, B waits at step 2 until A has written 10, reads 10, and refuses the payment. Plain reads that don't
lock aren't blocked.

In SQL this is `SELECT ... FOR UPDATE`, a "pessimistic lock": the database holds others back, instead of your code
noticing a clash afterwards.

Read, decide and write in one transaction. With async code, the TransactionScope needs
`TransactionScopeAsyncFlowOption.Enabled`, or the transaction doesn't follow the awaits:

*)

open System.Transactions

let addToFreight (orderId: int64) (amount: decimal) =
    task {
        let ctx = sql.GetDataContext()
        use scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled)

        // Locked until the scope completes or is disposed
        let! order =
            query {
                for order in ctx.Main.Orders do
                where (order.OrderId = orderId)
                select order
            }
            |> Seq.lockForUpdateHeadAsync

        order.Freight <- order.Freight + amount
        do! ctx.SubmitUpdatesAsync()
        scope.Complete()
    }

(**

## The functions

| Function | Does |
|---|---|
| `Seq.lockForUpdate` | Marks a query to lock its rows; run it with any executor, `Seq.toList` included. |
| `Seq.lockForUpdateAsync`, `Array.lockForUpdateAsync`, `List.lockForUpdateAsync` | Run the query with its rows locked. |
| `Seq.lockForUpdateHeadAsync`, `Seq.lockForUpdateTryHeadAsync` | Lock and read one row; `tryHead...` gives `None` when there's none. On Oracle not after a sort. |

## What each database does

| Database | SQL | Notes |
|---|---|---|
| PostgreSQL | `FOR UPDATE` | With a left outer join, `FOR UPDATE OF` the other tables: the rows of the outer-joined table aren't locked. |
| MySQL (InnoDB) | `FOR UPDATE` | |
| Oracle | `FOR UPDATE` | Not with skip, nor with take or head after a sort: refused. |
| Firebird | `FOR UPDATE WITH LOCK` | One table only: a query with joins is refused. `FOR UPDATE` alone doesn't lock in Firebird. |
| SQL Server | `WITH (UPDLOCK, ROWLOCK)` on every table | Held until the transaction ends. |
| SQLite, MS Access, DuckDB, ODBC | none | No row locks: refused with an error. Use a transaction that writes first, or a unique constraint. |

A query that isn't one table's rows can't be locked, so a distinct, grouped, counted or aggregated query, or a union, is
refused. The lock belongs to a whole statement, so a locked query is also refused as a part of another: as a sub-query
(`IN`, `EXISTS`), as one side of a union, as the table joined into a query, and in a delete. Lock the outer query
instead. In unit tests the mock context's query comes back as it is.

## Rules of thumb

- **Always inside a transaction.** Without one the database commits each statement on its own, and the lock ends as soon
  as the query has run. Run the queries of one scope one after another: SQLProvider opens a connection per query, and the
  connections of one TransactionScope share its transaction when they don't overlap.
- **What is locked:** every row the query returns, and with joins on PostgreSQL, MySQL and Oracle the joined rows too
  (on PostgreSQL not those of a left-outer-joined table); SQL Server's hints go on every table of the query. Rows the filter leaves out aren't locked, and neither is a row
  inserted later: to keep two transactions from both inserting "the first" row, lock a parent row that already exists
  (the customer, the account), or rely on a unique constraint.
- **Lock the same way everywhere.** Two transactions that lock the same rows in opposite orders wait for each other
  forever; the database spots this deadlock and fails one of them. Lock a parent row first, the same parent in every
  code path, and the deadlock can't happen.
- **Keep it short.** Others wait while you hold the lock: read, decide and write, and avoid slow work, such as calls to
  other services, in between where you can.
- **Waiting has no limit by default.** Set one in the database when a wait should fail instead: PostgreSQL's
  `lock_timeout`, MySQL's `innodb_lock_wait_timeout`, SQL Server's `SET LOCK_TIMEOUT`. `NOWAIT` and `SKIP LOCKED`
  aren't available through SQLProvider.

## When not to lock

A lock makes others wait. When clashes are rare and a retry is cheap, optimistic concurrency avoids the waiting: keep a
version or timestamp column, update with `where (row.Version = versionRead)`, and treat zero rows changed as "someone
else changed it, read again". When the rule is about rows that don't exist yet ("one open order per customer"), a unique
constraint states it best: the database refuses the second insert, and `OnConflict` on the insert decides what happens
instead (see [CRUD](crud.html)).

*)
