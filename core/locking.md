# Locking rows for update

## Most code needs no lock

`Seq.lockForUpdate` is for a few named places in a system. It is not a default, and not something to add to be on the
safe side. Without it the database already does this:

* **A statement is safe on its own.** The database locks a row for the moment it writes it. `SubmitUpdates` writes only
the columns you changed, so two requests that change different columns of one row both keep their change.
* **A transaction keeps a unit of work together.** Reads and `SubmitUpdates` inside one `TransactionScope` commit or
roll back as one (see [Transactions and isolation level](crud.html#Transactions-and-isolation-level)). At
`Serializable`, which a bare `new TransactionScope()` uses, two transactions that read and then change the same row
can't both commit: the database fails one of them with a deadlock or serialization error, so no change is lost
silently.
* **A unique constraint keeps a row from being inserted twice**, and `OnConflict` on the insert decides what happens
instead (see [CRUD](crud.html#OnConflict)).

What is left is the lost update described below, and only where two requests change the same row at the same moment.
Rows that belong to one user seldom meet that.

## What a lock costs

* Every other transaction that wants to change or lock the row waits. PostgreSQL and SQL Server wait without a limit by
default, MySQL for 50 seconds.
* A waiting request keeps its connection and the locks it already holds, so a queue behind one slow transaction reaches
requests that never touched the row.
* Two transactions that lock the same rows in opposite orders deadlock, and the database fails one of them.
* SQL Server replaces a statement's row locks with one lock on the whole table when there are thousands of them (lock
escalation, from about 5,000). Then every writer of the table waits.
* The waiting grows with the load: a lock that costs nothing with one customer can stop the system with a hundred.
Locks added without a need are hard to take out later, as nothing says which of them the code relies on.

## Where a lock is right: the lost update

A request reads a value, decides on it, and writes a value computed from it. When two requests do that to the same row
at the same moment, both read the old value, and the second write undoes or doubles the first:

1. Request A reads an account's balance: 50.
2. Request B reads the same balance: 50.
3. A pays out 40 and writes 10.
4. B pays out 40 and writes 10. 80 went out of 50, and the balance says 10.

At read committed both transactions see the committed 50, and each one's write is valid on its own. `Serializable`
fails one of the two. A lock makes B wait instead, so both requests get an answer.

Lock when all of these hold:

* The value written is computed from the value read, or the decision depends on it: a balance, a counter, a stock
level, a status that may change only once.
* Two requests for the same row at the same moment are likely enough to matter.
* The query reads one row, or a few, by key.
* Making the second request wait is better than failing it and running it again.

Otherwise use a transaction, a unique constraint, or the check at save time at the end of this page.

## Locking the rows you read

`Seq.lockForUpdate` asks the database to lock each row the query returns until your transaction ends. Another transaction
that wants to lock, change or delete one of those rows waits until yours commits or rolls back, and then reads the new
value. In the example, B waits at step 2 until A has written 10, reads 10, and refuses the payment. Plain reads that don't
lock aren't blocked.

In SQL this is `SELECT ... FOR UPDATE`, a "pessimistic lock": the database holds others back, instead of your code
noticing a clash afterwards.

Read, decide and write in one transaction. With async code, the TransactionScope needs
`TransactionScopeAsyncFlowOption.Enabled`, or the transaction doesn't follow the awaits:

```fsharp
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
```

## The functions

Function | Does
--- | ---
`Seq.lockForUpdate` | Marks a query to lock its rows; run it with any executor, `Seq.toList` included.
`Seq.lockForUpdateAsync`, `Array.lockForUpdateAsync`, `List.lockForUpdateAsync` | Run the query with its rows locked.
`Seq.lockForUpdateHeadAsync`, `Seq.lockForUpdateTryHeadAsync` | Lock and read one row; `tryHead...` gives `None` when there's none. On Oracle not after a sort.

## What each database does

Database | SQL | Notes
--- | --- | ---
PostgreSQL | `FOR UPDATE` | With a left outer join, `FOR UPDATE OF` the other tables: the rows of the outer-joined table aren't locked.
MySQL (InnoDB) | `FOR UPDATE` | &#32;
Oracle | `FOR UPDATE` | Not with skip, nor with take or head after a sort: refused.
Firebird | `FOR UPDATE WITH LOCK` | One table only: a query with joins is refused. `FOR UPDATE` alone doesn't lock in Firebird.
SQL Server | `WITH (UPDLOCK, ROWLOCK)` on every table | Held until the transaction ends.
SQLite, MS Access, DuckDB, ODBC | none | No row locks: refused with an error. Use a transaction that writes first, or a unique constraint.

A query that isn't one table's rows can't be locked, so a distinct, grouped, counted or aggregated query, or a union, is
refused. The lock belongs to a whole statement, so a locked query is also refused as a part of another: as a sub-query
(`IN`, `EXISTS`), as one side of a union, as the table joined into a query, and in a delete. Lock the outer query
instead. In unit tests the mock context's query comes back as it is.

## Rules of thumb

* **Always inside a transaction.** Without one the database commits each statement on its own, and the lock ends as soon
as the query has run. Run the queries of one scope one after another: SQLProvider opens a connection per query, and the
connections of one TransactionScope share its transaction when they don't overlap.
* **What is locked:** every row the query returns, and with joins on PostgreSQL, MySQL and Oracle the joined rows too
(on PostgreSQL not those of a left-outer-joined table); SQL Server's hints go on every table of the query. Rows the filter leaves out aren't locked, and neither is a row
inserted later: to keep two transactions from both inserting "the first" row, lock a parent row that already exists
(the customer, the account), or rely on a unique constraint.
* **Lock the same way everywhere.** Two transactions that lock the same rows in opposite orders wait for each other
forever; the database spots this deadlock and fails one of them. Lock a parent row first, the same parent in every
code path, and the deadlock can't happen.
* **Few rows, by key.** Lock the one row the decision is about, found by its key. A locked query that reads many rows
holds all of them, and on SQL Server may end up holding the table.
* **Keep it short.** Others wait while you hold the lock: read, decide and write, and avoid slow work, such as calls to
other services, in between where you can.
* **Limit the waiting.** PostgreSQL and SQL Server wait without a limit by default. Set one in the database when a wait
should fail instead: PostgreSQL's `lock_timeout`, MySQL's `innodb_lock_wait_timeout`, SQL Server's
`SET LOCK_TIMEOUT`. `NOWAIT` and `SKIP LOCKED` aren't available through SQLProvider.

## Check at save time (optimistic concurrency)

An update writes the columns you changed to the row with that primary key. It doesn't check whether another request
changed the row after you read it: the last write wins. When a row is read, shown to a user and saved later, no lock
can be held in between. Check at save time instead: read the row again, compare it with what the user saw, and write
only when it is the same.

The second read and the write are one unit, or a change could come between them. The sample locks the one row for the
length of the save. At `Serializable` the lock can be left out: a clash then fails one of the transactions.

```fsharp
/// Saves the new freight unless the order changed after the caller read it.
let saveFreight (orderId: int64) (freightSeen: decimal) (newFreight: decimal) =
    task {
        use scope =
            new TransactionScope(
                TransactionScopeOption.Required,
                TransactionOptions(IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted),
                TransactionScopeAsyncFlowOption.Enabled)

        let ctx = sql.GetDataContext()

        let! order =
            query {
                for order in ctx.Main.Orders do
                where (order.OrderId = orderId)
                select order
            }
            |> Seq.lockForUpdateHeadAsync

        if order.Freight <> freightSeen then
            // Someone else saved first: show the user the new values
            return false
        else
            order.Freight <- newFreight
            do! ctx.SubmitUpdatesAsync()
            scope.Complete()
            return true
    }
```

A column that changes with every write makes the comparison one value: SQL Server's `rowversion` (`timestamp`), which
the database sets itself and SQLProvider reads as a byte array, or a counter the code raises with each save.

When the rule is about rows that don't exist yet ("one open order per customer"), a unique constraint states it best:
the database refuses the second insert, and `OnConflict` on the insert decides what happens instead (see
[CRUD](crud.html#OnConflict)).
