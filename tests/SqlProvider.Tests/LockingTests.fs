module LockingTests

// Seq.lockForUpdate: the SQL each database's provider writes for a locked query, read without a connection, and the
// queries a lock is refused in. The statements here were run against MySQL 8.4, Oracle 23, Firebird 5 and
// SQL Server, where a second session couldn't lock the rows they read.

open System
open System.Linq
open FSharp.Data.Sql
open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Schema
open NUnit.Framework

let private createProvider (name: string) (arguments: obj array) =
    let providerType = typeof<FSharp.Data.Sql.Runtime.SqlDataContext>.Assembly.GetType("FSharp.Data.Sql.Providers." + name, true)
    let flags = Reflection.BindingFlags.Instance ||| Reflection.BindingFlags.Public ||| Reflection.BindingFlags.NonPublic
    Activator.CreateInstance(providerType, flags, null, arguments, null) :?> ISqlProvider

let private mark = { Schema = "dbo"; Name = "mark"; Type = "" }
let private child = { Schema = "dbo"; Name = "child"; Type = "" }

/// SELECT m.id FROM mark m
let private plain =
    { SqlQuery.Empty with UltimateChild = Some("m", mark) }

let private locked = { plain with ForUpdate = true }

/// ... INNER JOIN child c ON c.mark_id = m.id
let private joined (query: SqlQuery) =
    let link =
        { PrimaryTable = mark
          PrimaryKey = [ KeyColumn "id" ]
          ForeignTable = child
          ForeignKey = [ KeyColumn "mark_id" ]
          OuterJoin = false
          IsNullableOuter = false
          RelDirection = RelationshipDirection.Children }

    { query with Aliases = Map.ofList [ "c", child ]; Links = [ "m", link, "c" ] }

let private sorted (query: SqlQuery) = { query with Ordering = [ "m", KeyColumn "id", false ] }

/// The provider's SQL of the query, as SqlCheck compares it: upper case, single spaces.
let private textWith (connection: Data.IDbConnection) (provider: ISqlProvider) (query: SqlQuery) =
    let projection = Collections.Generic.Dictionary<string, ResizeArray<ProjectionParameter>>()
    projection.["m"] <- ResizeArray [ EntityColumn "id" ]
    let sql, _ = provider.GenerateQueryText(query, "m", mark, projection, false, connection)
    (SqlCheck.normalise sql).Trim().TrimEnd(';')

let private text (provider: ISqlProvider) (query: SqlQuery) = textWith null provider query

let private refused (expected: string) (run: unit -> unit) =
    let error = Assert.Catch(fun () -> run ())
    StringAssert.Contains(expected, error.ToString())

[<Test; SqlCheck.NoSql>]
let ``SQL Server locks rows with the UPDLOCK and ROWLOCK hints on every table``() =
    let provider = createProvider "MSSqlServerProvider" [| box ""; box "" |]
    // The paging depends on the server's version, looked up by the connection string; the connection is never opened
    use connection = provider.CreateConnection "Data Source=unused"
    let text = textWith connection
    StringAssert.DoesNotContain("UPDLOCK", text provider (joined plain))
    StringAssert.Contains("FROM [DBO].[MARK] AS [M] WITH(UPDLOCK,ROWLOCK)", text provider locked)
    let both = text provider (joined locked)
    StringAssert.Contains("FROM [DBO].[MARK] AS [M] WITH(UPDLOCK,ROWLOCK)", both)
    StringAssert.Contains("INNER JOIN [DBO].[CHILD] AS [C] WITH(UPDLOCK,ROWLOCK)ON", both)
    StringAssert.Contains("SELECT TOP 1", text provider { locked with Take = ValueSome 1 })

[<Test; SqlCheck.NoSql>]
let ``MySQL locks rows with FOR UPDATE after the paging``() =
    let provider = createProvider "MySqlProvider" [| box ""; box ""; box ""; box Array.empty<string> |]
    StringAssert.DoesNotContain("FOR UPDATE", text provider (joined plain))
    StringAssert.EndsWith(" FOR UPDATE", text provider locked)
    StringAssert.EndsWith(" FOR UPDATE", text provider (joined locked))
    StringAssert.EndsWith(" LIMIT 1 FOR UPDATE", text provider { locked with Take = ValueSome 1 })
    StringAssert.EndsWith(" LIMIT 1 OFFSET 2 FOR UPDATE", text provider { sorted locked with Take = ValueSome 1; Skip = ValueSome 2 })
    StringAssert.EndsWith(" OFFSET 2 FOR UPDATE", text provider { sorted locked with Skip = ValueSome 2 })

[<Test; SqlCheck.NoSql>]
let ``Firebird locks the rows of one table with FOR UPDATE WITH LOCK``() =
    let provider =
        createProvider "FirebirdProvider" [| box ""; box ""; box ""; box Array.empty<string>; box OdbcQuoteCharacter.DEFAULT_QUOTE |]

    StringAssert.DoesNotContain("FOR UPDATE", text provider (joined plain))
    StringAssert.EndsWith(" FOR UPDATE WITH LOCK", text provider locked)
    StringAssert.EndsWith(" ROWS 1 FOR UPDATE WITH LOCK", text provider { locked with Take = ValueSome 1 })
    StringAssert.EndsWith(" ROWS 3 TO 3 FOR UPDATE WITH LOCK", text provider { sorted locked with Take = ValueSome 1; Skip = ValueSome 2 })
    // "WITH LOCK can be used only with a single physical table"
    refused "one table only" (fun () -> text provider (joined locked) |> ignore)

[<Test; SqlCheck.NoSql>]
let ``Oracle locks rows with FOR UPDATE, not behind a skip or a sorted take``() =
    let provider = createProvider "OracleProvider" [| box ""; box ""; box ""; box Array.empty<string>; box "" |]
    StringAssert.DoesNotContain("FOR UPDATE", text provider (joined plain))
    StringAssert.EndsWith(" FOR UPDATE", text provider locked)
    StringAssert.EndsWith(" FOR UPDATE", text provider (joined (sorted locked)))
    // A head or take without a sort: the ROWNUM view's rows are the table's
    StringAssert.EndsWith(")WHERE ROWNUM <= 1 FOR UPDATE", text provider { locked with Take = ValueSome 1 })
    // ORA-02014
    refused "after a sort" (fun () -> text provider { sorted locked with Take = ValueSome 1 } |> ignore)
    refused "with skip" (fun () -> text provider { sorted locked with Skip = ValueSome 2 } |> ignore)
    refused "with skip" (fun () -> text provider { locked with Take = ValueSome 1; Skip = ValueSome 2 } |> ignore)

[<Test; SqlCheck.NoSql>]
let ``a query that isn't a database query isn't locked``() =
    // What a mock context's tables are
    let inMemory = [ 1; 2; 3 ].AsQueryable()
    Assert.AreSame(inMemory, Seq.lockForUpdate inMemory)

[<Test; SqlCheck.NoSql>]
let ``a lock stays on the query through later steps``() =
    let dc = CrudTests.sql.GetDataContext()
    let lockedCustomers = dc.Main.Customers |> Seq.lockForUpdate
    refused "SQLite has no row locks" (fun () -> lockedCustomers.Where(fun cust -> cust.City <> "").Take(1) |> Seq.toList |> ignore)
    refused "SQLite has no row locks" (fun () ->
        query { for cust in lockedCustomers do
                where (cust.City <> "")
                sortBy cust.CustomerId
                select cust.CustomerId }
        |> Seq.toList
        |> ignore)

// A lock clause ends a whole statement, so a locked query can't be a part of another one.
[<Test; SqlCheck.NoSql>]
let ``a locked query is refused inside another query and in a delete``() =
    let dc = CrudTests.sql.GetDataContext()
    let lockedIds = query { for cust in dc.Main.Customers do select cust.CustomerId } |> Seq.lockForUpdate
    let lockedCustomers = dc.Main.Customers |> Seq.lockForUpdate

    refused "can't be a sub-query" (fun () ->
        query { for order in dc.Main.Orders do
                where (lockedIds.Contains order.CustomerId)
                select order.OrderId }
        |> Seq.toList
        |> ignore)

    refused "can't be a sub-query" (fun () ->
        query { for order in dc.Main.Orders do
                where (query { for cust in lockedCustomers do
                               exists (cust.CustomerId = order.CustomerId) })
                select order.OrderId }
        |> Seq.toList
        |> ignore)

    let cities = query { for emp in dc.Main.Employees do select emp.City }
    let lockedCities = query { for cust in dc.Main.Customers do select cust.City } |> Seq.lockForUpdate
    refused "can't be a sub-query" (fun () -> cities.Union(lockedCities) |> Seq.toList |> ignore)
    refused "or a union" (fun () -> lockedCities.Union(cities) |> Seq.toList |> ignore)

    refused "not the table joined into it" (fun () ->
        query { for order in dc.Main.Orders do
                join cust in lockedCustomers on (order.CustomerId = cust.CustomerId)
                select order.OrderId }
        |> Seq.toList
        |> ignore)

    refused "A delete can't take" (fun () ->
        (query { for cust in dc.Main.Customers do
                 where (cust.CustomerId = "NO SUCH CUSTOMER")
                 select cust }
         |> Seq.lockForUpdate
         |> Seq.``delete all items from single table``)
            .Wait())

// Firebird's Serializable is SNAPSHOT TABLE STABILITY: it locks whole tables, and eight writers of one table at once lost
// 70 of their 80 rows to "lock conflict on no wait transaction" (Firebird 5). A write batch's own transaction is a
// snapshot instead. Inside the caller's transaction the options stay as given: the level there is the caller's.
[<Test; SqlCheck.NoSql>]
let ``Firebird's own write transaction is a snapshot, not table-locking``() =
    let writes = typeof<FSharp.Data.Sql.Runtime.SqlDataContext>.Assembly.GetType("FSharp.Data.Sql.Providers.FirebirdWrites", true)
    let flags = Reflection.BindingFlags.Static ||| Reflection.BindingFlags.Public ||| Reflection.BindingFlags.NonPublic
    let ownTransaction (options: Transactions.TransactionOptions) =
        writes.GetMethod("ownTransaction", flags).Invoke(null, [| box options |]) :?> Transactions.TransactionOptions
    let standard = Transactions.TransactionOptions.Default
    let readCommitted = { standard with IsolationLevel = Transactions.IsolationLevel.ReadCommitted }
    Assert.AreEqual(Transactions.IsolationLevel.Serializable, standard.IsolationLevel, "the default of every database")
    Assert.AreEqual(Transactions.IsolationLevel.RepeatableRead, (ownTransaction standard).IsolationLevel, "a snapshot")
    Assert.AreEqual(Transactions.IsolationLevel.ReadCommitted, (ownTransaction readCommitted).IsolationLevel, "another level stays")
    use scope = new System.Transactions.TransactionScope()
    Assert.AreEqual(Transactions.IsolationLevel.Serializable, (ownTransaction standard).IsolationLevel, "inside the caller's transaction")
