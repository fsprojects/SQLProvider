#if INTERACTIVE
#I @"../../bin/lib/net48/"
#r "FSharp.Data.SqlProvider.Common.dll"
#r "FSharp.Data.SqlProvider.dll"
#r @"System.Transactions.dll"
#r @"../../packages/NUnit/lib/netstandard2.0/nunit.framework.dll"
#else
module CrudTests
#endif

open System
open System.IO
open FSharp.Data.Sql
open System.Linq
open NUnit.Framework
open System
open System.Threading.Tasks
open System.Transactions

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + "/libs"

#if NETFRAMEWORK
[<Literal>]
let connectionString =  @"Data Source=" + __SOURCE_DIRECTORY__ + @"/db/northwindEF.db;Version=3;Read Only=false;FailIfMissing=True;"
[<Literal>]
let sqliteLib = Common.SQLiteLibrary.SystemDataSQLite
#else
[<Literal>]
let connectionString =  @"Data Source=" + __SOURCE_DIRECTORY__ + @"/db/northwindEF.db"
[<Literal>]
let sqliteLib = Common.SQLiteLibrary.MicrosoftDataSqlite
#endif

type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, CaseSensitivityChange=Common.CaseSensitivityChange.ORIGINAL, ResolutionPath = resolutionPath, SQLiteLibrary=sqliteLib>
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %O")


let createCustomer (dc:sql.dataContext) =
    let newCustomer = dc.Main.Customers.Create()
    newCustomer.CustomerId <- "SQLPROVIDER"
    newCustomer.Address <- "FsPRojects"
    newCustomer.City <- "Fsharp"
    newCustomer.CompanyName <- "FSProjects"
    newCustomer.ContactName <- "A DB"
    newCustomer.ContactTitle <- "Mr"
    newCustomer.Country <- "England"
    newCustomer.Fax <- "Fax Number"
    newCustomer.Phone <- "Phone Number"
    newCustomer.PostalCode <- "PostCode"
    newCustomer.Region <- "London"
    newCustomer

[<Test>]
let ``Can create and delete an entity``() =
    let dcTestParam = sql.GetDataContext 200
    let dc = sql.GetDataContext()

    let originalCustomers =
        query { for cust in dc.Main.Customers do
                select cust }
        |> Seq.toList

    createCustomer dc |> ignore

    dc.SubmitUpdates()

    let newCustomers =
        query { for cust in dc.Main.Customers do
                select cust  }
        |> Seq.toList

    let created =
        newCustomers |> List.find (fun x -> x.CustomerId = "SQLPROVIDER")

    Assert.AreEqual("Phone Number", created.Phone)
    created.Delete()
    dc.SubmitUpdates()
    Assert.AreEqual(originalCustomers.Length, newCustomers.Length - 1)

[<Test>]
let ``Can create, update and delete an entity``() =
    let dc = sql.GetDataContext()

    let originalCustomers =
        query { for cust in dc.Main.Customers do
                select cust }
        |> Seq.toList

    let ent = createCustomer dc

    dc.SubmitUpdates()
    dc.SubmitUpdates() // run twice just to test extra won't hurt

    ent.SetColumn("Phone", "Updated Number")

    dc.SubmitUpdates()
    dc.SubmitUpdates()

    // let's create new context just to test that it is actually there.
    let dc2 = sql.GetDataContext()

    let newCustomers =
        query { for cust in dc2.Main.Customers do
                select cust  }
        |> Seq.toList

    let created =
        newCustomers |> List.find (fun x -> x.CustomerId = "SQLPROVIDER")

    Assert.AreEqual(originalCustomers.Length, newCustomers.Length - 1)

    Assert.AreEqual("Updated Number", created.Phone)
    created.Delete()
    dc2.SubmitUpdates()
    dc2.SubmitUpdates()

    let reallyDeleted =
        query { for cust in dc2.Main.Customers do
                select cust  }
        |> Seq.toList

    Assert.AreEqual(originalCustomers.Length, reallyDeleted.Length)

[<Test>]
let ``Can persist a blob``() =
    let dc = sql.GetDataContext()

    let imageBytes = [| 0uy .. 100uy |]

    let savedEntity = dc.Main.Pictures.``Create(Image)`` imageBytes
    savedEntity.Id <- 123L+int64(Random().Next 10000)
    dc.SubmitUpdates()

    let reloadedEntity =
        query { for image in dc.Main.Pictures do
                where (image.Id = savedEntity.Id)
                head }

    Assert.That( reloadedEntity.Image, Is.EqualTo(imageBytes))
    savedEntity.Delete()
    dc.SubmitUpdates()

[<Test; Ignore("Something wrong with SQLite transactions...")>]
let ``Can enlist in a transaction scope and rollback changes without complete``() =

    try
        let dc = sql.GetDataContext()
        use ts = new TransactionScope()
        createCustomer dc |> ignore
        dc.SubmitUpdates()
        ts.Dispose()
    finally
        GC.Collect()
    let dc2 = sql.GetDataContext()

    let created =
        query { for cust in dc2.Main.Customers do
                where (cust.CustomerId = "SQLPROVIDER")
                select cust
        }
        |> Seq.toList

    Assert.AreEqual([], created)


[<Test>]
let ``Conflict resolution is correctly applied``() =
    let dc = sql.GetDataContext()

    let ent = createCustomer dc
    dc.SubmitUpdates()

    let getCurrentAddress =
        query { for cust in dc.Main.Customers do
                where (cust.CustomerId = ent.CustomerId)
                select cust.Address
        }

    // Works when reusing the same entity with changed properties
    let newAddress = "FsProjects 2.0"
    ent.Address <- newAddress
    ent.OnConflict <- Common.OnConflict.Update
    dc.SubmitUpdates()

    Assert.AreEqual(getCurrentAddress |> Seq.head, newAddress)

    // Works when creating a fresh entity
    let ent2 = createCustomer dc
    let newerAddress = "FsProjects 3.0"
    ent2.Address <- newerAddress
    ent2.OnConflict <- Common.OnConflict.Update
    dc.SubmitUpdates()

    Assert.AreEqual(getCurrentAddress |> Seq.head, newerAddress)

    // DoNothing doesn't update the address
    let ent3 = createCustomer dc
    ent3.Address <- "asdkjskdjsldjskjdls"
    ent3.OnConflict <- Common.OnConflict.DoNothing
    dc.SubmitUpdates()

    Assert.AreEqual(getCurrentAddress |> Seq.head, newerAddress)

    // Cleanup after testing
    query { for cust in dc.Main.Customers do
                where (cust.CustomerId = ent.CustomerId)
                select cust
    }
    |> Seq.head
    |> (fun c -> c.Delete())
    dc.SubmitUpdates()

[<Test; SqlCheck.ExpectSql("ON CONFLICT (\"CustomerID\") DO NOTHING", "ON CONFLICT (\"CustomerID\") DO UPDATE SET \"Address\" = excluded.\"Address\", \"City\" = @param")>]
let ``Conflict resolution on named columns changes only the named columns``() =
    let dc = sql.GetDataContext()
    let ent = createCustomer dc
    dc.SubmitUpdates()

    let current () =
        query { for cust in dc.Main.Customers do
                where (cust.CustomerId = ent.CustomerId)
                select (cust.Address, cust.City, cust.ContactName) }
        |> Seq.head

    try
        let before = current ()

        // DoNothingOn the key leaves the row as it is
        let same = createCustomer dc
        same.Address <- "Somewhere else"
        same.OnConflict <- Common.OnConflict.DoNothingOn [ "CustomerID" ]
        dc.SubmitUpdates()
        Assert.AreEqual(before, current ())

        // UpdateOn sets the inserted value or a constant, and the columns not named keep their old values; names match the
        // table's ignoring case
        let changed = createCustomer dc
        changed.Address <- "FsProjects 4.0"
        changed.City <- "Elsewhere"
        changed.ContactName <- "Someone else"
        changed.OnConflict <- Common.OnConflict.UpdateOn([ "customerid" ], [ Common.Inserted "Address"; Common.Constant("City", box "Constant city") ])
        dc.SubmitUpdates()
        let _, _, contact = before
        Assert.AreEqual(("FsProjects 4.0", "Constant city", contact), current ())

        // A null constant clears the column
        let cleared = createCustomer dc
        cleared.OnConflict <- Common.OnConflict.UpdateOn([ "CustomerID" ], [ Common.Constant("City", null) ])
        dc.SubmitUpdates()
        let _, city, _ = current ()
        Assert.IsTrue(String.IsNullOrEmpty city)

        // A column named twice is refused
        let twice = createCustomer dc
        twice.OnConflict <- Common.OnConflict.UpdateOn([ "CustomerID" ], [ Common.Inserted "City"; Common.Constant("city", box "x") ])
        let repeated = Assert.Throws<exn>(fun () -> dc.SubmitUpdates())
        StringAssert.Contains("more than once", repeated.Message)
        dc.ClearUpdates() |> ignore

        // A name that isn't one of the table's columns never reaches the SQL
        let wrong = createCustomer dc
        wrong.OnConflict <- Common.OnConflict.DoNothingOn [ "Address) DO NOTHING; DROP TABLE Customers; --" ]
        let error = Assert.Throws<exn>(fun () -> dc.SubmitUpdates())
        StringAssert.Contains("isn't a column of", error.Message)
        dc.ClearUpdates() |> ignore
    finally
        // Rows a failed step left pending would be submitted with the cleanup
        dc.ClearUpdates() |> ignore
        query { for cust in dc.Main.Customers do
                    where (cust.CustomerId = ent.CustomerId)
                    select cust }
        |> Seq.head
        |> (fun c -> c.Delete())
        dc.SubmitUpdates()

[<Test; SqlCheck.ExpectSql("ON CONFLICT (\"CustomerID\") DO NOTHING", "ON CONFLICT (\"CustomerID\") DO UPDATE SET \"Address\" = excluded.\"Address\", \"City\" = @param")>]
let ``Typed conflict handling names the columns through the row's properties``() =
    let dc = sql.GetDataContext()
    let ent = createCustomer dc
    dc.SubmitUpdates()

    let current () =
        query { for cust in dc.Main.Customers do
                where (cust.CustomerId = ent.CustomerId)
                select (cust.Address, cust.City, cust.ContactName) }
        |> Seq.head

    try
        let _, _, contact = current ()

        let same = createCustomer dc
        same.Address <- "Somewhere else"
        same.OnConflictDoNothing(fun c -> c.CustomerId)
        Assert.AreEqual(Common.OnConflict.DoNothingOn [ "CustomerID" ], same.OnConflict)
        dc.SubmitUpdates()
        Assert.AreEqual(("FsPRojects", "Fsharp", contact), current ())

        // The inserted value, a captured value; the contact isn't set, so it keeps its old value
        let city = "Constant city"
        let changed = createCustomer dc
        changed.Address <- "FsProjects 5.0"
        changed.ContactName <- "Someone else"
        changed.OnConflictUpdate((fun c -> c.CustomerId), fun existing inserted ->
            existing.Address <- inserted.Address
            existing.City <- city)
        Assert.AreEqual(
            Common.OnConflict.UpdateOn([ "CustomerID" ], [ Common.Inserted "Address"; Common.Constant("City", box city) ]),
            changed.OnConflict)
        dc.SubmitUpdates()
        Assert.AreEqual(("FsProjects 5.0", city, contact), current ())

        // What SQL can't take from the rows is refused when it's set, before any SQL
        let refused = createCustomer dc
        let fromRows = Assert.Throws<exn>(fun () -> refused.OnConflictUpdate((fun c -> c.CustomerId), fun existing _ -> existing.Address <- existing.Address + "!"))
        StringAssert.Contains("computed from the rows", fromRows.Message)
        let otherColumn = Assert.Throws<exn>(fun () -> refused.OnConflictUpdate((fun c -> c.CustomerId), fun existing inserted -> existing.Address <- inserted.City))
        StringAssert.Contains("of itself only", otherColumn.Message)
        let nothing = Assert.Throws<exn>(fun () -> refused.OnConflictUpdate((fun c -> c.CustomerId), fun _ _ -> ()))
        StringAssert.Contains("at least one property", nothing.Message)
        let twice =
            Assert.Throws<exn>(fun () ->
                refused.OnConflictUpdate((fun c -> c.CustomerId), fun existing inserted ->
                    existing.City <- inserted.City
                    existing.City <- "x"))
        StringAssert.Contains("more than once", twice.Message)
        dc.ClearUpdates() |> ignore
    finally
        // Rows a failed step left pending would be submitted with the cleanup
        dc.ClearUpdates() |> ignore
        query { for cust in dc.Main.Customers do
                    where (cust.CustomerId = ent.CustomerId)
                    select cust }
        |> Seq.head
        |> (fun c -> c.Delete())
        dc.SubmitUpdates()

// After a conflict SQLite's last_insert_rowid() is still the key of the row inserted before. A row left as it was reads
// no key, and a row updated in place reads the existing row's.
[<Test; SqlCheck.ExpectSql("SELECT last_insert_rowid() WHERE changes() > 0", "SELECT \"CategoryID\" FROM main.Categories WHERE \"CategoryName\" = @param0")>]
let ``A conflict on named columns gives the row no other row's key``() =
    // A copy of the database, where category names are unique
    let original = __SOURCE_DIRECTORY__ + "/db/northwindEF.db"
    let copy = Path.Combine(Path.GetTempPath(), "sqlprovider_" + Guid.NewGuid().ToString("N") + ".db")
    File.Copy(original, copy)
    try
        let dc = sql.GetDataContext(connectionString.Replace(original, copy))
        do
            use con = dc.CreateConnection()
            con.Open()
            use index = con.CreateCommand()
            index.CommandText <- "CREATE UNIQUE INDEX ux_categories_name ON Categories (CategoryName)"
            index.ExecuteNonQuery() |> ignore

        let category (name: string) (description: string) =
            let row = dc.Main.Categories.Create()
            row.CategoryName <- name
            row.Description <- description
            row

        let description (name: string) =
            query { for c in dc.Main.Categories do
                    where (c.CategoryName = name)
                    select c.Description }
            |> Seq.exactlyOne

        let first = category "sqlp first" "one"
        dc.SubmitUpdates()
        let second = category "sqlp second" "two"
        dc.SubmitUpdates()
        Assert.Greater(second.CategoryId, first.CategoryId)

        // Updated in place: the existing row's key, not the key of the row inserted last
        let updated = category "sqlp first" "three"
        updated.OnConflictUpdate((fun c -> c.CategoryName), fun existing inserted -> existing.Description <- inserted.Description)
        dc.SubmitUpdates()
        Assert.AreEqual(first.CategoryId, updated.CategoryId)
        Assert.AreEqual("three", description "sqlp first")

        // Left as it was: no key
        let ignored = category "sqlp first" "four"
        ignored.OnConflictDoNothing(fun c -> c.CategoryName)
        dc.SubmitUpdates()
        Assert.AreEqual(0L, ignored.CategoryId)
        Assert.AreEqual("three", description "sqlp first")

        // Without a conflict both read the new row's key
        let third = category "sqlp third" "five"
        third.OnConflictUpdate((fun c -> c.CategoryName), fun existing inserted -> existing.Description <- inserted.Description)
        dc.SubmitUpdates()
        Assert.Greater(third.CategoryId, second.CategoryId)
        let fourth = category "sqlp fourth" "six"
        fourth.OnConflictDoNothing(fun c -> c.CategoryName)
        dc.SubmitUpdates()
        Assert.Greater(fourth.CategoryId, third.CategoryId)
    finally
        // A pooled connection may still hold the file
        try File.Delete copy with _ -> ()

[<Test; SqlCheck.NoSql>]
let ``SQLite refuses to lock rows for update``() =
    let dc = sql.GetDataContext()
    let customers = query { for cust in dc.Main.Customers do select cust.CustomerId }
    let error = Assert.Throws<exn>(fun () -> customers |> Seq.lockForUpdate |> Seq.toList |> ignore)
    StringAssert.Contains("SQLite has no row locks", error.Message)
    // The executors lock too
    let viaArray = Assert.Catch(fun () -> (Array.lockForUpdateAsync customers).Wait())
    StringAssert.Contains("SQLite has no row locks", viaArray.ToString())
    let viaHead = Assert.Catch(fun () -> (Seq.lockForUpdateTryHeadAsync customers).Wait())
    StringAssert.Contains("SQLite has no row locks", viaHead.ToString())

module UtilsTests =

    [<Test; SqlCheck.NoSql>]
    let ``List.evaluateOneByOne test``() =
        // This is a helper for executing Tasks as one-by-one to not mess data connections or contexts.
        // If you use Aync, use rateher Async.Sequential
        task {
            let initList = [1;2;3;4;5;6;7;8;9]
            let processList =
                initList |> List.evaluateOneByOne(fun x -> task {
                    // Execute some query here, in a rare case that you need to hit database with N queries.
                    return x + 0
                })
            do! (processList :> Task)
            Assert.AreEqual(initList, processList.Result)
        } :> Task

    [<Test; SqlCheck.NoSql>]
    let ``List.evaluateOneByOne test, no stackoverflow``() =
        task {
            let initList = [1 .. 5000]
            let processList =
                initList |> List.evaluateOneByOne(fun x -> task { return x + 0 })
            do! (processList :> Task)
            Assert.AreEqual(initList, processList.Result)
        } :> Task

[<Test>]
let ``read only and writable contexts share the schema types``() =
    // Compiles only while dataContext alone declares the schema types: readDataContext declaring them too
    // re-parents them, and dataContext.mainSchema no longer matches once readDataContext has been used.
    let readSchema : sql.dataContext.mainSchema = sql.GetReadOnlyDataContext().Main
    let writeSchema : sql.dataContext.mainSchema = sql.GetDataContext().Main
    let readCustomers = query { for cust in readSchema.Customers do select cust.CustomerId } |> Seq.length
    let writeCustomers = query { for cust in writeSchema.Customers do select cust.CustomerId } |> Seq.length
    Assert.AreEqual(writeCustomers, readCustomers)
