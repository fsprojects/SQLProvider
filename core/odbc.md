# ODBC

SQLProvider with ODBC is available via both NuGet-Packages:

* NuGet: [SQLProvider.Odbc](https://www.nuget.org/packages/SQLProvider.Odbc) - Fixed libraries. Doesn't need resolution path. TypeProvider class: `FSharp.Data.Sql.Odbc.SqlDataProvider`
* NuGet: [SQLProvider](https://www.nuget.org/packages/SQLProvider) - Dynamic version. TypeProvider class: `FSharp.Data.Sql.SqlDataProvider`

### DSN

Configuring DSN on Windows ODBC Data Source Administrator server:
Control Panel -&gt; Administrative Tools -&gt; Data Sources (ODBC)
(or launch: c:\windows\syswow64\odbcad32.exe)
and add your driver to DSN.

```fsharp
open FSharp.Data.Sql
[<Literal>]
let dnsConn = @"DSN=foo"
type db = SqlDataProvider<Common.DatabaseProviderTypes.ODBC, dnsConn>
let ctx = db.GetDataContext()
```

You don't need DSN for all data sources, e.g. MS Access can be used directly
through the database file with a connection string like:
<code>@"Driver={Microsoft Access Driver (&#42;.mdb, *.accdb)};Dbq=C:\Northwind.mdb"</code>

### Quote Character

Because there are a lot of different kind of ODBC connection types, you can set
quote characters for SQL-clauses with optional parameter OdbcQuote.

* `OdbcQuoteCharacter.SQUARE_BRACKETS`
* `OdbcQuoteCharacter.GRAVE_ACCENT`
* `OdbcQuoteCharacter.NO_QUOTES`
* `OdbcQuoteCharacter.DOUBLE_QUOTES`
* `OdbcQuoteCharacter.APHOSTROPHE`

and the difference in executed SQL is:

SELECT [CourseID], [CourseName] FROM [Course] as [q]
SELECT `CourseID`, `CourseName` FROM `Course` as `q`
SELECT  CourseID ,  CourseName  FROM  Course  as  q
SELECT "CourseID", "CourseName" FROM "Course" as "q"
SELECT 'CourseID', 'CourseName' FROM 'Course' as 'q'

```fsharp
[<Literal>]
let quotechar = FSharp.Data.Sql.Common.OdbcQuoteCharacter.DEFAULT_QUOTE
type db2 = SqlDataProvider<Common.DatabaseProviderTypes.ODBC, dnsConn, OdbcQuote = quotechar>
let ctx2 = db2.GetDataContext()
```

### DTC Transactions

SQLProvider will do DTC-transactions over CRUD-operations. That will ensure
that all the database operations will either succeed or fail at once; when you do
`ctx.SubmitUpdates()`.

However, some ODBC-drivers (like MS-Access) don't support
DTC-transactions and will fail on the constructor call. The transaction creation can be
disabled in context creation:

```fsharp
let ctx3 =
    db.GetDataContext(
        { Timeout = TimeSpan.MaxValue;
          IsolationLevel = Transactions.IsolationLevel.DontCreateTransaction
        }:FSharp.Data.Sql.Transactions.TransactionOptions)
```

Don't disable transactions if you don't need to.
