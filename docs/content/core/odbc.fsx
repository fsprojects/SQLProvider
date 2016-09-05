(*** hide ***)
#I @"../../files/sqlite"
(*** hide ***)
#I "../../../bin"
(*** hide ***)
[<Literal>]
let connectionString = "Microsoft.ACE.OLEDB.12.0;Data Source=" + __SOURCE_DIRECTORY__ + @"\northwindEF.db;Version=3"
[<Literal>]
let connectionString2 = "Microsoft.ACE.OLEDB.12.0;Data Source=MyTest"
[<Literal>]
let connectionString3 = @"Driver={Microsoft Excel Driver (*.xls)};DriverId=790;Dbq=" + __SOURCE_DIRECTORY__ + @"\..\..\..\docs\files\ie_data.xls;DefaultDir=" + __SOURCE_DIRECTORY__ + @"\..\..\..\docs\files\;"

(*** hide ***)
[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + @"..\..\..\files\sqlite"
#r "FSharp.Data.SqlProvider.dll"
open FSharp.Data.Sql
(**


# ODBC

Configuring DSN on Windows ODBC Data Source Administrator server: 
Control Panel -> Administrative Tools -> Data Sources (ODBC) 
(or launch: c:\windows\syswow64\odbcad32.exe) 
and add your driver to DSN.

*)

open FSharp.Data.Sql 
[<Literal>] 
let dnsConn = @"DSN=foo" 
type db = SqlDataProvider<Common.DatabaseProviderTypes.ODBC, dnsConn>
let ctx = db.GetDataContext()

(**

Because there are a lot of different kind of ODBC connection types, you can set
quote characters for SQL-clauses with optional parameter OdbcQuote.

- `OdbcQuoteCharacter.SQUARE_BRACKETS`
- `OdbcQuoteCharacter.GRAVE_ACCENT`
- `OdbcQuoteCharacter.NO_QUOTES`

and the difference in executed SQL is:

```sql
SELECT [CourseID], [CourseName] FROM [Course] as [q] 
SELECT `CourseID`, `CourseName` FROM `Course` as `q` 
SELECT  CourseID ,  CourseName  FROM  Course  as  q 
```

*)

[<Literal>] 
let quotechar = FSharp.Data.Sql.Common.OdbcQuoteCharacter.DEFAULT_QUOTE
type db2 = SqlDataProvider<Common.DatabaseProviderTypes.ODBC, dnsConn, OdbcQuote = quotechar>
let ctx2 = db2.GetDataContext()
