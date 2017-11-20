### 1.1.23 - 20.11.2017
* Option type fix for Like-search

### 1.1.22 - 09.11.2017
* Option type fix for exactlyOneOrDefault. PR #491

### 1.1.21 - 07.11.2017
* Initial support for cross joins. PR #489

### 1.1.20 - 06.11.2017
* Option type exactlyOneOrDefault fix PR #484
* Oracle join columns fix PR #455

### 1.1.19 - 01.11.2017
* Oracle column name resolution PR #480 
* Procedure name overload fix PR #476 
* Allow option type return in where-lambda. #478 

### 1.1.18 - 26.10.2017
* Previous type provider SDK returned #472 #470 #468 #466 #465
* More info to tooltips.

### 1.1.17 - 26.10.2017
* Procedure result fix.

### 1.1.16 - 25.10.2017
* Excluded FSharp.Core dll from package #465

### 1.1.15 - 21.10.2017
* Initial support for MySqlConnector driver
* Latest type provider SDK

### 1.1.14 - 17.10.2017
* Canonical operations support for aggregation operators (SUM/AVG/MIN/MAX) in single table #459
* Latest type provider SDK
* More flexibility to type casting on basic math canonical operations

### 1.1.12 - 11.10.2017
* Initial version of .NET Standard 2.0 / .NET Coreapp 2.0 support
* Breaking change: The original dll path is now under net451 folder.
* SQLite: Performance improvements. Option to use Microsoft.Data.SQLite 
* MySql: Support for MySQL.Data.dll v6.10-rc3 and v8.0.9dmr (but 6.9.9 stable still recommended)
* Reference component assembly loading and error messages improved.

### 1.1.11 - 26.09.2017
* Firebird quotes fix PR #453

### 1.1.10 - 22.09.2017
* Access to command timeout #447
* Support Postgres arrays #450

### 1.1.8 - 07.09.2017
* Breaking change for MYSQL/MariaDB: Unsigned types mapping fixed #437
* Fix for MSSQL tables with no-values insert #440

### 1.1.7 - 10.08.2017
* Canonical operation AddMinutes supports now other sql-columns as parameter.

### 1.1.6 - 19.07.2017
* String to datetime parsing with DateTime.Parse in SQL will convert the types if ok for db.

### 1.1.5 - 05.07.2017
* Fixed (sortBy desc + skip + take) combination #432

### 1.1.4 - 19.06.2017
* ConnectionStringName when not running from application folder #428
* sortBy Key support to groupBy #429

### 1.1.3 - 02.06.2017
* Fix for issue when Oracle a sproc returns a cursor with an actual type
* FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent has now better structure
* Async stored procedure execution support (InvokeAsync)
* PostgreSQL 3.2 support (when System.Threading.Tasks.Extensions.dll near)

### 1.1.2 - 15.05.2017
* Mysql: more efficient schema queries, PR #415
* Firebird: Stored Procedures, PR #414

### 1.1.1 - 13.05.2017
* Support for Mono 5 #413

### 1.1.0 - 10.05.2017
* Support for Firebird SQL Server

### 1.0.57 - 09.05.2017
* Sort-by casts to IComparable fix #407
* Oracle Length and Trim fix #408
* Column <> null fix

### 1.0.55 - 08.05.2017
* Support canonical functions: Convert some basic .NET-functions to SQL.
* Support delete from -query to delete many items with a single clause.

### 1.0.54 - 26.04.2017
* Oracle connection leak fix #397

### 1.0.53 - 21.04.2017
* Oracle Delete causes ORA-00932 fix #394
* Misc minor fixes for MS-Access
* Group-by fixed over empty result groups

### 1.0.52 - 09.04.2017
* Initial support for 2 column key on group-by

### 1.0.51 - 06.04.2017
* Oracle comments fix

### 1.0.50 - 05.04.2017
* Fixed: Support for group-by where condition outside group-by operation

### 1.0.49 - 05.04.2017
* Database table and column descriptions to editor tooltips. #385 
* ODBC fixes.

### 1.0.47 - 18.03.2017
* Support for updating records on MySQL before 5.5 #384

### 1.0.46 - 14.03.2017
* Fix for #380, where-conditions on joined tables with itm.x > itm.y

### 1.0.45 - 13.03.2017
* More async operations.

### 1.0.44 - 12.03.2017
* GetDataContext overload for passing in the TransactionOptions instance
* On where-clauses plain SQL-columns can be now on both sides of operations

### 1.0.43 - 22.02.2017
* Added Array.executeQueryAsync and List.executeQueryAsync

### 1.0.42 - 03.02.2017
* SQLite spacing fix #371
* Cast fixes #370

### 1.0.41 - 11.01.2017
* Oracle update fix. #367

### 1.0.40 - 23.12.2016
* Breaking change for number-columns: naming fixed. #355

### 1.0.38 - 22.12.2016
* One table one key-column GROUP-BY support. #358
* HasColumn(niceName) method for entities

### 1.0.37 - 09.11.2016
* UNION queries
* Support Space in MSSQL inserts
* Support Space in MySQL table names #347
* TableNames parameter for MSSQL
* Fix Access unknown error #340

### 1.0.36 - 25.10.2016
Support views in Oracle table list

### 1.0.35 - 08.10.2016
* Fix for SQL CRUD operations, #338

### 1.0.34 - 04.10.2016
* Fix for contains search

### 1.0.33 - 16.09.2016
* Fix for primary key discovery on oracle #331

### 1.0.32 - 05.09.2016
* SQLite text-format date comparison fix #328
* Odbc quote characters option #327

### 1.0.31 - 27.08.2016
* Where-clause optimization and fix

### 1.0.30 - 27.08.2016
* Oracle performance improvements
* Postgre nullable insert fix
* SQL generation improvements
* MsAccess fix

### 1.0.29 - 23.08.2016
* Support for composite (multi-key-column) table joins
* Support for let-keywords

### 1.0.28 - 19.08.2016
* Configuration error info improvements

### 1.0.27 - 18.08.2016
* Added SourceLink (for debugging possibility).

### 1.0.26 - 15.08.2016
* CRUD support for composite key tables

### 1.0.25 - 13.08.2016
* Fixed functionality with some specially named tables and columns

### 1.0.24 - 05.08.2016
* Fixed exception on queries where are multiple times "take 1"

### 1.0.23 - 02.08.2016
* Fixed delete on ProcessUpdatesAsync

### 1.0.22 - 02.07.2016
* Async query sort order fix on ordered query

### 1.0.21 - 29.06.2016
* query: where (myTable.MyBoolean) -support
* MsSql Mono DateTime fix

### 1.0.20 - 23.06.2016
* Nested IN-queries

### 1.0.19 - 23.06.2016
* More query operators

### 1.0.18 - 21.06.2016
* Oracle large table count support
* Simple Linq contains query

### 1.0.17 - 15.06.2016
* Fixed SQLite IN-queries (had problem with all the parameters being param1)
* Added ys.Contains(x) to create IN-clause as well as |=|

### 1.0.16 - 14.06.2016
* Some concurrency fixes (#282)

### 1.0.15 - 13.06.2016
* Fix for #279

### 1.0.13 - 11.06.2016
* Byte array to Blob fixes for SQLite and MySQL
* Some performance fixes.

### 1.0.12 - 01.06.2016
* Odbc fixed (on some level).
* SQLite fix for reserved keywords table names.

### 1.0.11 - 31.05.2016
* MsSql IN with empty collection
* Added Linq Any support (contains)

### 1.0.10 - 29.05.2016
* Concurrency fix for SubmitUpdates()

### 1.0.9 - 29.05.2016
* Fixed possibility to call SubmitUpdates many times after delete. 

### 1.0.8 - 28.05.2016
* .PossibleError property to notify schema or database name (Owner)
* Fixed select string concat, e.g: Select (person.FirstName + " " + person.LastName)
* minBy and maxBy for DateTime fields
* Fixed operating table with no primary key

### 1.0.7 - 27.05.2016
* Better error reporting
* SQLite skip fix

### 1.0.6 - 15.05.2016
* sumBy, maxBy, minBy, averageBy

### 1.0.5 - 09.05.2016
* More async operations: Seq.lengthAsync, Seq.headAsync, Seq.tryHeadAsync

### 1.0.4 - 06.05.2016
* PostgreSQL types, Npgsql 2 and 3. (PR #261)

### 1.0.3 - 06.05.2016
* Improved option type usage in join and where clauses (PR #260)

### 1.0.2 - 05.05.2016
* Fixed: Insert did return object as id, not the id wanted.

### 1.0.1 - 04.05.2016
* Many community bug-fixes and PRs
* Documentation improvements
* Async operations (PR #257)

### 1.0.0 - 16.04.2016
* CRUD in Access (PR #211)
* Better error handling (PR #233)
* Better thread synchronisation (PR #222)
* General bug fixes
* V1 release

### 0.0.11-alpha - 02.01.2016
* Type provider generates nicer table names, relationships and sprocs
* Added static parameter to control case senstivity when generating queries (PR #143)
* Fixed relative paths for SQL lite
* Fixed differences in path between mono and .net for sql lite
* Added ability to use a connection string referenced in app or web config
* Stored procedures are now lazily generated
* Improved Postgre Type mappings (PR #127)
* Documentation improvements (PR #137, PR #148, PR #150)
* Fixes a regression that prevented UPPER on SqlServer (PR #149)
* Fixes for the LINQ expression converter (PR #131)

#### 0.0.10-alpha - Unreleased
* Fixed a regression when building oracle relationships (PR # 126)
* Fixes Postgres Type mapping issues (PR #117)
* Fixes command already disposed exception (PR #124)
* Fixes type mapping issues in Access provider (PR #122)
* Fixes issues with optional tyepd properties (Issue #119, PR #116)
* Converted project to use paket and latest project scaffold. (PR #113)
* Added a function to retrieve the primary key for the table. (PR #105)
* Fix for SQLite not releasing DB file (Issue #99, PR #100)
* Support for stored procedures on DB's that support them (PR #83)
* Support for SQLite on Mono (PR #81)

#### 0.0.9-alpha - 21.05.2014
* Fixing a bug with writing tinyints in SQL Server
* Fixes to MySQL array parameters
* You can now use any sequence with the in and not in operators, as opposed to just new array expressions
* Fix in PostgreSQL to allow the creation of entities with no set properties

#### 0.0.8-alpha - 20.05.2014
* Fixes an important bug that was causing all columns to be selected

#### 0.0.7-alpha - 17.05.2014
* ODBC support added
* CRUD operations added
* Tons of fixes and tweaks 

#### 0.0.6-alpha - 12.02.2014 
* MS Access support added!
* Various fixes for SQLite and Oracle
* Added support for head / First()

#### 0.0.5-alpha - 28.01.2014 
* Oracle support added!
* Various minor fixes

#### 0.0.4-alpha - 26.01.2014 
* MySQL support added!
* SQLite would only work if all tables had at least one FK constraint - fixed
* Various option type fixes
* GraphViz script to help visualize LINQ trees when debugging

#### 0.0.3-alpha - 24.12.2013 
* PostgreSQL support
* Option Types for nullable columns
* Various tweaks and cleanup
* Count() support in queries

#### 0.0.2-alpha - 16.12.2013 
* Initial release of Sqlprovider
