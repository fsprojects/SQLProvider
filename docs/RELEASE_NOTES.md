### 1.5.17 - 14.09.2025
* DateTimeOffset function translations
* Faster disposing of commands
* No reflection on SQLProvider.SQLite if pre-defined library

### 1.5.16 - 09.09.2025
* XML documentation updates
* Minor memory and performance improvements

### 1.5.14 - 29.08.2025
* Fix for Dacpac parsing null exception case when dependent dacpacs #848
* Firebird naming case.
* DuckDB dependencies update

### 1.5.13 - 21.07.2025
* MapTo fix on special cases where database is missing fields of MapTo FSharp record target.

### 1.5.11 - 24.06.2025
* Improved BigInteger support

### 1.5.10 - 19.06.2025
* Some SQL gereration optimizations

### 1.5.9 - 05.06.2025
* Moved internal methods from SqlEntity under a new interface IColumnHolder. 
* ...which is potential breaking change: If you still use them, cast first (entity :> IColumnHolder).GetColumn
* Moved TemplateAsRecord templates under DesignTimeCommands
* Better shared caching between same connection contexts.

### 1.5.8 - 04.06.2025
* MapTo support Option and ValueOption on target types
* TemplateAsRecord to generate list of properies and types
* Minor performance improvements
* Postgres case sensitivity fix

### 1.5.7 - 13.05.2025
* Performance improvement: ProvidedTypes update

### 1.5.6 - 06.05.2025
* Mock-database case sensitivity backup search

### 1.5.5 - 05.05.2025
* MySql boolean mapping fix.

### 1.5.4 - 02.05.2025
* SQLProvider.Oracle, SQLProvider.MySql and SQLProvider.MySqlConnector loading without reflection.

### 1.5.3 - 30.04.2025
* Type-provider package library and target framework fixes
* Compile-time assembly loading fixes to get rid of "platform not supported" on .NET 8.0/9.0

### 1.5.0 - 24.04.2025
* Packaging restructure

### 1.4.13 - 08.04.2025
* SSDT: Initial support for SQL functions, PR #781
* Expression.optimizer update

### 1.4.12 - 12.03.2025
* Some performance updates
* Memory consumption reduced

### 1.4.9 - 23.02.2025
* PostgreSQL primary key uppercase fix, PR #842

### 1.4.8 - 13.02.2025
* Dependency updates

### 1.4.7 - 07.02.2025
* Postgres 8 TimestampTz fix part 2

### 1.4.6 - 07.02.2025
* More design-time refactorings
* Assembly loading improvement
* Postgres 8 TimestampTz fix

### 1.4.5 - 04.02.2025
* Design-time refactorings

### 1.4.4 - 04.02.2025
* Update to support segregation of read and read+write operations on type-level (via dataContext and readDataContext)

### 1.4.3 - 15.01.2025
* TypeProviders SDK update

### 1.4.2 - 07.11.2024
* Performance optimization: Faster type-checks

### 1.4.1 - 18.10.2024
* Fixed navigation propery parameter names on async aggregate operations
* Better ConditionalExpression with ConstantExpression evaluation

### 1.4.0 - 27.09.2024
* F# dependency from 6.0.7 to 8.0.301
* Build with .NET 8.0 compiler
* .NET Framework 4.7.2 to 4.8

### 1.3.54 - 13.02.2025
* Dependency updates

### 1.3.53 - 07.02.2025
* More design-time refactorings
* Assembly loading improvement

### 1.3.52 - 04.02.2025
* Design-time refactorings

### 1.3.51 - 04.02.2025
* Update to support segregation of read and read+write operations on type-level (via dataContext and readDataContext)

### 1.3.50 - 15.01.2025
* TypeProviders SDK update

### 1.3.49 - 18.10.2024
* Performance optimization: Faster type-checks

### 1.3.48 - 18.10.2024
* Fixed navigation propery parameter names on async aggregate operations

### 1.3.47 - 09.10.2024
* Better ConditionalExpression with ConstantExpression evaluation

### 1.3.46 - 16.09.2024
* Ssdt performance optimisations, and some lists to arrays to reduce memory footprint, PR#832
* May need refresh for saved SchemaCache due to list to array change.

### 1.3.45 - 09.09.2024
* Fix cached return columns for function, PR#830
* Support for more complex sortBys
* More efficient async-head queries, PR#831

### 1.3.43 - 08.07.2024
* Fixed potential regression issue of 1.3.42

### 1.3.42 - 04.07.2024
* Minor performance improvements

### 1.3.41 - 29.06.2024
* More improvements on group-by queries support

### 1.3.40 - 27.06.2024
* Improvements on group-by queries

### 1.3.39 - 21.06.2024
* Small performance optimisations

### 1.3.38 - 05.06.2024
* Oracle, Postges and Firebird: Support computed columns, PR #824

### 1.3.37 - 25.05.2024
* Initial support for DuckDB, PR #823

### 1.3.36 - 19.04.2024
* ResolutionPath assembly load fix, fix for #818

### 1.3.35 - 09.04.2024
* Support navigation property queries in mocks

### 1.3.34 - 04.04.2024
* Minor performance improvement

### 1.3.33 - 01.04.2024
* Small memory footprint reduction

### 1.3.32 - 27.03.2024
* Postgres error message improvement

### 1.3.31 - 25.03.2024
* Trim paths in semicolon separator in ResolutionPath static parameter

### 1.3.30 - 18.03.2024
* IWithDataContext moved from FSharp.Data.Sql.Runtime to FSharp.Data.Sql.Common
* Support Create(...) in unit-tests

### 1.3.29 - 12.03.2024
* SQLite transactions support for Microsoft.Data.Sqlite driver, PR #817

### 1.3.28 - 11.03.2024
* Support for semicolon separator in ResolutionPath static parameter
* Expression optimization update

### 1.3.27 - 29.02.2024
* Mock context improved

### 1.3.26 - 27.02.2024
* Helper method for unit-testing SQL: way to mock data-context

### 1.3.25 - 27.02.2024
* Helper method for unit-testing SQL: way to mock data-tables

### 1.3.24 - 23.02.2024
* Support implicit converts from AddDays, AddHours, AddMinutes and AddSeconds int to float

### 1.3.23 - 11.01.2024
* PostgreSQL fixed invalid cast on array_dimensions on new npgsql driver.
* Microsoft SQL Database reference driver update

### 1.3.22 - 13.11.2023
* Minor performance improvements via ProvidedTypes update

### 1.3.21 - 10.11.2023
* Support Set in LINQ queries

### 1.3.19 - 30.10.2023
* Fixed param picking from nested SQL with navigation properties
* Added Int32.Parse to SQL translations

### 1.3.18 - 22.10.2023
* SqlEventData rendering improved

### 1.3.17 - 21.10.2023
* More performance optimizations of non-trivial queries

### 1.3.16 - 21.10.2023
* Individuals added to ContextSchemaCache
* Better parameter type instantiation 
* Fixed null-refernce exception on some database drivers

### 1.3.15 - 19.10.2023
* Less type-loading requirements 
* Better parameter type instantiation

### 1.3.14 - 10.10.2023
* More performance updates
* Support FSharp built-in isNull function

### 1.3.12 - 07.10.2023
* Some performance updates PR #802

### 1.3.11 - 13.07.2023
* SubmitUpdates time-based ordering PR #797

### 1.3.10 - 19.06.2023
* Even better way of updating thousands of entities

### 1.3.9 - 15.06.2023
* Avoid stackoverflow when updating thousands of entities

### 1.3.8 - 12.05.2023
* MySql support for OnConnflict, PR #790

### 1.3.7 - 08.03.2023
* ODBC: Support for views, PR #788
* SQLite: Support for columns outside pragma info PR #787
* Potetial breaking change for users of List.evaluateOneByOne: Fixed it not to reverse the order of output list.

### 1.3.6 - 09.01.2023
* User defined data type support for SQL Server SSDT 

### 1.3.5 - 20.12.2022
* Experimental use of Microsoft.Data.SqlClient in .net6.0 and .netstandard2.1

### 1.3.4 - 04.12.2022
* Always quote schema name with backticks in MySQL PR #777
* Smaller runtime by removing unnecessary code

### 1.3.3 - 18.11.2022
* Reference updates: FSharp.Core to 6.0.7 and SQLClient to 4.8
* Minor performance twaks
* SQLite better error reporting

### 1.3.2 - 08.11.2022
* Improvement for value tuple handling

### 1.3.1 - 10.08.2022
* Breaking change: Async<_> changed to Task<_>
* FSharp.Core dependency: Minimum 4.7.x -> 6.0.5
* Breaking change: UseOptionTypes: bool changed to FSharp.Data.Sql.Common.NullableColumnType
* List.evaluateOneByOne - workaround for 6.0.5 FSharp.Core task stack-overflow with over 1200 items

### 1.2.12 - 04.08.2022
* Fix for .NET 6 project with PublishSingleFile, PR #769

### 1.2.11 - 21.02.2022
* More efficient regex parsing table name
* Async readentities stackoverflow on AwaitTask fixed when over 2700

### 1.2.10 - 21.10.2021
* SQL server null parameter fix

### 1.2.9 - 09.08.2021
* Read .dacpac file from SSDT on Read-only File System, PR #745

### 1.2.8 - 09.08.2021
* Dacpac search locations extended, Azure Functions .dacpac Resolution Fix, PR #742

### 1.2.7 - 19.07.2021
* GroupBy parameter column name improvements

### 1.2.6 - 12.07.2021
* Seq.exactlyOneAsync and Seq.tryExactlyOneAsync added PR #735

### 1.2.5 - 28.06.2021
*  .dacpac table descriptions fixed

### 1.2.4 - 28.06.2021
*  .dacpac fix stored procedure returning multiple results

### 1.2.3 - 25.06.2021
*  .dacpac fix stored procedure call from different schema

### 1.2.2 - 25.06.2021
*  .dacpac fix stored procedure return parameters execution getting the results

### 1.2.1 - 15.03.2021
*  .dacpac file Ionide fix, PR #718

### 1.2.0 - 05.03.2021
* Packaged from the .NET Standard branch (merged to master).
* Removed support from .NET 3.5 and .NET 4.6.1: The least is .NET 4.7.2.

### 1.1.102 - 03.04.2022
* Service release due to NuGet downloads of old 1.1.101
* Taken some of 1.3.x SQL and performance improvements and memory reductions to 1.1.x.
* No dependencies updates, no still using old async, etc.

### 1.1.101 - 07.02.2021
* Minor SSDT improvements

### 1.1.100 - 24.01.2021
* SSDT fixes, stored procedures return type fix PR #711

### 1.1.99 - 12.01.2021
* SSDT type annotations PR #707

### 1.1.98 - 02.01.2021
* SSDT moved to use dacpac PR #704

### 1.1.97 - 25.12.2020
* SSDT improvements, views, sprocs PR #703

### 1.1.96 - 21.12.2020
* SSDT improvements PR #702

### 1.1.95 - 19.12.2020
* Initial SSDT support PR #700

### 1.1.94 - 27.11.2020
* More query support: where (table.NullableColumn = optionValue && not boolean_column)

### 1.1.93 - 13.10.2020
* Added capability to load drivers from AppDomain, PR #694

### 1.1.92 - 18.09.2020
* MsSQL and MySQL: Removed computed column from Create parameters list (insert clause)

### 1.1.91 - 04.06.2020
* MySQL procedure calls fixed
* Error handling and dll loading improvements

### 1.1.90 - 04.06.2020
* DatabaseProviderTypes.MSSQLSERVER_DYNAMIC to support Microsoft.Data.SqlClient.dll
* Improved SQLServer Udt parameters
* Caching relaxed for easier development
* Caching and reference assembly loading improvements

### 1.1.89 - 02.06.2020
* Reference assembly loading improvements
* Caching relaxed for easier development

### 1.1.88 - 31.05.2020
* Caching improvements
* Possibility to group full entities over multiple keys

### 1.1.87 - 20.05.2020
* Fixed issue on multiple different same column group-by calculations

### 1.1.86 - 05.05.2020
* Design Time Command methods to properties for easier usability.
* Fix for insert and update of SQLServer spatial data types #157

### 1.1.85 - 01.05.2020
* Moved SaveContextSchema under Design Time Commands -property
* Added ClearDatabaseSchemaCache under Design Time Commands -property
* Initial support to query SQLServer spatial types: geometry, geography, hierarchyid

### 1.1.84 - 26.02.2020
* Non-async query aggregates: Seq.sumQuery, Seq.maxQuery, ...
* Supported Group-by key-column count increased

### 1.1.83 - 11.02.2020
* MS Access in .NET Core dll deployment

### 1.1.82 - 10.02.2020
* Fix for MS Access in .NET Core

### 1.1.81 - 05.02.2020
* Fix for regression of fixing the async stored procedure exception handling, #667

### 1.1.79 - 04.02.2020
* Fix for async stored procedure exception handling, #667
* MS Access provider included in .NET Core
* .NET Core package references updated, e.g. System.Data.SqlClient

### 1.1.76 - 10.12.2019
* TypeProvider SDK-update: Perf by caching improvements

### 1.1.75 - 18.11.2019
* More alias query generation fixes

### 1.1.74 - 12.11.2019
* Fix for #652, alias generation

### 1.1.73 - 11.11.2019
* Better filter support to group-by join over 2 or 3 tables

### 1.1.72 - 01.11.2019
* Possibility to join 2 or 3 tables to group-by PR #650
* Better let and into -keyword handling PR #648

### 1.1.71 - 01.11.2019
* Fixed where-before-join -query
* Param naming fix, part 2

### 1.1.68 - 10.09.2019
* Query-translation fixes #634
* Initial exist-sub-query support, PR #603

### 1.1.67 - 29.08.2019
* Param naming fix, PR #632

### 1.1.66 - 31.07.2019
* Math.pow operator
* Typo fix, PR #627

### 1.1.65 - 06.06.2019
* Left join parameter aliases underscore fix

### 1.1.64 - 04.06.2019
* Views for SQLite, PR #618

### 1.1.63 - 14.05.2019
* Fix for multiple outer joins #614

### 1.1.62 - 23.04.2019
* .NET Core editor fix, PR #611

### 1.1.61 - 09.04.2019
* Reverted .NET Standard library to fix stackoverflow issue

### 1.1.60 - 20.03.2019
* Experimental ProvidedTypes.fs to fix .NET Standard stackoverflow issue

### 1.1.59 - 02.03.2019
* Provided types update
* .NET Standard ODBC support

### 1.1.58 - 19.01.2019
* SQL Server scalar function return parameter fix, #598

### 1.1.57 - 04.01.2019
* More asynchronous queries, fix #519

### 1.1.56 - 02.01.2019
* SQLite support for views #591

### 1.1.55 - 13.12.2018
* Left join with op_bangbang to non-fk columns #588

### 1.1.54 - 13.12.2018
* FIPS compliance #586

### 1.1.53 - 30.11.2018
* Fix old SQL server paging, #581
* Multiple similar sub-queries: parameter name generation fix
* Alias resolving fixed on async aggregate operations in multi-table queries

### 1.1.52 - 13.09.2018
* No limitation of 7 joins, fixes #190

### 1.1.51 - 10.09.2018
* Corrected use of concurrent dictionary in MS-SQL ServerVersion

### 1.1.50 - 05.09.2018
* TypeProvider SDK update
* Context schema path fix PR #560

### 1.1.49 - 15.08.2018
* MsSql: Add brackets around the OUTPUT column name of the INSERT command PR #567
* ContextSchemaCache to support stored procedures #PR 566

### 1.1.48 - 27.07.2018
* Ordering fixed on async sproc resultset

### 1.1.47 - 26.07.2018
* Firebird provider fix PR #562
* Fix for SQL Server sproc output parameters PR #564
* More asynchronous sprocs execution
* CloneTo to attach entities to contexts, #561

### 1.1.45 - 05.07.2018
*  Paging support for SQL Server 2008, PR #558
*  Upsert, PR #557

### 1.1.44 - 25.06.2018
* Extend SQLEntity.MapTo to allow mapping when field names are different than column names, PR #556

### 1.1.43 - 12.06.2018
* Comparison being flipped around when a constant comes first #553

### 1.1.42 - 03.05.2018
* Context schema cache support PR #545
* Improved option type support in convertTypes. PR #544
* Fixed broken overload of Create-method #545

### 1.1.41 - 16.04.2018
* Oracle: Added support for TIMESTAMP with precision specification. 
* Generate Create method params: nullable / default values / identity columns
* Fixed incorrect table alias on canonical join issue #533
* Reverted some component updateds to address build issue and .Net standard issue

### 1.1.40 - 10.04.2018
* Fix to allow async procedure calls without return values #535

### 1.1.39 - 28.03.2018
* Allow specifying multiple schemas for PostgreSQL / MySql, PR #530

### 1.1.38 - 14.03.2018
* Group-by key canonical operation support
* Crossjoin support for recent LINQ-trees
* Reference component updates

### 1.1.37 - 01.03.2018
* Add ConnectionStringHash property to SqlEventData PR #524

### 1.1.36 - 24.02.2018
* distinct count support

### 1.1.35 - 15.02.2018
* selectOperations parameter to enable running canonical operations on select clause, PR #518

### 1.1.34 - 15.02.2018
* Access Date cast fix
* ToString() as canonical operation
* Case-clause alias resolution fix

### 1.1.33 - 14.02.2018
* Canonical operations chaining type-checking fixed

### 1.1.32 - 13.02.2018
* initial case-when-else support on where-clauses, PR #515
* mapping SQL server time to TimeSpan, PR #517
* fixed canonical operations when constant is before column

### 1.1.31 - 01.02.2018
* Oracle fix insert rows into table without primary key #514

### 1.1.30 - 01.02.2018
* Postgres ltree mapping PR #510
* Single table group-by over a constant value PR #511
* More aggregate functions: StdDev, Variance
* More canonical math functions: Sqrt, Sin, Cos, Tan, ASin, ACos, ATan, Math.Max and Math.Min

### 1.1.29 - 25.01.2018
* Firebird fixes for numeric columns and column descriptions, PR #508

### 1.1.28 - 19.01.2018
* More dynamic naming for nested parameters

### 1.1.27 - 19.01.2018
* Support for parameters in union queries #505
* Support for string notlike-operations with LINQ syntax: not(x.StartsWith("..."))

### 1.1.26 - 12.01.2018
* Updated the Type Provider SDK

### 1.1.25 - 10.01.2018
* Fix base alias in Firebird Provider PR #503

### 1.1.24 - 10.01.2018
* Fix for Postgres "Invalid procedure" #482
* Fix for Postgres arrays on Mono
* Fix for LINQ where(not(xs.Contains(subquery)))
* Support for box-operator in null-checks to make easier checking of inner-join nulls.

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
