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
