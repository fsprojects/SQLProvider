[![Issue Status](https://img.shields.io/github/issues/fsprojects/SQLProvider.svg?style=flat)](https://github.com/fsprojects/SQLProvider/issues)
[![PR Status](https://img.shields.io/github/issues-pr/fsprojects/SQLProvider.svg?style=flat)](https://github.com/fsprojects/SQLProvider/pulls)

# SQLProvider [![NuGet Status](http://img.shields.io/nuget/v/SQLProvider.svg?style=flat)](https://www.nuget.org/packages/SQLProvider/)

[![Join the chat at https://gitter.im/fsprojects/SQLProvider](https://badges.gitter.im/fsprojects/SQLProvider.svg)](https://gitter.im/fsprojects/SQLProvider?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A general .NET/Mono SQL database type provider. Current features :
 * LINQ queries
 * Lazy schema exploration 
 * Automatic constraint navigation
 * Individuals 
 * Transactional CRUD operations with identity support
 * Two-way data binding
 * Stored Procedures
 * Functions
 * Packages (Oracle)
 * Composable Query integration
 * Optional option types
 * Mapping to record types
 * Custom Operators
 * Supports Asynchronous Operations
 * Supports .NET Standard / .NET Core 
 * Supports saving DB schema offline, and SQL-Server *.dacpac files
  
The provider currently has explicit implementations for the following database vendors : 
* SQL Server
* SQLite
* PostgreSQL
* Oracle
* MySQL (& MariaDB)
* MsAccess
* Firebird
* DuckDB

There is also an ODBC provider that will let you connect to any ODBC source with limited features. 

## Documentation

 [SQLProvider home page](https://fsprojects.github.io/SQLProvider/) contains the core documentation and samples. This 
documentation originates from 
[docs/content/](https://github.com/fsprojects/SQLProvider/tree/master/docs/content), 
so please feel free to submit a pull request if you fix typos or add 
additional samples and documentation!

## Building

* Mono: Run *build.sh*  [![Mono build status](https://travis-ci.org/fsprojects/SQLProvider.svg?branch=master)](https://travis-ci.org/fsprojects/SQLProvider)
* Windows: Run *build.cmd* [![Build status](https://ci.appveyor.com/api/projects/status/ngbj9995twhfqn28/branch/master?svg=true)](https://ci.appveyor.com/project/colinbull/sqlprovider-ogy2l/branch/master)

## Known issues

- Database vendors other than SQL Server and Access use dynamic assembly loading.  This 
may cause some security problems depending on your system's configuration and 
which version of the .NET framework you are using.  If you encounter problems 
loading dynamic assemblies, they can likely be resolved by applying the 
following element into the configuration files of  fsi.exe, devenv.exe and 
your program or the program using your library: http://msdn.microsoft.com/en-us/library/dd409252(VS.100).aspx

## Example

![No OR-mapping: FSharp compiles your database to .NET-types.](https://raw.githubusercontent.com/fsprojects/SQLProvider/master/docs/files/sqlprovider.gif "No OR-mapping: FSharp compiles your database to .NET-types.")

## Maintainer(s)

- [@pezipink](https://github.com/pezipink)
- [@colinbull](https://github.com/colinbull)
- [@Thorium](https://github.com/Thorium)

The default maintainer account for projects under "fsprojects" is [@fsprojectsgit](https://github.com/fsprojectsgit) - F# Community Project Incubation Space (repo management)

## Nuget Packages
| Database | Nuget Package | TypeProvider Class | NuGet Status |
| ------- | ----- | -----------| ---------- | 
| Microsoft SQL Server | [SQLProvider.MsSql](https://www.nuget.org/packages/SQLProvider.MsSql) | FSharp.Data.Sql.MsSql.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.MsSql) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.MsSql)](https://www.nuget.org/packages/SQLProvider.MsSql) 
| PostgreSQL | [SQLProvider.PostgreSql](https://www.nuget.org/packages/SQLProvider.PostgreSql) | FSharp.Data.Sql.PostgreSql.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.PostgreSql) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.PostgreSql)](https://www.nuget.org/packages/SQLProvider.PostgreSql) 
| MySQL and MariaDB | [SQLProvider.MySqlConnector](https://www.nuget.org/packages/SQLProvider.MySqMySqlConnectorl) |  FSharp.Data.Sql.MySqlConnector.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.MySqlConnector) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.MySqlConnector)](https://www.nuget.org/packages/SQLProvider.MySqMySqlConnectorl) 
| MySQL | [SQLProvider.MySql](https://www.nuget.org/packages/SQLProvider.MySql)  | FSharp.Data.Sql.MySql.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.MySql) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.MySql)](https://www.nuget.org/packages/SQLProvider.MySql) 
| Oracle | [SQLProvider.Oracle](https://www.nuget.org/packages/SQLProvider.Oracle)  | FSharp.Data.Sql.Oracle.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.Oracle) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.Oracle)](https://www.nuget.org/packages/SQLProvider.Oracle) 
| Any ODBC connection | [SQLProvider.Odbc](https://www.nuget.org/packages/SQLProvider.Odbc) | FSharp.Data.Sql.Odbc.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.Odbc) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.Odbc)](https://www.nuget.org/packages/SQLProvider.Odbc) 
| SQLite | [SQLProvider.SQLite](https://www.nuget.org/packages/SQLProvider.SQLite) | FSharp.Data.Sql.SQLite.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.SQLite) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.SQLite)](https://www.nuget.org/packages/SQLProvider.SQLite) 
| Microsoft Access | [SQLProvider.MsAccess](https://www.nuget.org/packages/SQLProvider.MsAccess)  | FSharp.Data.Sql.MsAccess.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.MsAccess) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.MsAccess)](https://www.nuget.org/packages/SQLProvider.MsAccess) 
| FireBird | [SQLProvider.FireBird](https://www.nuget.org/packages/SQLProvider.FireBird) | FSharp.Data.Sql.FireBird.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.FireBird) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.FireBird)](https://www.nuget.org/packages/SQLProvider.FireBird) 
| DuckDb | [SQLProvider.DuckDb](https://www.nuget.org/packages/SQLProvider.DuckDb) | FSharp.Data.Sql.DuckDb.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider.DuckDb) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider.DuckDb)](https://www.nuget.org/packages/SQLProvider.DuckDb) 
| General, all via manual config | [SQLProvider](https://www.nuget.org/packages/SQLProvider) | FSharp.Data.Sql.SqlDataProvider | [![Nuget](https://img.shields.io/nuget/v/SQLProvider) ![Nuget](https://img.shields.io/nuget/dt/SQLProvider)](https://www.nuget.org/packages/SQLProvider) 


Depending on the used provider, the namespace of the SqlDataProvider type class varies as above.