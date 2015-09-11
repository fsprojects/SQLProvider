[![Issue Stats](http://issuestats.com/github/fsprojects/SQLProvider/badge/issue)](http://issuestats.com/github/fsprojects/SQLProvider)
[![Issue Stats](http://issuestats.com/github/fsprojects/SQLProvider/badge/pr)](http://issuestats.com/github/fsprojects/SQLProvider)

# SQLProvider [![NuGet Status](http://img.shields.io/nuget/v/SQLProvider.svg?style=flat)](https://www.nuget.org/packages/SQLProvider/)

A general SQL database type provider, supporting LINQ queries, schema exploration, individuals, CRUD operations and much more besides. The provider currently supports MS SQL Server, SQLite, PostgreSQL, Oracle, MySQL, ODBC and MS Access.

## Documentation

Core documentation and samples can be found at the [SQLProvider home page](http://fsprojects.github.io/SQLProvider/). This documentation is generated from [docs/content/](https://github.com/fsprojects/SQLProvider/tree/master/docs/content), so please feel free to submit a pull request if you have fixed typos are added additional samples and documentation!

## Building

* Mono: Run *build.sh*  [![Mono build status](https://travis-ci.org/fsprojects/SQLProvider.png)](https://travis-ci.org/fsprojects/SQLProvider)
* Windows: Run *build.cmd* 

## Known issues

- Database vendors other than MS SQL Server use dynamic assembly loading.  This may cause some security problems depending on your system's configuration and which version of the .NET framework you are using.  If you encounter problems loading dynamic assemblies, they can likely be resolved by applying the following element into the configuration files of  fsi.exe, devenv.exe and your program or the program using your library : http://msdn.microsoft.com/en-us/library/dd409252(VS.100).aspx

## Disclaimer

This is an alpha build and as such may have problems yet undetected. This is not suitable for production use.  I am not responsible for any inadvertent damage and destruction caused by this software. 

Always fully assert the results of your queries!

## Maintainer(s)

- [@pezipink](https://github.com/pezipink)
- [@colinbull](https://github.com/colinbull)

The default maintainer account for projects under "fsprojects" is [@fsprojectsgit](https://github.com/fsprojectsgit) - F# Community Project Incubation Space (repo management)

