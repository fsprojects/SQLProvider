# SQLProvider

A general SQL database type provider, supporting LINQ queries, schema exploration, individuals and much more besides.

The provider currently supports MS SQL Server, SQLite, PostgreSQL, Oracle and MySQL.  All database vendors except SQL Server will require 3rd party ADO.NET connector objects to function.  These are dynamically loaded at runtime so that the SQL provider project is not dependent on them.  You must supply the location of the assemblies with the "ResolutionPath" static parameter.

SQLite is based on the .NET drivers found [here](http://system.data.sqlite.org/index.html/doc/trunk/www/downloads.wiki).  You will need the correct version for your specific architecture and setup.

PostgreSQL is based on the .NET drivers found [here](http://npgsql.projects.pgfoundry.org/).  The type provider will make frequent calls to the database.  I found that using the default settings for the PostgreSQL server on my Windows machine would deny the provider constant access - you may need to try setting  `Pooling=false` in the connection string, increasing timeouts or setting other relevant security settings to enable a frictionless experience.

MySQL is based on the .NET drivers found [here](http://dev.mysql.com/downloads/connector/net/1.0.html).  You will need the correct version for your specific architecture and setup.

Oracle is based on the current release (12.1.0.1.2) of the managed ODP.NET driver found [here](http://www.oracle.com/technetwork/topics/dotnet/downloads/index.html). However although the managed version is recommended it should also work with previous versions of the native driver.
## Documentation

Core documentation and samples can be found at the [SQLProvider home page](http://fsprojects.github.io/SQLProvider/). This documentation is generated from [docs/content/](https://github.com/fsprojects/SQLProvider/tree/master/docs/content), so please feel free to submit a pull request if you have fixed typos are added additional samples and documentation!

The provider currently supportsMS SQL Server, SQLite, PostgreSQL, Oracle and MySQL.  All database vendors except SQL Server will require 3rd party ADO.NET connector objects to function.  These are dynamically loaded at runtime so that the SQL provider project is not dependency on them.  You must supply the location of the assemblies with the "ResolutionPath" static parameter.

SQLite is based on the .NET drivers found [here]http://system.data.sqlite.org/index.html/doc/trunk/www/downloads.wiki.  You will need the correct version for your specific architecture and setup.

PostgreSQL is based on the .NET drivers found [here]http://npgsql.projects.pgfoundry.org/.  The type provider will make frequent calls to the database.  I found that using the default settings for the PostgreSQL server on my Windows machine would deny the provider constant access - you may need to increase timeouts or set other relevant security settings to enable a frictionless experience.

## Building

* Mono: Run *build.sh*  [![Mono build status](https://travis-ci.org/fsprojects/SQLProvider.png)](https://travis-ci.org/fsprojects/SQLProvider)
* Windows: Run *build.cmd* 

## Known issues

- Database vendors other than MS SQL Server use dynamic assembly loading.  This may cause some security problems depending on your system's configuration and which version of the .NET framework you are using.  If you encounter problems loading dynamic assemblies, they can likely be resolved by applying the following element into the configuration files of  fsi.exe, devenv.exe and your program or the program using your library : http://msdn.microsoft.com/en-us/library/dd409252(VS.100).aspx

## Disclaimer

This is an alpha build and as such may have problems yet undetected. This is not suitable for production use.  I am not responsible for any inadvertent damage and destruction caused by this software. 

Always fully assert the results of your queries!


