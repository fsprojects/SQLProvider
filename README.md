# SqlProvider

A general SQL database type provider, supporting LINQ queries, schema exploration, individuals and much more besides.

More documentation to come, for now you will have to settle with the post at my website [here](http://pinksquirrellabs.com/post/2013/12/09/The-Erasing-SQL-type-provider.aspx)

The provider currently supports MS SQL Server, SQLite and PostgreSQL.  All database vendors except SQL Server will require 3rd party ADO.NET connector objects to function.  These are dynamically loaded at runtime so that the SQL provider project is not dependency on them.  You must supply the location of the assemblies with the "ResolutionPath" static parameter.

SQLite is based on the .NET drivers found [here]http://system.data.sqlite.org/index.html/doc/trunk/www/downloads.wiki.  You will need the correct version for your specific architecture and setup.

PostgreSQL is based on the .NET drivers found [here]http://npgsql.projects.pgfoundry.org/.  The type provider will make frequent calls to the database.  I found that using the default settings for the PostgreSQL server on my Windows machine would deny the provider constant access - you may need to increase timeouts or set other relevant security settings to enable a frictionless experience.

## Building

run build.cmd

## Known issues

- Database vendors other than MS SQL Server use dynamic assembly loading.  This may cause some security problems depending on your system's configuration and which version of the .NET framework you are using.  If you encounter problems loading dynamic assemblies, they can likely be resolved by applying the following element into the configuration files of  fsi.exe, devenv.exe and your program or the program using your library : http://msdn.microsoft.com/en-us/library/dd409252(VS.100).aspx

## Disclaimer

This is an alpha build and as such may have problems yet undetected. This is not suitable for production use.  I am not responsible for any inadvertent damage and destruction caused by this software. 

Always fully assert the results of your queries!


