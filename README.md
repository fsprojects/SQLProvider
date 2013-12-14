# SqlProvider

A general SQL database type provider, supporting LINQ queries, schema exploration, individuals and much more besides.

More documentation to come, for now you will have to settle with the post at my website [here](http://pinksquirrellabs.com/post/2013/12/09/The-Erasing-SQL-type-provider.aspx)

## Building

Open solution file /src/SqlProvider.sln 

Build.

## Known issues

- You are only able to use the SQL provider to access one database in your project. This is because the data context has some static state to enable the communication between the provided types and the runtime. A future release should remove this constraint. 

- Database vendors other than MS SQL Server use dynamic assembly loading.  This may cause some security problems depending on your system's configuration and which version of the .NET framework you are using.  If you encounter problems loading dynamic assemblies, they can likely be resolved by applying the following element into the configuration files of  fsi.exe, devenv.exe and your program or the program using your library : http://msdn.microsoft.com/en-us/library/dd409252(VS.100).aspx

## Disclaimer

This is an alpha build and as such may have problems yet undetected. This is not suitable for production use.  I am not responsible for any inadvertent damage and destruction caused by this software. 

Always fully assert the results of your queries!


