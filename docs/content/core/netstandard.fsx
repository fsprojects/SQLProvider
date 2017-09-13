(**

# .NET Standard / .NET Core support #

There are some limitations in .NET-standard version.

You need your database connection driver to also support .NET Core.
If your database connection driver has external dependencies, they has to be also present.

The providers that are not included in .NET Core version:

* Ms-Access in not supported
* Odbc is not supported

Connection string can be passed as hard-coded static parameter (development) or `GetDataContext(connectionstring)` but fetching it automatically from the application configuration is not supported.

You can develop with the original version and then just replace the dll with the core version.

*)
