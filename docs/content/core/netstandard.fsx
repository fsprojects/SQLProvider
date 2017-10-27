(**

# .NET Standard / .NET Core support

This is very initial / experimental, from version 1.1.18.

Install e.g. with: `dotnet add package SQLProvider --version 1.1.18`



You will need some configuration to your project files, see the examples. 
 - You need a path to fsc.
 - You need your database driver and dependency dlls to resolutionPath-folder. 

```
Core / NetStandard:
                     | Win10 | Mac  | Ubuntu (needs sudo) | Comments   | 
Microsoft SQL Server | [x]   | [x]  | [x]        | See prebuild task. |
Postgres             | [x]   | [x]  | [x]        | See post-build task. |
MySQL / MariaDB      | [x]   | [x]  | [x]        | See post-build task. |
SQLite               | [x]   |      |            | See post-build task. Builds on Win, runs on all |
FireBird             |  ?    |  ?   |  ?         | Not tested. |
Oracle               |       |      |            | No ODP.NET-driver yet. |
Odbc                 |       |      |            | Not supported. |
Access               |       |      |            | Not supported. |

.NET 4.5.1 / Mono: All should work.
```
 
## Example projects

There are some example .NET-Coreapp 2.0 projects at: [tests\SqlProvider.Core.Tests](https://github.com/fsprojects/SQLProvider/tree/master/tests/SqlProvider.Core.Tests)

There is a build.cmd which runs as follows:

```
dotnet restore
dotnet build
dotnet run
```

## Limitations

You need your database connection driver to also support .NET Core.
If your database connection driver has external dependencies, they have to be also present
(e.g. a project prebuild-task to move them to resolution path).

Connection string can be passed as hard-coded static parameter (development) or `GetDataContext(connectionstring)` parameter on runtime, but fetching it automatically from the application configuration is not supported.

The target frameworks are defined in the project file: `<TargetFrameworks>net461;netcoreapp2.0;netstandard2.0</TargetFrameworks>`
Corresponding files goes to root bin paths, e.g.: \bin\netstandard2.0

#### Microsoft Sql Server

If you plan to run Microsoft SQL Server, you need a dependency to System.Data.SqlClient and a post-build task to
copy correct dll from under `System.Data.SqlClient\runtimes\...\` to your execution folder.

#### MySql

MySQL is using MySQLConnector.
Alternative is to use provided custom build of MySQL.Data.dll (6.10-rc) compiled from sources to .Net Standard 2.0.
As the official MySQL.Data.dll does not support .NET Standard 2.0. See: https://bugs.mysql.com/bug.php?id=88016

#### SQLite

SQLite is using Microsoft.Data.Sqlite library which has dependency to non-managed e_sqlite3.dll.
Ensure that the correct platform specific file is found either from referencePath or PATH environment variable.

Due to lack of Mono-support in Microsoft.Data.SQLite and Core-support in System.Data.SQLite, there is no
common driver that could work on both environments. That's why build is not possible with Mac/Ubuntu.
Microsoft.Data.Sqlite.Core.Backport could maybe work, not tested.

#### Non-Windows environment (Mac / Ubuntu / Linux / ...)

On Windows you can reference .NET Standard 2.0 dlls in FSharp interactive but in Osx or Linux you cannot as Mono is not .NET Standard 2.0 compatible. 

Non-Windows typeprovider will call Mono on compile-time. Mono will use .Net 4.5.1 libraries. 
Typeprovider handles that on compilation, and after compile your assembly will ve .NET Core compatible.
But your compilation resolutionPath reference assemblies have to be .Net 4.5.1 version and your build folder
have to have core references, e.g. via PackageReferences.

On Ubuntu you probably have to use sudo for both dotnet restore and dotnet build.
If you forgot the sudo, try to remove bin and obj folders and then run restore again.

## Some Technical Details

.NET Standard solution is located at `/src/SQLProvider.Standard/SQLProvider.Standard.fsproj`

The following files is needed to the NuGet package, from the .NET Standard SDK:
netstandard.dll, System.Console.dll, System.IO.dll, System.Reflection.dll, System.Runtime.dll

You can find net461 versions of them by default from (e.g. 2.0.0, can be also something else like 2.0.2):
Win: `C:\Program Files\dotnet\sdk\2.0.0\Microsoft\Microsoft.NET.Build.Extensions\net461\lib\`
Others: `/usr/local/share/dotnet/sdk/2.0.0/Microsoft/Microsoft.NET.Build.Extensions/net461/lib`

and also System.Data.SqlClient.dll from that NuGet package.

The NuGet cache is located at:
Win: `C:\Users\(your-user-name)\.nuget\packages\SQLProvider\`
Others: `~/.nuget/packages/SQLProvider/`
*)

