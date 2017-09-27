(**

# .NET Standard / .NET Core support #

This is very initial / experimental, from version 1.1.12-alpha5.

Install e.g. with: `dotnet add package SQLProvider --version 1.1.12-alpha5`

## Example project ##

There is an example .NET-Coreapp 2.0 project at: [tests\SqlProvider.Core.Tests](https://github.com/fsprojects/SQLProvider/tree/netstandard/tests/SqlProvider.Core.Tests)

There is a build.cmd which runs as follows:

```
dotnet restore
dotnet build
dotnet run
```

If you plan to run Microsoft SQL Server, you need a dependency to System.Data.SqlClient and a post-build task to
copy correct dll from under `System.Data.SqlClient\runtimes\...\` to your execution folder.


## Some Technical Details ##

Sources are in the [netstandard branch](https://github.com/fsprojects/SQLProvider/tree/netstandard).

The following files is needed to the NuGet package, from the .NET Standard SDK:
netstandard.dll, System.Console.dll, System.IO.dll, System.Reflection.dll, System.Runtime.dll

You can find net461 versions of them by default from:
C:\Program Files\dotnet\sdk\2.0.0\Microsoft\Microsoft.NET.Build.Extensions\net461\lib\

and also System.Data.SqlClient.dll from that NuGet package.

The NuGet cache is located at:
`C:\Users\(your-user-name)\.nuget\packages\SQLProvider\`


## Limitations ##

You need your database connection driver to also support .NET Core.
If your database connection driver has external dependencies, they have to be also present.

The providers that are not included in .NET-Standard version:

* Ms-Access in not supported
* Odbc is not supported

Connection string can be passed as hard-coded static parameter (development) or `GetDataContext(connectionstring)` but fetching it automatically from the application configuration is not supported.

.NET Standard solution is located at `/src/SQLProvider.Standard/SQLProvider.Standard.fsproj`

The target frameworks are defined in the project file: `<TargetFrameworks>net461;netcoreapp2.0;netstandard2.0</TargetFrameworks>`
Corresponding files goes to root bin paths, e.g.: \bin\netstandard2.0

You should use .netstandard2.0 and not netcoreapp2.0, because .netstandard2.0 can be referenced from the netcoreapp2.0. But if you want to still build as netcoreapp2.0, a modification is needed: ProvidedTypes.fs needs a type hint at line 94:

```fsharp
|> Seq.forall (fun (originalName:string, newName:string) ->
```

Build is not tested with Mono, so .Net Standard build is disabled from build.fsx on Mono.

*)

