(**

# .NET Standard / .NET Core support #

This is very initial / experimental.

## Example project ##

There is an example .NET-Coreapp 2.0 project at: `tests\SqlProvider.Core.Tests`

It is build and run as follows:

```
dotnet restore
dotnet build
dotnet run
```

## Details ##

The following files is needed to the NuGet package:
netstandard.dll System.Reflection.dll System.Runtime.dll System.Data.SqlClient.dll

Those files are by default the net461 versions, gotten from paths:

```
"C:\Program Files\dotnet\sdk\2.0.0\Microsoft\Microsoft.NET.Build.Extensions\net461\lib\netstandard.dll"
"C:\Program Files\dotnet\sdk\2.0.0\Microsoft\Microsoft.NET.Build.Extensions\net461\lib\System.Reflection.dll"
"C:\Program Files\dotnet\sdk\2.0.0\Microsoft\Microsoft.NET.Build.Extensions\net461\lib\System.Runtime.dll"
"..\..\packages\System.Data.SqlClient\lib\net461\System.Data.SqlClient.dll"
```

The NuGet cache is located at:
`C:\Users\(your-user-name)\.nuget\packages\SQLProvider\`

There is an example .NET-Coreapp 2.0 project at: `tests\SqlProvider.Core.Tests`


## Limitations ##

You need your database connection driver to also support .NET Core.
If your database connection driver has external dependencies, they has to be also present.

The providers that are not included in .NET-Standard version:

* Ms-Access in not supported
* Odbc is not supported

Connection string can be passed as hard-coded static parameter (development) or `GetDataContext(connectionstring)` but fetching it automatically from the application configuration is not supported.

.NET Standard solution is located at `/src/SQLProvider.Standard/SQLProvider.Standard.fsproj`

The target frameworks are defined in the project file: `<TargetFrameworks>net461;netcoreapp2.0;netstandard2.0</TargetFrameworks>`
Corresponding files goes to root bin paths, e.g.: \bin\netstandard2.0

You should use .netstandard2.0 and not netcoreapp2.0. But if you want to still build  netcoreapp2.0, a modification is needed: ProvidedTypes.fs needs a type hint at line 94:

```fsharp
|> Seq.forall (fun (originalName:string, newName:string) ->
```

Build is not tested with Mono, so .Net Standard build is disabled from build.fsx on Mono.

*)

