# PostgreSQL - Getting started

You can either clone this repository and observe the more complex 
multi-environment version of
SqlProvider.Core.Tests.fsproj and Program.fs (and database scripts at /src/DatabaseScripts/PostgreSQL)
or you can start with these tutorials, for Windows, Linux and Mac:

## Part 1: Create project

```
dotnet new console --language f#
dotnet add package SQLProvider
dotnet add package Npgsql
dotnet add package System.Console
dotnet add package System.Data.Common
dotnet add package System.Runtime
dotnet add package System.Runtime.Extensions
dotnet add package System.Reflection
dotnet add package System.Reflection.TypeExtensions
dotnet add package System.Runtime.Serialization.Formatters
dotnet add package System.Threading.Tasks.Extensions
dotnet restore
code .
```

Create a new folder `libraries` under the root folder of your project.

## Part 2: Reference libraries and project file - On Windows

Locate the corresponding dll files of the packages
(Npgsql.dll, System.Threading.Tasks.Extensions.dll, System.Data.Common.dll)
from your NuGet cache `C:\Users\(user)\.nuget\packages\` (or `%USERPROFILE%\.nuget\packages\`)
the corresponding packages, versions `\lib\netstandard2.0`, 
or download those from NuGet.org under Manual Download, nuget-packages are just renamed zip-files.

Copy the dlls to the `libraries` folder.

If you use Paket package manager you could simply add a pre-build task for copying dlls, but let's skip it for now.

Add this to the .fsproj-file:

```xml
  <PropertyGroup>
    <FscToolPath>C:\Program Files (x86)\Microsoft SDKs\F#\4.1\Framework\v4.0</FscToolPath>
    <FscToolExe>fsc.exe</FscToolExe>
  </PropertyGroup>
```

## Part 2: Reference libraries and project file - On Linux

Locate the corresponding dll files of the packages
(Npgsql.dll, System.Threading.Tasks.Extensions.dll, System.Data.Common.dll)
from your NuGet cache `~/.nuget/packages/`, 
or download those from NuGet.org under Manual Download, nuget-packages are just renamed zip-files.

Copy the dlls to the `libraries` folder.

If you use Paket package manager you could simply add a pre-build task for copying dlls, but let's skip it for now.

Add this to the .fsproj-file:

```xml
  <PropertyGroup>
    <FscToolPath>/usr/bin</FscToolPath>
    <FscToolExe>fsharpc</FscToolExe>
  </PropertyGroup>
```

## Part 2: Reference libraries and project file - On Mac

Locate the corresponding dll files of the packages
(Npgsql.dll, System.Threading.Tasks.Extensions.dll, System.Data.Common.dll)
from your NuGet cache `~/.nuget/packages/`, 
or download those from NuGet.org under Manual Download, nuget-packages are just renamed zip-files.

Copy the dlls to the `libraries` folder.

If you use Paket package manager you could simply add a pre-build task for copying dlls, but let's skip it for now.

Add this to the .fsproj-file:

```xml
  <PropertyGroup>
    <FscToolPath>/Library/Frameworks/Mono.framework/Versions/Current/Commands</FscToolPath>
    <FscToolExe>fsharpc</FscToolExe>
  </PropertyGroup>
```

## Part 3: Source code, build and run

Replace content of Program.fs with this:

```fsharp
open System
open FSharp.Data.Sql

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + "/libraries"

[<Literal>]
let connStr = "User ID=postgres;Host=localhost;Port=5432;Database=sqlprovider;Password=postgres"

type HR = SqlDataProvider<
            Common.DatabaseProviderTypes.POSTGRESQL, connStr,
            ResolutionPath = resolutionPath, Owner="sqlprovider">

[<EntryPoint>]
let main argv =
    let ctx = HR.GetDataContext()
    let employeesFirstName = 
        query {
            for emp in ctx.Public.Employees do
            select emp.FirstName
        } |> Seq.head

    printfn "Hello %s!" employeesFirstName
    0 // return an integer exit code
```

Run:

```
dotnet build
dotnet run
```
