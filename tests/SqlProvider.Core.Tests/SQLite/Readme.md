# SQLite - Getting started

You can either clone this repository and observe the more complex 
multi-environment version of
SqlProvider.Core.Tests.fsproj and Program.fs (and database file at /tests/SqlProvider.Tests/db/northwindEF.db)
or you can start with this simple tutorial, for Windows.

## Part 1: Create project

```
dotnet new console --language f#
dotnet add package SQLProvider
dotnet add package SQLitePCLRaw.bundle_green
dotnet add package System.Data.SqlClient
dotnet add package System.Runtime
dotnet add package System.Runtime.Extensions
dotnet add package System.Reflection
dotnet add package System.Reflection.TypeExtensions
dotnet add package System.Runtime.Serialization.Formatters
dotnet restore
code .
```

Create a new folder `libraries` under the root folder of your project.

## Part 2: Reference libraries and project file - On Windows

From your NuGet cache `C:\Users\(user)\.nuget\packages\` (or `%USERPROFILE%\.nuget\packages\`) versions `lib\netstandard2.0`
locate the following dll files:

 - e_sqlite3.dll - package sqlitepclraw.lib.e_sqlite3.v110_xp, under runtimes-folder (the physical architecture has to be correct), 
 - Microsoft.Data.Sqlite.dll - package microsoft.data.sqlite.core under lib\netstandard2.0
 - SQLitePCLRaw.batteries_green.dll and SQLitePCLRaw.batteries_v2.dll - package sqlitepclraw.bundle_green under lib\netstandard1.1
 - SQLitePCLRaw.core.dll - package sqlitepclraw.core under lib\netstandard1.1
 - SQLitePCLRaw.provider.e_sqlite3.dll - package SQLitePCLRaw.provider.e_sqlite3.netstandard11 under lib\netstandard1.1
 
(or download those from NuGet.org under Manual Download, nuget-packages are just renamed zip-files).
Copy the dlls to the `libraries` folder.

If you use Paket package manager you could simply add a pre-build task for dlls, but let's skip it for now.

Add this to the .fsproj-file:

```xml
  <PropertyGroup>
    <FscToolPath>C:\Program Files (x86)\Microsoft SDKs\F#\4.1\Framework\v4.0</FscToolPath>
    <FscToolExe>fsc.exe</FscToolExe>
  </PropertyGroup>
```

## Part 3: Source code, build and run

Replace content of Program.fs with this:

```fsharp
open System
open FSharp.Data.Sql

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + "/libraries"

[<Literal>] // Relative to resolutionPath:
let connStr = @"Filename=" + @"./../myDatabaseFile.db"

// In case of dependency error, note that SQLite dependencies are processor architecture dependant
type HR = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connStr, ResolutionPath = resolutionPath, SQLiteLibrary=Common.SQLiteLibrary.MicrosoftDataSqlite>

[<EntryPoint>]
let main argv =
    let ctx = HR.GetDataContext()
    let employeesFirstName = 
        query {
            for emp in ctx.Main.Employees do
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

Although the .NET Core version should run on other platforms,
there seems not to be a simple version to compile SQLite version
on other platforms.
