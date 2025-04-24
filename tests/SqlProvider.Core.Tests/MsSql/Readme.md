There are two ways of using SQLProvider: Either via dynamic package SQLProvider or with pre-defined dependency SQLProvider.MsSql 
The dynamic package needs more configuration but has more flexibility if you want manual control over all reference dlls.

# Using SQLProvider.MsSql 

The SQLProvider.MsSql can be used with direct connection or with reading the schema from SSDT dacpac file.
The later is good option if you expect to need longer maintenance project, you can create .sqlproj and save your DB schema to your version control.
SQLProvider.MsSql uses Microsoft.Data.SqlClient to connect to the database.

## Part 1: Create project

```
dotnet new console --language f#
dotnet add package SQLProvider.MsSql
dotnet add package Microsoft.Data.SqlClient --version 5.2.2
``` 

## Part 2: Source code, build and run

```fsharp
open System
open FSharp.Data.Sql
open FSharp.Data.Sql.MsSql

[<Literal>]
let connStr = "Data Source=localhost; Initial Catalog=HR; Integrated Security=True;TrustServerCertificate=true"

type HR = FSharp.Data.Sql.MsSql.SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER, connStr>

[<EntryPoint>]
let main argv =
    let ctx = HR.GetDataContext()
    let employeesFirstName = 
        query {
            for emp in ctx.Dbo.Employees do
            select emp.FirstName
        } |> Seq.head

    printfn "Hello %s!" employeesFirstName
    0 // return an integer exit code
```

Run:
Build with VS2022 and run.

```
dotnet build
dotnet run
```

Now, add .sqlproj to your project, change `type HR = FSharp.Data.Sql.MsSql.SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER, connStr>` to
`type HR = FSharp.Data.Sql.MsSql.SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER_SSDT, connStr, SsdtPath= @"c:\mydatabase.dacpac">`

Happy development!


# Using the dynamic SQLProvider

This is the other, more complex way.

You can either clone this repository and observe the more complex 
multi-environment version of
SqlProvider.Core.Tests.fsproj and Program.fs (and database scripts at /src/DatabaseScripts/MSSQLServer)
or you can start with these tutorials, for Windows, Linux and Mac:

## Part 1: Create project

```
dotnet new console --language f#
dotnet add package SQLProvider
dotnet add package System.Data.SqlClient
dotnet restore
code .
```

## Part 2: Project file - On Windows

Add this to the .fsproj-file:

```xml
  <PropertyGroup>
    <FscToolPath>C:\Program Files (x86)\Microsoft SDKs\F#\4.1\Framework\v4.0</FscToolPath>
    <FscToolExe>fsc.exe</FscToolExe>
  </PropertyGroup>
```

## Part 2: Project file - On Linux

Add this to the .fsproj-file:

```xml
  <PropertyGroup>
    <FscToolPath>/usr/bin</FscToolPath>
    <FscToolExe>fsharpc</FscToolExe>
  </PropertyGroup>
```

## Part 2: Project file - On Mac

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
let connStr = "Data Source=localhost; Initial Catalog=HR; Integrated Security=True"

type HR = SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER, connStr>

[<EntryPoint>]
let main argv =
    let ctx = HR.GetDataContext()
    let employeesFirstName = 
        query {
            for emp in ctx.Dbo.Employees do
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

## Using Microsoft.Data.SqlClient.dll instead of build-in System.Data.SqlClient.dll
To use another driver, Microsoft.Data.SqlClient.dll instead of build-in System.Data.SqlClient.dll, you have to set your provider to `Common.DatabaseProviderTypes.MSSQLSERVER_DYNAMIC` and copy the reference files
from the NuGet package to local resolutionPath (e.g. Microsoft.Data.SqlClient.dll, Microsoft.Data.SqlClient.SNI.dll and Microsoft.Data.SqlClient.SNI.x86.dll).
