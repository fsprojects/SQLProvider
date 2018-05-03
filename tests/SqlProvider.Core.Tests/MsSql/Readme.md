# Ms SQL Server - Getting started

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
