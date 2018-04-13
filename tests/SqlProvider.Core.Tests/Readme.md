
## .NET Core and .NET Standard - Getting started

Prerequisites:

 - FSharp and Dotnet installed. On Mac and Linux: Have also Mono installed.
 - Have a database, with a connection string
 - Have a code editor, e.g. Visual Studio Code and access to internet.
 - On Linux: Sudo permissons

## Select your database

The F# source-code will be basically the same for all the databases, 
but the initial configuration varies based on the database on operating system. 

You can either clone this repository and observe the more complex 
multi-environment version of SqlProvider.Core.Tests.fsproj, or
you can continue with Readme.md under each of the folders:

- Microsoft SQL Server - /MsSql
- MySQL / Maria DB - /MySql
- PostgreSQL - /Postgres
- SQLite - /SQLite

If you start with the tutorials, after these, there is a common optional step 4:
(In case you want to build to .NET Standard, or your build is not working.)

### Part 4: Run .NET Framwork on compile-time, .NET Standard on runtime

You need to compile the .NET dll with .NET Framework / Mono and then change the runtime to use the .NET Standard version.
That needs some manual changes to fsproj-project file.

First exclude the compile-time reference to .net-standard version, so change the PackageReference of SqlProvider to not copy the file automatically:

```xml
<PackageReference Include="SqlProvider" Version="1.1.38">
	<ExcludeAssets>compile</ExcludeAssets>
</PackageReference>
```

And add a prebuild-target to copy the dll to some compile-time cache directory.
This example is on Windows, but it's just a file-copy really:

```xml
<Target Name="PreBuild" BeforeTargets="PreBuildEvent">
    <Exec Command="xcopy %USERPROFILE%\.nuget\packages\SQLProvider\1.1.38\lib\net451\FSharp.Data.SqlProvider.dll compiletime\ /y" />
</Target>
```

Add a manual reference to SQLProvider net451:

```xml
<ItemGroup>
    <Reference Include="FSharp.Data.SqlProvider">
        <HintPath>compiletime/FSharp.Data.SqlProvider.dll</HintPath>
    </Reference>
</ItemGroup>
```

And a post-build target to replace the runtime-file back to .NET Standard version to your compilation path. Target path is based on your target framework:

```xml
<Target Name="PostBuild" AfterTargets="PostBuildEvent">
    <Exec Command="xcopy %USERPROFILE%\.nuget\packages\SQLProvider\1.1.38\lib\net451\FSharp.Data.SqlProvider.dll bin\Debug\netcoreapp2.0\ /y" />
</Target>
```

Compile and run again to check that everything is ok.