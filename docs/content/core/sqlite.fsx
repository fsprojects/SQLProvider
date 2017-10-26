(*** hide ***)
#I @"../../../bin/net451"
#r "FSharp.Data.SqlProvider.dll"
open System
open FSharp.Data.Sql
(**

# SQLite Provider

## Parameters

### ConnectionString

Basic connection string used to connect to the SQLite database.

*)

[<Literal>]
let connectionString = 
    "Data Source=" + 
    __SOURCE_DIRECTORY__ + @"/../../../tests/SqlProvider.Tests/scripts/northwindEF.db;" + 
    "Version=3;foreign keys=true"

(**
### ResolutionPath

Path to search for database vendor speficic assemblies. Specify the path where `System.Data.SQLite.dll` is stored. 
If you use the System.Data.SQLite NuGet package and target .NET 4.6, the path would be something like 
`__SOURCE_DIRECTORY__ + @"\..\packages\System.Data.SQLite.Core.<version>\lib\net46"`. 
Both absolute and relative paths are supported.

Note that `System.Data.SQLite.dll` will look for the native interop library:

- on Windows: `SQLite.Interop.dll` in the `x64` and `x86` subdirectories of the resolution path.
- on Linux: `libSQLite.Interop.so` in the resolution path directory.

The interop libraries are not properly placed afer the System.Data.SQLite NuGet package is added, so you might have to 
manually copy the interop libraries:

- on Windows: copy `x64` and `x86` subdirectories from SQLite build directory, which typically is 
 `<project root>\packages\System.Data.SQLite.Core.<version>\build\net46`.
- on Linux: first build the `libSQLite.Interop.so` using `<srcDir>/Setup/compile-interop-assembly-release.sh` script from [System.Data.SQLite source distribution](https://system.data.sqlite.org/index.html/doc/trunk/www/downloads.wiki) `sqlite-netFx-source-1.x.xxx.x.zip`. And then copy it from `<srcDir>/bin/2013/Release/bin/`.

If `System.Data.SQLite.dll` is in the location where NuGet places it by default, you don't have to submit
the ResolutionPath parameter at all, but you still need to copy the interop libraries as described above.

If you use Microsoft.Data.Sqlite driver, you still need the physical dll, (in that case e_sqlite3.dll), to be in resolutionPath or folder under operating system PATH variable.

*)

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + @"/../../../tests/SqlProvider.Tests/libs"

(**
### SQLiteLibrary

Specifies what SQLite library to use. This is an `SQLiteLibrary` enumeration, defined in the `FSharp.Data.Sql.Common`
namespace, which has the following members:

- **`AutoSelect`** - Uses System.Data.SQLite under .NET, Mono.Data.SQLite under Mono and Microsoft.Data.Sqlite under NET Core. This is the default.
- **`MonoDataSQLite`** - Always uses Mono.Data.SQLite.
- **`SystemDataSQLite`** - Always uses [System.Data.SQLite](https://system.data.sqlite.org/).
- **`MicrosoftDataSqlite`** - Always uses [Microsoft.Data.Sqlite](https://github.com/aspnet/Microsoft.Data.Sqlite). This is experimental until it supports GetSchema().


## Example

*)
type sql = SqlDataProvider<
                Common.DatabaseProviderTypes.SQLITE, 
                SQLiteLibrary = Common.SQLiteLibrary.SystemDataSQLite,
                ConnectionString = connectionString, 
                ResolutionPath = resolutionPath, 
                CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL>

let ctx = sql.GetDataContext()

let customers = 
    ctx.Main.Customers
    |> Seq.map(fun c -> c.ContactName)
    |> Seq.toList

(**

# CRUD

When you do insert operation, after .SubmitUpdates call you can get inserted rowid like this:

*)

let myCustomer = ctx.Main.Customers.``Create(CompanyName)``("MyCompany")
ctx.SubmitUpdates()
let rowid = myCustomer.GetColumn("rowid") : int
