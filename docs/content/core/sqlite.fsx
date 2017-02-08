(*** hide ***)
#I @"../../../bin"
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

Typically, this parameter is not needed. It specifies the path to the directory where 
`System.Data.SQLite.dll` is located. 
If you use the System.Data.SQLite NuGet package and target .NET 4.6, the path would be something like 
`__SOURCE_DIRECTORY__ + @"\..\packages\System.Data.SQLite.Core.<version>\lib\net46"`. 
Both absolute and relative paths are supported.

Note that `System.Data.SQLite.dll` loads a native interop library, which it looks for in the following
directories:

- on Windows: `SQLite.Interop.dll` in the `x64` and `x86` subdirectories of the resolution path.
- on Linux: `libSQLite.Interop.so` in the resolution path directory.
- alternatively in a directory specified in the "System.Data.SQLite.dll.config" file.

When adding the System.Data.SQLite NuGet package, the default "System.Data.SQLite.dll.config" 
file ensures that finding the native interop library "just works".

*)

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + @"/../../../tests/SqlProvider.Tests/libs"

(**
### SQLiteLibrary

Specifies what SQLite library to use. This is an `SQLiteLibrary` enumeration, defined in the `FSharp.Data.Sql.Common`
namespace, which has the following members:

- **`AutoSelect`** - Uses System.Data.SQLite under .NET and Mono.Data.SQLite under Mono. This is the default.
- **`MonoDataSQLite`** - Always uses Mono.Data.SQLite.
- **`SystemDataSQLite`** - Always uses System.Data.SQLite.


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
