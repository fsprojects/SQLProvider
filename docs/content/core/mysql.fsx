(*** hide ***)
#I @"../../files/sqlite"
(*** hide ***)
#I "../../../bin/net451"
(*** hide ***)
#r "FSharp.Data.SqlProvider.dll"
open FSharp.Data.Sql
open System

(**


# MySQL Provider

## Parameters

### ConnectionString

Basic connection string used to connect to MySQL instance; typical connection 
string parameters apply here.  See 
[MySQL Connector/NET Connection Strings Documentation](https://dev.mysql.com/doc/connector-net/en/connector-net-connection-options.html) 
for a complete list of connection string options.

*)
[<Literal>]
let connString  = "Server=localhost;Database=HR;User=root;Password=password"

(**

To deal with some MySQL data connection problems you might want to add some more parameters to connectionstring:
`Auto Enlist=false; Convert Zero Datetime=true;`

### ConnectionStringName

Instead of storing the connection string in the source code, you
can store it in the `App.config` file.  This is the name of the
connectionString key/value pair stored in App.config (TODO: confirm filename).
*)

// found in App.config (TODO: confirm)
let connexStringName = "DefaultConnectionString"

(**
### Database Vendor

Use `MYSQL` from the `FSharp.Data.Sql.Common.DatabaseProviderTypes` enumeration.

*)
[<Literal>]
let dbVendor    = Common.DatabaseProviderTypes.MYSQL

(**
### Resolution Path

Path to search for assemblies containing database vendor specific connections and custom types. Type the path where
`Mysql.Data.dll` is stored. Both absolute and relative paths are supported.

*)
[<Literal>]
let resPath = __SOURCE_DIRECTORY__ + @"/../../../packages/scripts/MySql.Data/lib/net45"

(**
### Individuals Amount

Sets the count of records to load for each table. See [individuals](individuals.html) for further info.

*)
[<Literal>]
let indivAmount = 1000

(**
### Use Option Types

If true, F# option types will be used in place of nullable database columns.  If false, you will always receive the default value of the column's type even if it is null in the database.

*)
[<Literal>]
let useOptTypes = true

(**
### Example

*)

type sql = SqlDataProvider<
                dbVendor,
                connString,
                ResolutionPath = resPath,
                IndividualsAmount = indivAmount,
                UseOptionTypes = useOptTypes,
                Owner = "HR"
            >
let ctx = sql.GetDataContext()

let employees = 
    ctx.Hr.Employees 
    |> Seq.map (fun e -> e.ColumnValues |> Seq.toList)
    |> Seq.toList

(**

## Working with Type-mappings

### Basic types

MySql.Data types are not always the ones you have used to in .NET, so here is a little help:

*)

let myEmp = 
    query {
        for jh in ctx.Hr.JobHistory do
        where (jh.Years > 10u)
        select (jh)
    } |> Seq.head

let myUint32 = 10u
let myInt64 = 10L
let myUInt64 = 10UL

(**

### System.Guid Serialization

If you use string column to save a Guid to database, you may want to skip the hyphens ("-")
when serializing them:

*)
let myGuid = System.Guid.NewGuid() //e.g. b8fa7880-ce44-4315-8d60-a160e5734c4b

let myGuidAsString = myGuid.ToString("N") // e.g. "b8fa7880ce4443158d60a160e5734c4b"

(**
The problem with this is that you should never forgot to use "N" in anywhere.

### System.DateTime Serialization

Another problem with MySql.Data is that DateTime conversions may fail if your culture is 
not the expected one.

So you may have to convert datetimes as strings instead of using just `myEmp.BirthDate <- DateTime.UtcNow`:
*)

myEmp.SetColumn("BirthDate", DateTime.UtcNow.ToString("yyyy-MM-dd HH\:mm\:ss") |> box)

(**

Notice that if you use `.ToString("s")` there will be "T" between date and time: "yyyy-MM-ddTHH\:mm\:ss".
And comparing two datetimes as strings with "T" and without "T" will generate a problem with the time-part.


If your DateTime columns are strings in the database, you can use `DateTime.Parse` in your where-queries:

```fsharp
let longAgo = DateTime.UtcNow.AddYears(-5)
let myEmp = 
    query {
        for emp in ctx.Hr.Employees do
        where (DateTime.Parse(emp.HireDate) > longAgo)
        select (emp)
    } |> Seq.head
```

You should be fine even with canonical functions like `DateTime.Parse(a.MeetStartTime).AddMinutes(10.)`.

## Caveats / Additional Info

Check [General](general.html), [Static Parameters](parameters.html) and [Querying](querying.html) documentation.


# Suppport for MySqlConnector

[MySqlConnector](https://github.com/mysql-net/MySqlConnector) is alternative driver to use instead of MySql.Data.dll.
It has less features but a lot better performance than the official driver.

You can use it with SQLProvider:
Just remove MySql.Data.dll from your resolutionPath and insert there MySqlConnector.dll instead. (Get the latest from NuGet.)
It uses references to System.Buffers.dll, System.Runtime.InteropServices.RuntimeInformation.dll and System.Threading.Tasks.Extensions.dll
so copy those files also to your referencePath. You can get them from corresponding NuGet packages.

If you want to use the drivers in parallel, you need two resolution paths:

*)

type HRFast = SqlDataProvider<Common.DatabaseProviderTypes.MYSQL, connString, ResolutionPath = @"c:\mysqlConnectorPath", Owner = "HR">
type HRProcs = SqlDataProvider<Common.DatabaseProviderTypes.MYSQL, connString, ResolutionPath = @"c:\MysqlDataPath", Owner = "HR">

(**

### Example performance difference from our unit tests

One complex query:

``` 
MySql.Data.dll:     Real: 00:00:00.583, CPU: 00:00:00.484, GC gen0: 1, gen1: 0, gen2: 0
MySqlConnector.dll: Real: 00:00:00.173, CPU: 00:00:00.093, GC gen0: 1, gen1: 0, gen2: 0
```

Lot of async queries:

```
MySQL.Data.dll      Real: 00:00:01.425, CPU: 00:00:02.078, GC gen0: 16, gen1: 1, gen2: 0
MySqlConnector.dll: Real: 00:00:01.091, CPU: 00:00:02.000, GC gen0: 14, gen1: 1, gen2: 0
```

*)
