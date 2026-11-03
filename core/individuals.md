# Individuals

Find individual rows in tables with code completion in the editor.

```fsharp
open System
open FSharp.Data.Sql

type sql  = SqlDataProvider<
                Common.DatabaseProviderTypes.SQLITE,
                connectionString,
                SQLiteLibrary=Common.SQLiteLibrary.SystemDataSQLite,
                ResolutionPath = resolutionPath,
                CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL
            >

let ctx = sql.GetDataContext()

let customers = ctx.Main.Customers
```

Get individual customer row by primary key value

```fsharp
customers.Individuals.COMMI
```

Get individual customer row using address

```fsharp
customers.Individuals.``As ContactName``.``COMMI, Pedro Afonso``
```

Get individual customer row using address

```fsharp
customers.Individuals.``As Address``.``CONSH, Berkeley Gardens 12  Brewery``
```
