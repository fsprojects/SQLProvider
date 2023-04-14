module MsSqlSsdt.Tests.UserDefinedDataTypesTests
open FSharp.Data.Sql
open FSharp.Data.Sql.Schema

[<Literal>]
let AdventureWorks = @"Server=localhost\SQLEXPRESS;Database=AdventureWorksLT2019;Trusted_Connection=True;"

[<Literal>]
let SsdtPath = __SOURCE_DIRECTORY__ + "/AdventureWorks_SSDT/bin/Debug/AdventureWorks_SSDT.dacpac"

type DB = SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER_SSDT, SsdtPath = SsdtPath>

let ctx = DB.GetDataContext(AdventureWorks)

let ``Flag UDDT should be equals to a System.Boolean`` =
    let entity = ctx.SalesLt.SalesOrderHeader.Create()
    if (entity.OnlineOrderFlag.GetType().FullName) <> "System.Boolean"
    then failwith "Failed test condition for user defined data type: Flag UDDT should be equals to a System.Boolean."
