// This is with classic FSI:
#I @"../../../bin/net451"
//#r @"../../../bin/net451/FSharp.Data.SqlProvider.dll"
#r @"../../../src/SQLProvider/obj/Debug/FSharp.Data.SqlProvider.dll"
// This is with dotnet.exe fsi:
//#I @"../../../bin/netstandard2.0"
//#r @"../../../bin/netstandard2.0/FSharp.Data.SqlProvider.dll"

open FSharp.Data.Sql
open FSharp.Data.Sql.Common

//[<Literal>]
//let resolutionFolder = __SOURCE_DIRECTORY__ + @"/../../../packages/Microsoft.SqlServer.Management.SqlParser/lib/net462"

[<Literal>]
let resolutionFolder = __SOURCE_DIRECTORY__ + @"/../../../packages/Microsoft.SqlServer.Management.SqlParser/lib/netstandard2.0"

[<Literal>]
let path = @"C:\_mdsk\CEI.BimHub\Product\Source\CEI.BimHub.DB\dbo"

type DB = SqlDataProvider<
            Common.DatabaseProviderTypes.MSSQLSERVER_SSDT,
            ResolutionPath = resolutionFolder,
            SsdtPath = path>


//DB.GetDataContext()

let getSheets(ctx: DB.dataContext) =
    query {
        for p in ctx.Dbo.Projects do
        select p
    }
