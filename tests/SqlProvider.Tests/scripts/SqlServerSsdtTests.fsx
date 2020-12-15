// This is with classic FSI:
#I @"../../../bin/net451"
//#r @"../../../bin/net451/FSharp.Data.SqlProvider.dll"
#r @"../../../src/SQLProvider/obj/Debug/FSharp.Data.SqlProvider.dll"
// This is with dotnet.exe fsi:
//#I @"../../../bin/netstandard2.0"
//#r @"../../../bin/netstandard2.0/FSharp.Data.SqlProvider.dll"

open FSharp.Data.Sql

[<Literal>]
let resolutionFolder = __SOURCE_DIRECTORY__ + @"/../../../packages/Microsoft.SqlServer.Management.SqlParser/lib/netstandard2.0"

[<Literal>]
let ssdtPath = __SOURCE_DIRECTORY__ +  @"/SSDT Project/dbo"


type DB = SqlDataProvider<
            Common.DatabaseProviderTypes.MSSQLSERVER_SSDT,
            ResolutionPath = resolutionFolder,
            SsdtPath = ssdtPath>

let getProjects(ctx: DB.dataContext) =
    query {
        for p in ctx.Dbo.Projects do
        for t in p.``dbo.ProjectTasks by Id`` do
        for c in t.``dbo.ProjectTaskCategories by Id`` do
        where (p.IsActive && not p.IsDeleted)
        select
            {| Id = p.Id
               Name = p.Name
               Number = p.ProjectNumber
               Type = p.ProjectType |}
    }
