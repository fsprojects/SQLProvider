
#r @"D:\Gibran\Projetos\GitRep\SQLProvider\bin\FSharp.Data.SqlProvider.dll"
open FSharp.Data.Sql
open FSharp.Data.Sql.Providers

let [<Literal>] resolutionPath = "D:/Gibran/Projetos/FirebirdNetProvider141216/src/FirebirdSql.Data.FirebirdClient/bin/Debug/NET40" 
let [<Literal>] connectionString = @"character set=NONE;data source=localhost;initial catalog=d:\Tisul\Gestao\Dados\ROMENA.FDB;user id=SYSDBA;password=masterkey;dialect=1"
// create a type alias with the connection string and database vendor settings

              




type sql = SqlDataProvider< 
              ConnectionString = connectionString,
              DatabaseVendor = Common.DatabaseProviderTypes.FIREBIRD,
              ResolutionPath = resolutionPath,
              IndividualsAmount = 1000,
              UseOptionTypes = true >

let ctx = sql.GetDataContext()

FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %s")

let ni = 
    query
        {
        for i in ctx.Dbo.Notafiscal do
        for v in i.``Dbo.VENDEDOR by VENDICOD`` do 
        where (i.Nofia13id = "0010390164336")
        select v.Venda60nome
        } |> Seq.toArray

