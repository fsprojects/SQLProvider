
#r @"D:\Gibran\Projetos\GitRep\SQLProvider\bin\net451\FSharp.Data.SqlProvider.dll"
open FSharp.Data.Sql
open FSharp.Data.Sql.Providers


let [<Literal>] resolutionPath = "D:/Gibran/Projetos/GitRep/SQLProvider/packages/scripts/FirebirdSql.Data.FirebirdClient/lib/net452" //"D:/Gibran/Projetos/FirebirdNetProvider141216/src/FirebirdSql.Data.FirebirdClient/bin/Debug/NET40" 
let [<Literal>] connectionString = @"character set=NONE;data source=localhost;port=3051;initial catalog=d:\Tisul\Gestao\Dados\ROMENA.FDB;user id=SYSDBA;password=masterkey;dialect=1"
// create a type alias with the connection string and database vendor settings

              




type sql = SqlDataProvider< 
              ConnectionString = connectionString,
              DatabaseVendor = Common.DatabaseProviderTypes.FIREBIRD,
              ResolutionPath = resolutionPath,
              IndividualsAmount = 1000,
              UseOptionTypes = true >

let ctx = sql.GetDataContext()

FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %O")

//let nfi1 = query { for ni in ctx.Dbo.Notafiscalitem do take 1; select ni} |> Seq.head
//nfi1.Cfopa5cod <- Some "9999"
//nfi1.Nfitiitem <- 1001
//ctx.ClearUpdates()
//let nfiN = ctx.Dbo.Notafiscalitem.Create nfi1.ColumnValues
//ctx.SubmitUpdates()

let ni = 
    query
        {
        for i in ctx.Dbo.Notafiscal do
        //join v in ctx.Dbo.Vendedor on (i.Vendicod = Some v.Vendicod)  
        for v in i.``Dbo.VENDEDOR by VENDICOD`` do 
        //where (i.Nofia13id = "0010390164336")
        //skip 2
        take 5
        select v.Venda60nome  //i.Nofia13id
        } |> Seq.toArray

