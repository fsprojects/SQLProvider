open System
open FSharp.Data
open FSharp.Data.Sql
open FSharp.Data.Sql.MsAccess

[<Literal>]
let connectionString = @"Provider=Microsoft.ACE.OLEDB.12.0; Data Source= " + __SOURCE_DIRECTORY__ + @"\..\..\..\docs\files\msaccess\Northwind.mdb"

type TypeProviderConnection =
    SqlDataProvider< 
        ConnectionString = connectionString,
        DatabaseVendor = Common.DatabaseProviderTypes.MSACCESS,
        IndividualsAmount=100,
        UseOptionTypes=FSharp.Data.Sql.Common.NullableColumnType.VALUE_OPTION>

let ctx = TypeProviderConnection.GetDataContext()


let qry =
    query {
        for c in ctx.Northwind.Customers do
        where (c.ContactName.IsSome)
        select c.ContactName.Value
    } |> Seq.head

printf "%s" qry
//Console.ReadLine()

