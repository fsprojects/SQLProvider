module SoundCheck

open FSharp.Data.Sql
open NUnit.Framework

[<Literal>]
let connectionString = @"Data Source=.\libraries\northwindEF.db; Version = 3; Read Only=true; FailIfMissing=True;"

type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE, connectionString, ResolutionPath="">
    
[<Test>]
let ``Can do a simple select``() = 
    let dc = sql.GetDataContext()
    let query = 
        query {
            for cust in dc.Main.Customers do
            select cust.ContactName
        } |> Seq.toArray
    
    CollectionAssert.IsNotEmpty query
