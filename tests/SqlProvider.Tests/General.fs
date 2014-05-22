module SoundCheck

open NUnit.Framework

open FSharp.Data.Sql

[<Literal>]
let connectionString = @"Data Source=northwindEF.db;Version=3"

[<Literal>]
let resolutionPath = "../../docs/files/sqlite/"

type sql = SqlDataProvider<connectionString, Common.DatabaseProviderTypes.SQLITE, resolutionPath >

[<Test>]
let Test1() = Assert.IsTrue true


[<Test>]
let DbCheck() = 
    let dc = sql.GetDataContext()
    let customers = dc.``[main].[Customers]``
    Assert.IsTrue true