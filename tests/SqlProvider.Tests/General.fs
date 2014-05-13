module SoundCheck

open NUnit.Framework

open FSharp.Data.Sql
[<Literal>]
let connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"\northwindEF.db;Version=3"

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ 

type sql = SqlDataProvider<connectionString, Common.DatabaseProviderTypes.SQLITE, resolutionPath >

[<Test>]
let Test1() = Assert.IsTrue true


[<Test>]
let DbCheck() = 
    let dc = sql.GetDataContext()
    Assert.IsTrue true
