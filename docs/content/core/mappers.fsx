(*** hide ***)
#I "../../files/sqlite"
(*** hide ***)
#I "../../../bin"
(*** hide ***)
#r @"../../../bin/FSharp.Data.SqlProvider.dll"

(*** hide ***)
[<Literal>]
let connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"/../../../tests/SqlProvider.Tests/scripts/northwindEF.db;Version=3"

(*** hide ***)
[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + @"/../../files/sqlite"

(**
## Adding a Mapper using dataContext to use generated types from db

This mapper will get sure that you always sync your types with types you receive from your db.

### First add an Domain Model

*)

type Employee = {
    EmployeeId : int32
    FirstName : string
    LastName : string
    HireDate : DateTime
}

(**
### Then you can create the mapper using dataContext to use generated types from db
TODO: Find the right Entity
*)

let mapEmployee (dbRecord:ctx.Dbo.``[Main].[Employees]Entity``) : Employee =
    { EmployeeId = dbRecord.EmployeeId
      FirstName = dbRecord.FirstName
      LastName = dbRecord.LastName
      HireDate = dbRecord.HireDate }
