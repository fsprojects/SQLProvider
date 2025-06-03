(*** hide ***)
#r "../../../bin/lib/netstandard2.0/FSharp.Data.SqlProvider.Common.dll"
#r "../../../bin/lib/netstandard2.0/FSharp.Data.SqlProvider.dll"
(*** hide ***)
let [<Literal>] resolutionPath = __SOURCE_DIRECTORY__ + @"/../../files/sqlite"
(*** hide ***)
let [<Literal>] connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"\..\northwindEF.db;Version=3;Read Only=false;FailIfMissing=True;"

(*** hide ***)
open FSharp.Data.Sql

(*** hide ***)
type sql  = SqlDataProvider<
                Common.DatabaseProviderTypes.SQLITE,
                connectionString,
                SQLiteLibrary=Common.SQLiteLibrary.SystemDataSQLite,
                ResolutionPath = resolutionPath,
                CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL
            >

(**

## Adding a Mapper using dataContext to use generated types from the DB

Typically, F# is about writing business logic and not about OR-mapping. Consider using your database types as is. And select only the columns you need, not full entities. But sometimes you want to
map objects to different ones, for example to interact with other languages like C# domain.

First, add a Domain Model

*)

open System

type Employee = {
    EmployeeId : int64
    FirstName : string
    LastName : string
    HireDate : DateTime
}

(**
Then you can create the mapper using dataContext to use the generated types from the DB
*)

let mapEmployee (dbRecord:sql.dataContext.``main.EmployeesEntity``) : Employee =
    { EmployeeId = dbRecord.EmployeeId
      FirstName = dbRecord.FirstName
      LastName = dbRecord.LastName
      HireDate = dbRecord.HireDate }

(**

### TemplateAsRecord

If you want to copy and paste the current SQLProvider objects as separate classes, you can use `TemplateAsRecord` property of your database table:

*)

ctx.Main.Employees.TemplateAsRecord
// intellisense will generate you code that you can copy and paste as template to create your own type:
// ``type MainOrders = { CustomerId : String voption; EmployeeId : Int64 voption; Freight : Decimal voption; OrderDate : DateTime voption; OrderId : Int64; RequiredDate : DateTime voption; ShipAddress : String voption; ShipCity : String voption; ShipCountry : String voption; ShipName : String voption; ShipPostalCode : String voption; ShipRegion : String voption; ShippedDate : DateTime voption }``

(**

This could be useful if you e.g. want to use SQLProvider objects in some reflection based code-generator (because the normal objects are erased).


### MapTo

SqlProvider also has a `.MapTo<'T>` convenience method:
*)



let ctx = sql.GetDataContext()

let orders = ctx.Main.Orders
let employees = ctx.Main.Employees

type Employee2 = {
    FirstName:string
    LastName:string
    }

let qry = query { for row in employees do
                  select row} |> Seq.map (fun x -> x.MapTo<Employee2>())

(**
The target type can be a record (as in the example) or a class type with properties named as the source columns and with a parameterless setter.

Target will support mapping database nullable fields to Option and ValueOption types automatically.

The target field name can also be different than the column name; in this case, it must be decorated with the MappedColumnAttribute custom attribute:
*)

open FSharp.Data.Sql.Common

type Employee3 = {
    [<MappedColumn("FirstName")>] GivenName:string
    [<MappedColumn("LastName")>] FamilyName:string
    }

let qry2 =
          query {
                for row in employees do
                select row} |> Seq.map (fun x -> x.MapTo<Employee3>())


(**

### ColumnValues

Or alternatively, the ColumnValues from SQLEntity can be used to create a map, with the
column as a key:
*)

let rows =
        query {
            for row in employees do
            select row} |> Seq.toArray

let employees2map = rows |> Seq.map(fun i -> i.ColumnValues |> Map.ofSeq)
let firstNames = employees2map |> Seq.map (fun x -> x.["FirstName"])
