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
let resolutionPath = __SOURCE_DIRECTORY__ + @"/../"

(**
# CRUD sample
*)

open System
open FSharp.Data.Sql

type sql = SqlDataProvider<Common.DatabaseProviderTypes.SQLITE,
                           connectionString,
                           ResolutionPath = resolutionPath,
                           CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL>

let ctx = sql.GetDataContext()

let orders = ctx.Main.Orders
let employees = ctx.Main.Employees

let customer = ctx.Main.Customers |> Seq.head 
let employee = ctx.Main.Employees |> Seq.head
let now = DateTime.Now

(**
Create() has various overloads to make inserting records simple.
*)

(**
Create a new row
*)

let row = orders.Create()
row.CustomerId <- customer.CustomerId
row.EmployeeId <- employee.EmployeeId
row.Freight <- 10M
row.OrderDate <- now.AddDays(-1.0)
row.RequiredDate <- now.AddDays(1.0)
row.ShipAddress <- "10 Downing St"
row.ShipCity <- "London"
row.ShipName <- "Dragons den"
row.ShipPostalCode <- "SW1A 2AA"
row.ShipRegion <- "UK"
row.ShippedDate <- now

(**
Submit updates to the database
*)
ctx.SubmitUpdates()

(**
After updating your item (row) will have the Id property.



You can also create with the longer \`\`Create(...)\`\`(parameters)-method like this:

*)

let emp = ctx.Main.Employees.``Create(FirstName, LastName)``("Don", "Syme")

(**

Delete the row
*)
row.Delete()

(**
Submit updates to the database
*)
ctx.SubmitUpdates()


(**Insert a list of records:
*)

type Employee = {
    FirstName:string
    LastName:string
}

let mvps1 = [
    {FirstName="Andrew"; LastName="Kennedy"};
    {FirstName="Mads"; LastName="Torgersen"};
    {FirstName="Martin";LastName="Odersky"};
]

mvps1 
    |> List.map (fun x ->
                    let row = employees.Create()
                    row.FirstName <- x.FirstName
                    row.LastName <- x.LastName)

ctx.SubmitUpdates()

(**Or directly specify the fields:
*)

let mvps2 = [
    {FirstName="Byron"; LastName="Cook"};
    {FirstName="James"; LastName="Huddleston"};
    {FirstName="Xavier";LastName="Leroy"};
]

mvps2
    |> List.map (fun x ->                                 
                   employees.Create(x.FirstName, x.LastName)                  
                    )

ctx.SubmitUpdates()

(** update a single row
    assuming Id is unique
*)

type Employee2 = {
    Id:int
    FirstName:string
    LastName:string
}

let updateEmployee (employee: Employee2) =
    let foundEmployee = query {
        for p in ctx.Public.Employee2 do
        where (p.Id = employee.Id)
        exactlyOneOrDefault
    }
    if not (isNull foundEmployee) then
        foundEmployee.FirstName <- employee.FirstName
        foundEmployee.LastName <- employee.LastName
    ctx.SubmitUpdates()

let updateEmployee' (employee: Employee2) =
    query {
        for p in ctx.Public.Employee2 do
        where (p.Id = employee.Id)
    }
    |> Seq.iter( fun e ->
        e.FirstName <- employee.FirstName
        e.LastName <- employee.LastName
    )
    ctx.SubmitUpdates()

let john = {
  Id = 1
  FirstName = "John"
  LastName = "Doe" }

updateEmployee john
updateEmployee' john


(**Finally it is also possible to specify a seq of `string * obj` which is exactly the 
output of .ColumnValues:
*)

employees 
    |> Seq.map (fun x ->
                employee.Create(x.ColumnValues)) // create twins
    |>  Seq.toList

let twins = ctx.GetUpdates() // Retrieve the FSharp.Data.Sql.Common.SqlEntity objects

ctx.ClearUpdates() // delete the updates
ctx.GetUpdates() // Get the updates
ctx.SubmitUpdates() // no record is added

(** 

Inside SubmitUpdate the transaction is created by default TransactionOption, which is Required: Shares a transaction, if one exists, and creates a new transaction if necessary. So e.g. if you have query-operation before SubmitUpdates, you may want to create your own transaction to wrap these to the same transaction.

SQLProvider also supports async database operations: 

*)

ctx.SubmitUpdatesAsync() |> Async.StartAsTask

(**

### Delete-query for multiple items

If you want to delete many items from a database table, `DELETE FROM [dbo].[EMPLOYEES] WHERE (...)`, there is a way, although we don't recommend deleting items from a database. Instead you should consider a deletion-flag column. And you should backup your database before even trying this.

*)

query {
    for c in ctx.Dbo.Employees do
    where (...)
} |> Seq.``delete all items from single table``  |> Async.RunSynchronously

(**

### Selecting which Create() to use

There are 3 overrides of create.

**The ideal one to use is the long one \`\`Create(...)\`\`(...):**

*)

let emp = ctx.Main.Employees.``Create(FirstName, LastName)``("Don", "Syme")

(**

This is because it will fail if your database structure changes.
So, when your table gets new columns, the code will fail at compile time.
Then you decide what to do with the new columns, and not let a bug to customers.

But you may want to use the plain .Create() if your setup is not optimal.
Try to avoid these conditions:

* If your editor intellisense is not working for backtick-variables.
* You have lot of nullable columns in your database.
* You want to use F# like a dynamic language.

In the last case you'll be maintaining code like this:

*)

let employeeId = 123
// Got some untyped array of data from the client
let createSomeItem (data: seq<string*obj>)  = 
    data
    |> Seq.map( // Some parsing and validation:
        function 
        // Skip some fields
        | "EmployeeId", x
        | "PermissionLevel", x -> "", x
        // Convert and validate some fields
        | "PostalCode", x -> 
            "PostalCode", x.ToString().ToUpper().Replace(" ", "") |> box
        | "BirthDate", x -> 
            let bdate = x.ToString() |> DateTime.Parse
            if bdate.AddYears(18) > DateTime.UtcNow then
                failwith "Too young!"
            else
                "BirthDate", bdate.ToString("yyyy-MM-dd") |> box
        | others -> others)
    |> Seq.filter (fun (key,_) -> key <> "")
                  // Add some fields:
    |> Seq.append [|"EmployeeId", employeeId |> box; 
                    "Country", "UK" |> box |]
    |> ctx.Main.Employees.Create

(**
### What to do if your creation fails systematically every time

Some underlying database connection libraries have problems with serializing underlying data types.
So, if this fails:

*)

emp.BirthDate <- DateTime.UtcNow
ctx.SubmitUpdates()

(**

Try using `.SetColumn("ColumnName", value |> box)`
for example:

*)

emp.SetColumn("BirthDate", DateTime.UtcNow.ToString("yyyy-MM-dd HH\:mm\:ss") |> box)
ctx.SubmitUpdates()

(**

SetColumn takes object, so you have more control over the type serialization.

*)