#r @"../../../bin/net451/FSharp.Data.SqlProvider.dll"
#r @"../../../packages/scripts/Newtonsoft.Json/lib/net45/Newtonsoft.Json.dll"

open System
open FSharp.Data.Sql
open FSharp.Data.Sql.Common
open Newtonsoft.Json

[<Literal>]
let port = "3050"
[<Literal>]
let connectionString1 = @" Data Source=localhost;port=" + port + ";initial catalog=" + __SOURCE_DIRECTORY__ + @"\northwindfbd1.fdb;user id=SYSDBA;password=masterkey;Dialect=1"
[<Literal>]
let connectionString3 = @" Data Source=localhost; port=" + port + ";initial catalog=" + __SOURCE_DIRECTORY__ + @"\northwindfbd3.fdb;user id=SYSDBA;password=masterkey;Dialect=3"

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + "/../../../packages/scripts/FirebirdSql.Data.FirebirdClient/lib/net452"

type HR = SqlDataProvider<Common.DatabaseProviderTypes.FIREBIRD, connectionString1, ResolutionPath = resolutionPath>
let ctx = HR.GetDataContext()
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %O")

let processId = System.Diagnostics.Process.GetCurrentProcess().Id;

//*************** quoted table names ***********
//just get the single existing record
type HRCamelCase = SqlDataProvider<Common.DatabaseProviderTypes.FIREBIRD, connectionString3, ResolutionPath = resolutionPath, OdbcQuote = OdbcQuoteCharacter.DOUBLE_QUOTES>

let ctxCamelCase = HRCamelCase.GetDataContext()
let cc = query {
    for camelCase in ctxCamelCase.Dbo.CamelCase do
    head
}
printfn "got some name from CC table: %s" cc.Name
//insert/create
let cc1 = ctxCamelCase.Dbo.CamelCase.Create()
cc1.Id <- 2
cc1.Name <- "nr 2"
ctxCamelCase.SubmitUpdates()
//individuals/update
let ccForUpdate = ctxCamelCase.Dbo.CamelCase.Individuals.``1``
ccForUpdate.Name <- "nr 11"
//delete
let ccForDelete = query {
    for camelCase in ctxCamelCase.Dbo.CamelCase do
    where (camelCase.Id = 2)
    head
}
ccForDelete.Delete()
ctxCamelCase.SubmitUpdates()


type Employee = {
    EmployeeId : int32
    FirstName : string
    LastName : string
    HireDate : DateTime
}


//***************** Individuals ***********************//
let indv = ctx.Dbo.Employees.Individuals.``As FirstName``.``100, Steven``


indv.FirstName + " " + indv.LastName + " " + indv.Email


//*************** QUERY ************************//
let employeesFirstName = 
    query {
        for emp in ctx.Dbo.Employees do
        select (emp.FirstName, emp.LastName)
    } |> Seq.toList

let employeesFirstNameAsync = 
    query {
        for emp in ctx.Dbo.Employees do
        select (emp.FirstName, emp.LastName, emp.PhoneNumber)
    } |> Seq.executeQueryAsync |> Async.RunSynchronously

// Note that Employees-table and PhoneNumber should have a Comment-field in database, visible as XML-tooltip in your IDE.

let salesNamedDavid = 
    query {
            for emp in ctx.Dbo.Employees do
            join d in ctx.Dbo.Departments on (emp.DepartmentId = d.DepartmentId)
            where (d.DepartmentName |=| [|"Sales";"IT"|] && emp.FirstName =% "David")
            select (d.DepartmentName, emp.FirstName, emp.LastName)
    } |> Seq.toList

let employeesJob =
    let hr = ctx.Dbo
    query {
            for emp in hr.Employees do
            for manager in emp.``Dbo.EMPLOYEES by MANAGER_ID`` do
            join dept in hr.Departments on (emp.DepartmentId = dept.DepartmentId)
            where ((dept.DepartmentName |=| [|"Sales";"Executive"|]) && emp.FirstName =% "David")
            select (emp.FirstName, emp.LastName, manager.FirstName, manager.LastName )
    } |> Seq.toList

//Can map SQLEntities to a domain type
let topSales5ByCommission = 
    query {
        for emp in ctx.Dbo.Employees do
        sortByDescending emp.CommissionPct
        select emp
        take 5
    } 
    |> Seq.map (fun e -> e.MapTo<Employee>())
    |> Seq.toList

type OtherCountryInformation = {
    Id : string
    Population : int
}

type Country = {
    CountryId : string
    CountryName : string
    Other : OtherCountryInformation
}

//Can customise SQLEntity mapping
let countries = 
    query {
        for emp in ctx.Dbo.Countries do
        select emp
    } 
    |> Seq.map (fun e -> e.MapTo<Country>(fun (prop,value) -> 
                                               match prop with
                                               | "Other" -> 
                                                    if value <> null
                                                    then JsonConvert.DeserializeObject<OtherCountryInformation>(value :?> string) |> box
                                                    else Unchecked.defaultof<OtherCountryInformation> |> box
                                               | _ -> value
                                         )
               )
    |> Seq.toList

open System.Linq

let nestedQueryTest = 
    let qry1 = query {
        for emp in ctx.Dbo.Employees do
        where (emp.FirstName.StartsWith("S"))
        select (emp.FirstName)
    }
    query {
        for emp in ctx.Dbo.Employees do
        where (qry1.Contains(emp.FirstName))
        select (emp.FirstName, emp.LastName)
    } |> Seq.toArray

// Simple group-by test
open System.Linq
let qry = 
    query {
        for e in ctx.Dbo.Employees do
        groupBy e.DepartmentId into p
        select (p.Key, p.Sum(fun f -> f.Salary))
    } |> Seq.toList
    
let canoncicalOpTest = 
    query {
        // Silly query not hitting indexes, so testing purposes only...
        for job in ctx.Dbo.Jobs do
        join emp in ctx.Dbo.Employees on (job.JobId.Trim() + "x" = emp.JobId.Trim() + "x")
        where (
            //floor(job.MaxSalary)+1m > 4m 
            job.MaxSalary+1 > 4 
            && emp.Email.Length > 2
            && emp.HireDate.Date.AddYears(-3).Year + 1 > 1997
        )
        sortBy emp.HireDate.Day
        select (emp.HireDate, emp.Email, job.MaxSalary)
    } |> Seq.toArray

//************************ CRUD *************************//


let antartica =
    let result =
        query {
            for reg in ctx.Dbo.Regions do
            where (reg.RegionId = 5)
            select reg
        } |> Seq.toList
    match result with
    | [ant] -> ant
    | _ -> 
        let newRegion = ctx.Dbo.Regions.Create() 
        newRegion.RegionName <- "Antartica"
        newRegion.RegionId <- 5
        ctx.SubmitUpdates()
        newRegion

antartica.RegionName <- "ant"
ctx.SubmitUpdates()

antartica.Delete()
ctx.SubmitUpdatesAsync() |> Async.RunSynchronously

//********************** Procedures **************************//
//make sure to delete any conflicting record before trying to insert
let historyItem = 
    query {
        for job in ctx.Dbo.JobHistory do
        where (job.EmployeeId = 101 && job.StartDate = DateTime(1993, 1, 13))
        select job    
        head
    } 
try historyItem.Delete() with | _ -> ignore ()
ctx.SubmitUpdates()
//insert using sproc
ctx.Procedures.AddJobHistory.Invoke(101, DateTime(1993, 1, 13), DateTime(1998, 7, 24), "IT_PROG", 60)

//Support for sprocs that return ref cursors
let employees =
    [
      for e in ctx.Procedures.GetEmployees.Invoke().ResultSet do
        yield e.MapTo<Employee>()
    ]

type Region = {
    RegionId : decimal
    RegionName : string
  //  RegionDescription : string
}

(*//Support for MARS procs
let locations_and_regions =
    let results = ctx.Procedures.GetLocationsAndRegions.Invoke()
    [
//      for e in results.ReturnValue do
//        yield e.ColumnValues |> Seq.toList |> box
//             
      for e in results.ColumnValues do
        yield e// |> Seq.toList |> box
    ]
*)

//Support for sprocs that return ref cursors and has in parameters
let getemployees hireDate =
    let results = (ctx.Procedures.GetEmployeesStartingAfter.Invoke hireDate)
    [
      for e in results.ResultSet do
        yield e.MapTo<Employee>()
    ]

getemployees (new System.DateTime(1999,4,1))

//********************** Functions ***************************//

//let fullName = ctx.Functions.EmpFullname.Invoke(100u).ReturnValue

//********************** Thread test ***************************//

let rnd = new Random()
open System.Threading

let taskarray = 
   [1u..100u] |> Seq.map(fun itm ->
       let itm = rnd.Next(1, 300) |> int
       let t1 = Tasks.Task.Run(fun () ->
           let ctx1 = HR.GetDataContext()
           let country1 = 
               query {
                   for c in ctx1.Dbo.Countries do
                   where (c.CountryId = "AR")
                   head
               }
           Console.WriteLine Thread.CurrentThread.ManagedThreadId
           country1.CountryName <- "Argentina" + itm.ToString()
           ctx1.SubmitUpdates()
       )
       let t2 = Tasks.Task.Run(fun () ->
           let ctx2 = HR.GetDataContext()
           Console.WriteLine Thread.CurrentThread.ManagedThreadId
           let country2 = 
               query {
                   for c in ctx2.Dbo.Countries do
                   where (c.CountryId = "BR")
                   head
               }
           country2.RegionId <- itm
           ctx2.SubmitUpdates()
       )
       Tasks.Task.WhenAll [|t1; t2|]
       //ctx.ClearUpdates() |> ignore
   ) |> Seq.toArray |> Tasks.Task.WaitAll

ctx.GetUpdates()

//******************** Delete all test **********************//

query {
    for count in ctx.Dbo.Countries do
    where (count.CountryName = "Andorra" || count.RegionId = 99934)
} |> Seq.``delete all items from single table`` 
|> Async.RunSynchronously
