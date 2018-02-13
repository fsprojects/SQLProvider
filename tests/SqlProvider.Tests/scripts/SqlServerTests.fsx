#I @"../../../bin/net451"
#r @"../../../bin/net451/FSharp.Data.SqlProvider.dll"

open System
open FSharp.Data.Sql

[<Literal>]
let connStr = @"Data Source=localhost; Initial Catalog=HR; Integrated Security=True"
//let connStr = "Data Source=SQLSERVER;Initial Catalog=HR;User Id=sa;Password=password"
[<Literal>]
let resolutionFolder = __SOURCE_DIRECTORY__
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %O")

let processId = System.Diagnostics.Process.GetCurrentProcess().Id;

type HR = SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER, connStr, ResolutionPath = resolutionFolder>
let ctx = HR.GetDataContext()

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
        select emp.FirstName
    } |> Seq.toList

let employeesFirstNameAsync = 
    query {
        for emp in ctx.Dbo.Employees do
        select emp.FirstName
    } |> Seq.executeQueryAsync |> Async.RunSynchronously

// Note that Employees-table should have a Description-field in database, visible as XML-tooltip in your IDE.
// Column-level descriptions work also, but they are not included to exported SQL-scripts by SQL-server.

//Ref issue #92
let employeesFirstNameEmptyList = 
    query {
        for emp in ctx.Dbo.Employees do
        where (emp.EmployeeId > 10000)
        select emp
    } |> Seq.toList

let regionsEmptyTable = 
    query {
        for r in ctx.Dbo.Regions do
        select r
    } |> Seq.toList

//let tableWithNoKey = 
//    query {
//        for r in ctx.Dbo.Table1 do
//        select r.Col
//    } |> Seq.toList
//
//let entity = ctx.Table1.Create()
//entity.Col <- 123uy
//ctx.SubmitUpdates()

let salesNamedDavid = 
    query {
            for emp in ctx.Dbo.Employees do
            join d in ctx.Dbo.Departments on (emp.DepartmentId = d.DepartmentId)
            where (d.DepartmentName |=| [|"Sales";"IT"|] && emp.FirstName =% "David")
            select (d.DepartmentName, emp.FirstName, emp.LastName)
            
    } |> Seq.toList

let employeesJob = 
    let dbo = ctx.Dbo
    query {
            for emp in dbo.Employees do
            for manager in emp.``dbo.EMPLOYEES by EMPLOYEE_ID`` do
            join dept in dbo.Departments on (emp.DepartmentId = dept.DepartmentId)
            where ((dept.DepartmentName |=| [|"Sales";"Executive"|]) && emp.FirstName =% "David")
            select (emp.FirstName, emp.LastName, manager.FirstName, manager.LastName)
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

let pagingTest = 
    query {
        for emp in ctx.Dbo.Employees do
        sortByDescending emp.CommissionPct
        select emp
        skip 2
        take 5
    } 
    |> Seq.map (fun e -> e.MapTo<Employee>())
    |> Seq.toList

#I @"../../../packages/scripts/Newtonsoft.Json/lib/net45"
#r @"../../../packages/scripts/Newtonsoft.Json/lib/net45/Newtonsoft.Json.dll"

open Newtonsoft.Json

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


let ``simple math operationsquery``() =
    let itemOf90 = 
        query {
            for p in ctx.Dbo.Departments do
            where (p.DepartmentId - 85 = 5)
            select p.DepartmentId 
        } |> Seq.toList

    let ``should be empty`` = 
        query {
            for p in ctx.Dbo.Departments do
            where (p.DepartmentId <> 100 &&  (p.DepartmentId - 100 = 100 - p.DepartmentId))
            select p.DepartmentId 
        } |> Seq.toList
    itemOf90, ``should be empty``


let canoncicalOpTest = 
    query {
        // Silly query not hitting indexes, so testing purposes only...
        for job in ctx.Dbo.Jobs do
        join emp in ctx.Dbo.Employees on (job.JobId.Trim() + "z" = emp.JobId.Trim() + "z")
        where (
            floor(job.MaxSalary)+1m > 4m
            && emp.Email.Length > 1  
            && emp.HireDate.Date.AddYears(-3).Year + 1 > 1997
            && emp.HireDate.AddDays(1.).Subtract(emp.HireDate).Days = 1
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

ctx.Procedures.AddJobHistory.Invoke(100, DateTime(1993, 1, 13), DateTime(1998, 7, 24), "IT_PROG", 60)


//Support for sprocs that return ref cursors
let employees =
    [
      for e in ctx.Procedures.GetEmployees.Invoke().ResultSet do
        yield e.MapTo<Employee>()
    ]

let employeesAsync =
    async {
        let! ia = ctx.Procedures.GetEmployees.InvokeAsync()
        return ia.ResultSet
    } |> Async.RunSynchronously

type Region = {
    RegionId : int
    RegionName : string
    RegionDescription : string
}

//Support for MARS procs
let locations_and_regions =
    let results = ctx.Procedures.GetLocationsAndRegions.Invoke()
    printfn "%A" results.ColumnValues
    [
      for e in results.ResultSet do
        yield e.ColumnValues |> Seq.toList |> box
              
      for e in results.ResultSet_1 do
        yield e.MapTo<Region>() |> box
    ]


//Support for sprocs that return ref cursors and has in parameters
let getemployees hireDate =
    let results = (ctx.Procedures.GetEmployeesStartingAfter.Invoke hireDate)
    [
      for e in results.ResultSet do
        yield e.MapTo<Employee>()
    ]

getemployees (new System.DateTime(1999,4,1))


// Distinct alias test
let employeesFirstNameSort = 
    query {
        for emp in ctx.Dbo.Employees do
        sortBy (emp.FirstName)
        select (emp.FirstName, emp.FirstName)
    } |> Seq.toList

// Standard deviation test
let stdDevTest = 
    query {
        for emp in ctx.Dbo.Employees do
        select (float emp.Salary)
    } |> Seq.stdDevAsync |> Async.RunSynchronously


//******************** Delete all test **********************//

query {
    for c in ctx.Dbo.Employees do
    where (c.FirstName = "Tuomas")
} |> Seq.``delete all items from single table`` 
|> Async.RunSynchronously
