#I @"../../../bin"
#r @"../../../bin/FSharp.Data.SqlProvider.dll"

open System
open FSharp.Data.Sql

[<Literal>]
let connStr = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=ORACLE)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=XE)));User Id=HR;Password=password;"

[<Literal>]
let resolutionFolder = "/Users/colinbull/appdev/SqlProvider/tests/SqlProvider.Tests/libs/"
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %s")

let processId = System.Diagnostics.Process.GetCurrentProcess().Id;

type HR = SqlDataProvider<Common.DatabaseProviderTypes.ORACLE, connStr, ResolutionPath = resolutionFolder, Owner = "HR">
let ctx = HR.GetDataContext()

type Employee = {
    EmployeeId : int32
    FirstName : string
    LastName : string
    HireDate : DateTime
}

//***************** Individuals ***********************//
let indv = ctx.Hr.Employees.Individuals.``As FirstName``.``100, Steven``


indv.FirstName + " " + indv.LastName + " " + indv.Email


//*************** QUERY ************************//
let employeesFirstName = 
    query {
        for emp in ctx.Hr.Employees do
        select (emp.FirstName, emp.LastName)
    } |> Seq.toList

let salesNamedDavid = 
    query {
            for emp in ctx.Hr.Employees do
            join d in ctx.Hr.Departments on (emp.DepartmentId = d.DepartmentId)
            where (d.DepartmentName |=| [|"Sales";"IT"|] && emp.FirstName =% "David")
            select (d.DepartmentName, emp.FirstName, emp.LastName)
    } |> Seq.toList

let employeesJob = 
    query {
            for emp in ctx.Hr.Employees do
            for manager in emp.``HR.EMPLOYEES by MANAGER_ID`` do
            join dept in ctx.Hr.Departments on (emp.DepartmentId = dept.DepartmentId)
            where ((dept.DepartmentName |=| [|"Sales";"Executive"|]) && emp.FirstName =% "David")
            select (emp.FirstName, emp.LastName, manager.FirstName, manager.LastName )
    } |> Seq.toList

//Can select from views
let empDetails =
    query {
        for empd in ctx.Hr.EmpDetailsView do
        select (empd.EmployeeId, empd.ManagerId, empd.DepartmentId, empd.JobId)
    }
    |> Seq.toList

//Can map SQLEntities to a domain type
let topSales5ByCommission =
    query {
        for emp in ctx.Hr.Employees do
        sortByDescending emp.CommissionPct
        select emp
        take 5
    } 
    |> Seq.map (fun e -> e.MapTo<Employee>())
    |> Seq.toList

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
        for emp in ctx.Hr.Countries do
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

//************************ CRUD *************************//


let antartica =
    let result =
        query {
            for reg in ctx.Hr.Regions do
            where (reg.RegionId = 5M)
            select reg
        } |> Seq.toList
    match result with
    | [ant] -> ant
    | _ -> 
        let newRegion = ctx.Hr.Regions.Create() 
        newRegion.RegionName <- "Antartica"
        newRegion.RegionId <- 5M
        ctx.SubmitUpdates()
        newRegion

antartica.RegionName <- "ant"
ctx.SubmitUpdates()

antartica.Delete()
ctx.SubmitUpdates()

//********************** Procedures **************************//


ctx.Procedures.AddJobHistory.Invoke(100M, DateTime(1993, 1, 13), DateTime(1998, 7, 24), "IT_PROG", 60M)

//Support for sprocs with no parameters
ctx.Procedures.SecureDml.Invoke()

//Support for sprocs that return ref cursors
let employees =
    [
      for e in ctx.Procedures.GetEmployees.Invoke().CATCUR do
        yield e.MapTo<Employee>()
    ]

type Region = {
    RegionId : decimal
    RegionName : string
  //  RegionDescription : string
}

//Support for MARS procs
let locations_and_regions =
    let results = ctx.Procedures.GetLocationsAndRegions.Invoke()
    [
      for e in results.LOCATIONS do
        yield e.ColumnValues |> Seq.toList |> box
              
      for e in results.REGIONS do
        yield e.MapTo<Region>() |> box
    ]


//Support for sprocs that return ref cursors and has in parameters
let getemployees hireDate =
    let results = (ctx.Procedures.GetEmployeesStartingAfter.Invoke hireDate)
    [
      for e in results.RESULTS do
        yield e.MapTo<Employee>()
    ]

getemployees (new System.DateTime(1999,4,1))

//********************** Functions ***************************//

let fullName = ctx.Functions.EmpFullname.Invoke(100M).ReturnValue

//********************** Packaged Procs **********************//

ctx.Packages.TestPackage.InsertJobHistory.Invoke(100M, DateTime(1993, 1, 13), DateTime(1998, 7, 24), "IT_PROG", 60M)

//********************** Packaged Funcs **********************//

let fullNamPkg = ctx.Packages.TestPackage.Fullname.Invoke("Bull", "Colin").ReturnValue
