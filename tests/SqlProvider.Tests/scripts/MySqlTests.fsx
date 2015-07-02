#I @"../../../packages/MySql.Data/lib/net40"
#r @"../../../packages/MySql.Data/lib/net40/MySql.Data.dll"
#I @"../bin/Debug"
#r @"../bin/Debug/FSharp.Data.SqlProvider.dll"

open System
open FSharp.Data.Sql

[<Literal>]
let connStr = "Server=MYSQL;Database=HR;Uid=admin;Pwd=password;"
[<Literal>]
let resolutionFolder = __SOURCE_DIRECTORY__ + @"/../../../packages/MySql.Data/lib/net40/"
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %s")

let processId = System.Diagnostics.Process.GetCurrentProcess().Id;

type HR = SqlDataProvider<Common.DatabaseProviderTypes.MYSQL, connStr, ResolutionPath = resolutionFolder, Owner = "HR">
let ctx = HR.GetDataContext()


type Employee = {
    EmployeeId : int32
    FirstName : string
    LastName : string
    HireDate : DateTime
}

//***************** Individuals ***********************//
let indv = ctx.Employees.Individuals.``As FirstName``.``100, Steven``


indv.FirstName + " " + indv.LastName + " " + indv.Email


//*************** QUERY ************************//
let employeesFirstName = 
    query {
        for emp in ctx.Employees do
        select (emp.FirstName, emp.LastName)
    } |> Seq.toList

let salesNamedDavid = 
    query {
            for emp in ctx.Employees do
            join d in ctx.Departments on (emp.DepartmentId = d.DepartmentId)
            where (d.DepartmentName |=| [|"Sales";"IT"|] && emp.FirstName =% "David")
            select (d.DepartmentName, emp.FirstName, emp.LastName)
    } |> Seq.toList

let employeesJob = 
    query {
            for emp in ctx.Employees do
            for manager in emp.employees_ibfk_3 do
            join dept in ctx.Departments on (emp.DepartmentId = dept.DepartmentId)
            where ((dept.DepartmentName |=| [|"Sales";"Executive"|]) && emp.FirstName =% "David")
            select (emp.FirstName, emp.LastName, manager.FirstName, manager.LastName )
    } |> Seq.toList

//Can map SQLEntities to a domain type
let topSales5ByCommission = 
    query {
        for emp in ctx.Employees do
        sortByDescending emp.CommissionPct
        select emp
        take 5
    } 
    |> Seq.map (fun e -> e.MapTo<Employee>())
    |> Seq.toList

#r @"..\..\packages\Newtonsoft.Json.6.0.3\lib\net45\Newtonsoft.Json.dll"

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
        for emp in ctx.Countries do
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
            for reg in ctx.Regions do
            where (reg.RegionId = 5u)
            select reg
        } |> Seq.toList
    match result with
    | [ant] -> ant
    | _ -> 
        let newRegion = ctx.Regions.Create() 
        newRegion.RegionName <- "Antartica"
        newRegion.RegionId <- 5u
        ctx.SubmitUpdates()
        newRegion

antartica.RegionName <- "ant"
ctx.SubmitUpdates()

antartica.Delete()
ctx.SubmitUpdates()

//********************** Procedures **************************//

ctx.Procedures.AddJobHistory.Invoke(100u, DateTime(1993, 1, 13), DateTime(1998, 7, 24), "IT_PROG", 60u)


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

//Support for MARS procs
let locations_and_regions =
    let results = ctx.Procedures.GetLocationsAndRegions.Invoke()
    [
//      for e in results.ReturnValue do
//        yield e.ColumnValues |> Seq.toList |> box
//             
      for e in results.ColumnValues do
        yield e// |> Seq.toList |> box
    ]


//Support for sprocs that return ref cursors and has in parameters
let getemployees hireDate =
    let results = (ctx.Procedures.GetEmployeesStartingAfter.Invoke hireDate)
    [
      for e in results.ResultSet do
        yield e.MapTo<Employee>()
    ]

getemployees (new System.DateTime(1999,4,1))

//********************** Functions ***************************//

let fullName = ctx.Functions.FnEmpFullname.Invoke(100u).ReturnValue