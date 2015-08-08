#r @"..\..\bin\FSharp.Data.SqlProvider.dll"

open System
open FSharp.Data.Sql

[<Literal>]
let connStr = "User ID=postgres;Password=password;Host=POSTGRESQL;Port=9090;Database=hr;"

[<Literal>]
let resolutionFolder = @"D:\Downloads\Npgsql-2.1.3-net40\"
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %s")

let processId = System.Diagnostics.Process.GetCurrentProcess().Id;

type HR = SqlDataProvider<Common.DatabaseProviderTypes.POSTGRESQL, connStr, ResolutionPath = resolutionFolder>
let ctx = HR.GetDataContext()

type Employee = {
    EmployeeId : int32
    FirstName : string
    LastName : string
    HireDate : DateTime
}

//***************** Individuals ***********************//
let indv = ctx.``[PUBLIC].[EMPLOYEES]``.Individuals.``As first_name``.``100, Steven``

indv.first_name + " " + indv.last_name + " " + indv.email


//*************** QUERY ************************//
let employeesFirstName = 
    query {
        for emp in ctx.``[PUBLIC].[EMPLOYEES]`` do
        select (emp.first_name, emp.last_name)
    } |> Seq.toList

let salesNamedDavid = 
    query {
            for emp in ctx.``[PUBLIC].[EMPLOYEES]`` do
            join d in ctx.``[PUBLIC].[DEPARTMENTS]`` on (emp.department_id = d.department_id)
            where (d.department_name |=| [|"Sales";"IT"|] && emp.first_name =% "David")
            select (d.department_name, emp.first_name, emp.last_name)
            
    } |> Seq.toList

let employeesJob = 
    query {
            for emp in ctx.``[PUBLIC].[EMPLOYEES]`` do
            for manager in emp.employees_manager_id_fkey do
            join dept in ctx.``[PUBLIC].[DEPARTMENTS]`` on (emp.department_id = dept.department_id)
            where ((dept.department_name |=| [|"Sales";"Executive"|]) && emp.first_name =% "David")
            select (emp.first_name, emp.last_name, manager.first_name, manager.last_name )
    } |> Seq.toList

//Can map SQLEntities to a domain type
let topSales5ByCommission = 
    query {
        for emp in ctx.``[PUBLIC].[EMPLOYEES]`` do
        sortByDescending emp.commission_pct
        select emp
        take 5
    } 
    |> Seq.map (fun e -> e.MapTo<Employee>())
    |> Seq.toList

type Simple = {First : string}

type Dummy<'t> = D of 't

let employeesFirstName1 = 
    query {
        for emp in ctx.``[PUBLIC].[EMPLOYEES]`` do
        select (D {First=emp.first_name})
    } |> Seq.toList

let employeesFirstName2 = 
    query {
        for emp in ctx.``[PUBLIC].[EMPLOYEES]`` do
        select ({First=emp.first_name} |> D)
    } |> Seq.toList

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
        for emp in ctx.``[PUBLIC].[COUNTRIES]`` do
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
            for reg in ctx.``[PUBLIC].[REGIONS]`` do
            where (reg.region_id = 5)
            select reg
        } |> Seq.toList
    match result with
    | [ant] -> ant
    | _ -> 
        let newRegion = ctx.``[PUBLIC].[REGIONS]``.Create() 
        newRegion.region_name <- "Antartica"
        newRegion.region_id <- 5
        ctx.SubmitUpdates()
        newRegion

antartica.region_name <- "ant"
ctx.SubmitUpdates()

antartica.Delete()
ctx.SubmitUpdates()

//********************** Procedures **************************//

let removeIfExists employeeId startDate =
    let existing = query { for x in ctx.``[PUBLIC].[JOB_HISTORY]`` do
                           where ((x.employee_id = employeeId) && (x.start_date = startDate))
                           headOrDefault }
    if existing <> null then
        existing.Delete()
        ctx.SubmitUpdates()

removeIfExists 100 (DateTime(1993, 1, 13))
ctx.Functions.add_job_history.Invoke(100, DateTime(1993, 1, 13), DateTime(1998, 7, 24), "IT_PROG", 60)

//Support for sprocs that return ref cursors
let employees =
    [
      for e in ctx.Functions.get_employees.Invoke().ReturnValue do
        yield e.MapTo<Employee>()
    ]

type Region = {
    RegionId : decimal
    RegionName : string
  //  RegionDescription : string
}

//Support for MARS procs
let locations_and_regions =
    let results = ctx.Functions.get_locations_and_regions.Invoke()
    [
//      for e in results.ReturnValue do
//        yield e.ColumnValues |> Seq.toList |> box
//             
      for e in results.ColumnValues do
        yield e// |> Seq.toList |> box
    ]


//Support for sprocs that return ref cursors and has in parameters
let getemployees hireDate =
    let results = (ctx.Functions.get_employees_starting_after.Invoke hireDate)
    [
      for e in results.ReturnValue do
        yield e.MapTo<Employee>()
    ]

getemployees (new System.DateTime(1999,4,1))

//********************** Functions ***************************//

let fullName = ctx.Functions.emp_fullname.Invoke(100).ReturnValue
