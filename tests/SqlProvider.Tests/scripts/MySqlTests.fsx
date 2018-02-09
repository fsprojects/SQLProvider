#I @"../../../bin/net451"
#r @"../../../bin/net451/FSharp.Data.SqlProvider.dll"
#r @"../../../packages/scripts/Newtonsoft.Json/lib/net45/Newtonsoft.Json.dll"


open System
open FSharp.Data.Sql
open FSharp.Data.Sql.Common
open Newtonsoft.Json
        
[<Literal>]
let connStr = "Server=localhost;Database=HR;Uid=admin;Pwd=password;Auto Enlist=false; Convert Zero Datetime=true;" // SslMode=none;

[<Literal>]
let dataPath = __SOURCE_DIRECTORY__ + @"/../../../packages/scripts/MySql.Data/lib/net45/"

let processId = System.Diagnostics.Process.GetCurrentProcess().Id;

type HR = SqlDataProvider<Common.DatabaseProviderTypes.MYSQL, connStr, ResolutionPath = dataPath, Owner = "HR">
let ctx = HR.GetDataContext()
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %O")

        
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

let employeesFirstNameAsync = 
    query {
        for emp in ctx.Hr.Employees do
        select (emp.FirstName, emp.LastName, emp.PhoneNumber)
    } |> Seq.executeQueryAsync |> Async.RunSynchronously

// Note that Employees-table and PhoneNumber should have a Comment-field in database, visible as XML-tooltip in your IDE.

let salesNamedDavid = 
    query {
            for emp in ctx.Hr.Employees do
            join d in ctx.Hr.Departments on (emp.DepartmentId = d.DepartmentId)
            where (d.DepartmentName |=| [|"Sales";"IT"|] && emp.FirstName =% "David")
            select (d.DepartmentName, emp.FirstName, emp.LastName)
    } |> Seq.toList

let employeesJob =
    let hr = ctx.Hr
    query {
            for emp in hr.Employees do
            for manager in emp.``HR.EMPLOYEES by MANAGER_ID`` do
            join dept in hr.Departments on (emp.DepartmentId = dept.DepartmentId)
            where ((dept.DepartmentName |=| [|"Sales";"Executive"|]) && emp.FirstName =% "David")
            select (emp.FirstName, emp.LastName, manager.FirstName, manager.LastName )
    } |> Seq.toList

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

open System.Linq

let nestedQueryTest = 
    let qry1 = query {
        for emp in ctx.Hr.Employees do
        where (emp.FirstName.StartsWith("S"))
        select (emp.FirstName)
    }
    query {
        for emp in ctx.Hr.Employees do
        where (qry1.Contains(emp.FirstName))
        select (emp.FirstName, emp.LastName)
    } |> Seq.toArray

// Simple group-by test
open System.Linq
let qry = 
    query {
        for e in ctx.Hr.Employees do
        groupBy e.DepartmentId into p
        select (p.Key, p.Sum(fun f -> f.Salary))
    } |> Seq.toList
    
let canoncicalOpTest = 
    query {
        // Silly query not hitting indexes, so testing purposes only...
        for job in ctx.Hr.Jobs do
        join emp in ctx.Hr.Employees on (job.JobId.Trim() + "x" = emp.JobId.Trim() + "x")
        where (
            floor(job.MaxSalary)+1m > 4m
            && emp.Email.Length > 2
            && emp.HireDate.Date.AddYears(-3).Year + 1 > 1997
            && Math.Min(emp.Salary, 3m) = 3m
            && emp.HireDate.AddDays(1.).Subtract(emp.HireDate).Days = 1
        )
        sortBy emp.HireDate.Day
        select (emp.HireDate, emp.Email, job.MaxSalary)
    } |> Seq.toArray


// Standard deviation test
let stdDevTest = 
    query {
        for emp in ctx.Hr.Employees do
        select (float emp.Salary)
    } |> Seq.stdDevAsync |> Async.RunSynchronously


//************************ CRUD *************************//


let antartica =
    let result =
        query {
            for reg in ctx.Hr.Regions do
            where (reg.RegionId = 5)
            select reg
        } |> Seq.toList
    match result with
    | [ant] -> ant
    | _ -> 
        let newRegion = ctx.Hr.Regions.Create() 
        newRegion.RegionName <- "Antartica"
        newRegion.RegionId <- 5
        ctx.SubmitUpdates()
        newRegion

antartica.RegionName <- "ant"
ctx.SubmitUpdates()

antartica.Delete()
ctx.SubmitUpdatesAsync() |> Async.RunSynchronously

//********************** Procedures **************************//

ctx.Procedures.AddJobHistory.Invoke(101, DateTime(1993, 1, 13), DateTime(1998, 7, 24), "IT_PROG", 60)


//Support for sprocs that return ref cursors
let employees =
    [
      for e in ctx.Procedures.GetEmployees.Invoke().ResultSet do
        yield e.MapTo<Employee>()
    ]

let employeesAsync =
    async {
        let! res = ctx.Procedures.GetEmployees.InvokeAsync()
        return res.ResultSet |> Array.map(fun e -> e.MapTo<Employee>())
    } |> Async.RunSynchronously

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

let fullName = ctx.Functions.EmpFullname.Invoke(100).ReturnValue

//********************** Thread test ***************************//

let rnd = new Random()
open System.Threading

let taskarray = 
    [1u..100u] |> Seq.map(fun itm ->
        let itm = rnd.Next(1, 300)
        let t1 = Tasks.Task.Run(fun () ->
            let ctx1 = HR.GetDataContext()
            let country1 = 
                query {
                    for c in ctx1.Hr.Countries do
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
                    for c in ctx2.Hr.Countries do
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
    for count in ctx.Hr.Countries do
    where (count.CountryName = "Andorra" || count.RegionId = 99934)
} |> Seq.``delete all items from single table`` 
|> Async.RunSynchronously

//******************** Unsigned mapping test **********************//

query {
    for c in ctx.Hr.JobHistory do
    where (c.Years > 3u)
    select c.Years
} |> Seq.tryHead


//******************** MySqlConnector **********************//

// MySqlConnector is unofficial driver, a lot faster execution, better async. 
// Stored-procedures and function calls not supported.

// NOTE: Needs also System.Threading.Tasks.Extensions.dll, System.Buffers.dll and System.Runtime.InteropServices.RuntimeInformation.dll to ResolutionPath

//"../../../packages/standard/System.Buffers/lib/netstandard1.1/System.Buffers.dll"
//"../../../packages/standard/System.Runtime.InteropServices.RuntimeInformation/lib/net45/System.Runtime.InteropServices.RuntimeInformation.dll"
//"../../../packages/standard/System.Threading.Tasks.Extensions/lib/portable-net45+win8+wp8+wpa81/System.Threading.Tasks.Extensions.dll"

[<Literal>]
let connectorPath = __SOURCE_DIRECTORY__ + @"/../../../packages/scripts/MySqlConnector/lib/net46/"
type HRFast = SqlDataProvider<Common.DatabaseProviderTypes.MYSQL, connStr, ResolutionPath = connectorPath, Owner = "HR">
let ctx2 = HRFast.GetDataContext()

let firstNameAsync = 
    query {
        for emp in ctx2.Hr.Employees do
        select (emp.FirstName, emp.LastName, emp.PhoneNumber)
    } |> Seq.executeQueryAsync |> Async.StartAsTask
