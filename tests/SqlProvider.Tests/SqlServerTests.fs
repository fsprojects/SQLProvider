#if INTERACTIVE
#I @"../../bin/net451"
#r @"../../bin/net451/FSharp.Data.SqlProvider.dll"
#I @"../../packages/scripts/Newtonsoft.Json/lib/net45"
#r @"../../packages/scripts/Newtonsoft.Json/lib/net45/Newtonsoft.Json.dll"
#else
module SqlServerTests
#endif

#if LOCALBUILD
#else

open System
open FSharp.Data.Sql
open System.Data
open NUnit.Framework

#if APPVEYOR
let [<Literal>] connStr = "Data Source=(local)\SQL2008R2SP2;User Id=sa;Password=Password12!; Initial Catalog=sqlprovider;"
#else
let [<Literal>] connStr = "Data Source=localhost; Initial Catalog=sqlprovider; Integrated Security=True"
#endif 
#endif

open System
open FSharp.Data.Sql

[<Literal>]
let resolutionFolder = __SOURCE_DIRECTORY__
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %O")


let processId = System.Diagnostics.Process.GetCurrentProcess().Id;

type HR = SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER, connStr, ResolutionPath = resolutionFolder>

type Employee = {
    EmployeeId : int32
    FirstName : string
    LastName : string
    HireDate : DateTime
}


//***************** Individuals ***********************//
[<Test>]
let ``get individuals``  () =
    let ctx = HR.GetDataContext()
 
  let indv = ctx.Dbo.Employees.Individuals.``As FirstName``.``100, Steven``

  indv.FirstName + " " + indv.LastName + " " + indv.Email
  |> Assert.IsNotNullOrEmpty


//*************** QUERY ************************//
[<Test>]
let employeesFirstName  () =
    let ctx = HR.GetDataContext()
 
  query {
      for emp in ctx.Dbo.Employees do
      select emp.FirstName
  } |> Seq.toList |> Assert.IsNotEmpty

[<Test>]
let employeesFirstNameAsync  () =
    let ctx = HR.GetDataContext()
 
    query {
        for emp in ctx.Dbo.Employees do
        select emp.FirstName
    } |> Seq.executeQueryAsync |> Async.RunSynchronously |> Assert.IsNotEmpty

// Note that Employees-table should have a Description-field in database, visible as XML-tooltip in your IDE.
// Column-level descriptions work also, but they are not included to exported SQL-scripts by SQL-server.

//Ref issue #92
[<Test>]
let employeesFirstNameEmptyList  () =
    let ctx = HR.GetDataContext()
 
    query {
        for emp in ctx.Dbo.Employees do
        where (emp.EmployeeId > 10000)
        select emp
    } |> Seq.toList |> Assert.IsNotEmpty

[<Test>]
let regionsEmptyTable  () =
    let ctx = HR.GetDataContext()
 
    query {
        for r in ctx.Dbo.Regions do
        select r
    } |> Seq.toList |> Assert.IsNotEmpty

//let tableWithNoKey = 
//    query {
//        for r in ctx.Dbo.Table1 do
//        select r.Col
//    } |> Seq.toList |> Assert.IsNotEmpty
//
//let entity = ctx.Table1.Create()
//entity.Col <- 123uy
//ctx.SubmitUpdates()

[<Test>]
let salesNamedDavid  () =
    let ctx = HR.GetDataContext()
 
    query {
            for emp in ctx.Dbo.Employees do
            join d in ctx.Dbo.Departments on (emp.DepartmentId = d.DepartmentId)
            where (d.DepartmentName |=| [|"Sales";"IT"|] && emp.FirstName =% "David")
            select (d.DepartmentName, emp.FirstName, emp.LastName)
            
    } |> Seq.toList |> Assert.IsNotEmpty

[<Test>]
let employeesJob  () =
    let ctx = HR.GetDataContext()
 
    let dbo = ctx.Dbo
    query {
            for emp in dbo.Employees do
            for manager in emp.``dbo.EMPLOYEES by EMPLOYEE_ID`` do
            join dept in dbo.Departments on (emp.DepartmentId = dept.DepartmentId)
            where ((dept.DepartmentName |=| [|"Sales";"Executive"|]) && emp.FirstName =% "David")
            select (emp.FirstName, emp.LastName, manager.FirstName, manager.LastName)
    } |> Seq.toList |> Assert.IsNotEmpty

//Can map SQLEntities to a domain type
let topSales5ByCommission = 
    query {
        for emp in ctx.Dbo.Employees do
        sortByDescending emp.CommissionPct
        select emp
        take 5
    } 
    |> Seq.map (fun e -> e.MapTo<Employee>())
    |> Seq.toList |> Assert.IsNotEmpty

[<Test>]
let pagingTest  () =
    let ctx = HR.GetDataContext()
 
    query {
        for emp in ctx.Dbo.Employees do
        sortByDescending emp.CommissionPct
        select emp
        skip 2
        take 5
    } 
    |> Seq.map (fun e -> e.MapTo<Employee>())
    |> Seq.toList |> Assert.IsNotEmpty

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

[<Test>]
let ``Can customise SQLEntity mapping`` () =
  let ctx = HR.GetDataContext()
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
      |> Seq.toList |> Assert.IsNotEmpty


open System.Linq

[<Test>]
let nestedQueryTest  () =
    let ctx = HR.GetDataContext()
 
    let qry1 = query {
        for emp in ctx.Dbo.Employees do
        where (emp.FirstName.StartsWith("S"))
        select (emp.FirstName)
    }
    query {
        for emp in ctx.Dbo.Employees do
        where (qry1.Contains(emp.FirstName))
        select (emp.FirstName, emp.LastName)
    } |> Seq.toArray |> Assert.IsNotEmpty


[<Test>]
let ``simple math operationsquery`` () =
    let ctx = HR.GetDataContext()

    let itemOf90 = 
        query {
            for p in ctx.Dbo.Departments do
            where (p.DepartmentId - 85 = 5)
            select p.DepartmentId 
        } |> Seq.toList |> Assert.IsNotEmpty

    let ``should be empty``() = 
        query {
            for p in ctx.Dbo.Departments do
            where (p.DepartmentId <> 100 &&  (p.DepartmentId - 100 = 100 - p.DepartmentId))
            select p.DepartmentId 
        } |> Seq.toList |> Assert.IsEmpty
    itemOf90, ``should be empty``


[<Test>]
let canoncicalOpTest  () =
    let ctx = HR.GetDataContext()
 
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
    } |> Seq.toArray |> Assert.IsNotEmpty

//************************ CRUD *************************//


[<Test>]
let ``can successfully update records`` () =
  let ctx = HR.GetDataContext()
 
  let antartica =
    let result =
        query {
            for reg in ctx.Dbo.Regions do
            where (reg.RegionId = 5)
            select reg
        } |> Seq.toList
    result |> Assert.IsNotEmpty |> ignore
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

[<Test>]
let ``can invoke a sproc`` () =
    let ctx = HR.GetDataContext()
 
  ignore <| ctx.Procedures.AddJobHistory.Invoke(100, DateTime(1993, 1, 13), DateTime(1998, 7, 24), "IT_PROG", 60)




//Support for sprocs that return ref cursors
[<Test>]
let employees  () =
    let ctx = HR.GetDataContext()

    [
      for e in ctx.Procedures.GetEmployees.Invoke().ResultSet do
        yield e.MapTo<Employee>()
    ]
    |> Assert.IsNotEmpty

[<Test>]
let employeesAsync  () =
    let ctx = HR.GetDataContext()

    async {
        let! ia = ctx.Procedures.GetEmployees.InvokeAsync()
        return ia.ResultSet
    } |> Async.RunSynchronously |> Assert.IsNotNull

type Region = {
    RegionId : int
    RegionName : string
    RegionDescription : string
}

[<Test>]
let ``Support for MARS procs`` () =
    let ctx = HR.GetDataContext()
   
    let results = ctx.Procedures.GetLocationsAndRegions.Invoke()
    printfn "%A" results.ColumnValues
    [
      for e in results.ResultSet do
        yield e.ColumnValues |> Seq.toList |> box
              
      for e in results.ResultSet_1 do
        yield e.MapTo<Region>() |> box
    ]
    |> Assert.IsNotEmpty

[<Test>]
let ``Support for sprocs that return ref cursors and has in parameters`` () =
  let ctx = HR.GetDataContext()
 
  let getemployees hireDate =
    let results = (ctx.Procedures.GetEmployeesStartingAfter.Invoke hireDate)
    [
      for e in results.ResultSet do
        yield e.MapTo<Employee>()
    ]

  getemployees (new System.DateTime(1999,4,1))

[<Test>]
let ``Distinct alias test`` () =
    let ctx = HR.GetDataContext()
 
    query {
        for emp in ctx.Dbo.Employees do
        sortBy (emp.FirstName)
        select (emp.FirstName, emp.FirstName)
    } |> Seq.toList |> Assert.IsNotEmpty

[<Test>]
let ``Standard deviation test`` () =
    let ctx = HR.GetDataContext()
 
    let salaryStdDev : float = 
      query {
          for emp in ctx.Dbo.Employees do
          select (float emp.Salary)
      } |> Seq.stdDevAsync |> Async.RunSynchronously
    Assert.Greater(salaryStdDev, 0.0)


//******************** Delete all test **********************//
let ``Delte all tests``() = 
  let ctx = HR.GetDataContext()
  query {
      for c in ctx.Dbo.Employees do
      where (c.FirstName = "Tuomas")
  } |> Seq.``delete all items from single table`` 
  |> Async.RunSynchronously
