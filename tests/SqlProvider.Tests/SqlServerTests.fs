#if INTERACTIVE
#I @"../../bin/net451"
#r @"../../bin/net451/FSharp.Data.SqlProvider.dll"
#I @"../../packages/scripts/Newtonsoft.Json/lib/net45"
#r @"../../packages/scripts/Newtonsoft.Json/lib/net45/Newtonsoft.Json.dll"

let [<Literal>] connStr2008R2 = "Data Source=localhost; Initial Catalog=sqlprovider; Integrated Security=True"
let [<Literal>] connStr2017 = connStr2008R2

#else
module SqlServerTests

let [<Literal>] connStr2008R2 = "Data Source=(local)\SQL2008R2SP2;User Id=sa;Password=Password12!; Initial Catalog=sqlprovider;"
let [<Literal>] connStr2017 = "Data Source=(local)\SQL2017;User Id=sa;Password=Password12!; Initial Catalog=sqlprovider;"

#endif

// Compile only on AppVeyor
#if APPVEYOR

open System
open FSharp.Data.Sql
open System.Data
open NUnit.Framework


[<Literal>]
let resolutionFolder = __SOURCE_DIRECTORY__

FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %O")

type HR = SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER, connStr2008R2, ResolutionPath = resolutionFolder>

type Employee = {
    EmployeeId : int32
    FirstName : string
    LastName : string
    HireDate : DateTime
}

//***************** API *****************//
[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let ``test that existing overloads aren't broken`` (runtimeConnStr : string)= 
  let customConnStr = runtimeConnStr
  let customResPath = resolutionFolder + "////" // semantically neutral change
  let customTransOpts = { FSharp.Data.Sql.Transactions.TransactionOptions.Default with IsolationLevel = FSharp.Data.Sql.Transactions.IsolationLevel.Chaos } 
  let customCmdTimeout = 999
  let customSelectOps = FSharp.Data.Sql.SelectOperations.DatabaseSide
  // execute no-ops to ensure no exceptions are thrown and that ctx isn't null
  HR.GetDataContext().SubmitUpdates()
  (HR.GetDataContext customConnStr).SubmitUpdates()
  HR.GetDataContext(customConnStr).SubmitUpdates()
  HR.GetDataContext(customConnStr, customResPath).SubmitUpdates()
  HR.GetDataContext(customConnStr, customTransOpts).SubmitUpdates()
  HR.GetDataContext(customConnStr, customResPath, customTransOpts).SubmitUpdates()
  HR.GetDataContext(customConnStr, customCmdTimeout).SubmitUpdates()
  HR.GetDataContext(customConnStr, customResPath, customCmdTimeout).SubmitUpdates()
  HR.GetDataContext(customConnStr, customTransOpts, customCmdTimeout).SubmitUpdates()
  HR.GetDataContext(customConnStr, customResPath, customTransOpts, customCmdTimeout).SubmitUpdates()
  HR.GetDataContext(customTransOpts).SubmitUpdates()
  HR.GetDataContext(customCmdTimeout).SubmitUpdates()
  HR.GetDataContext(customTransOpts, customCmdTimeout).SubmitUpdates()
  HR.GetDataContext(customSelectOps).SubmitUpdates()
  HR.GetDataContext(customConnStr, customSelectOps).SubmitUpdates()
  HR.GetDataContext(customConnStr, customTransOpts, customSelectOps).SubmitUpdates()
  HR.GetDataContext(customConnStr, customCmdTimeout, customSelectOps).SubmitUpdates()
  HR.GetDataContext(customConnStr, customResPath, customTransOpts, customCmdTimeout, customSelectOps).SubmitUpdates()
  Assert.True(true)

//***************** Individuals ***********************//
[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let ``get individuals``  (runtimeConnStr) =
  let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
 
  let indv = ctx.Dbo.Employees.Individuals.``As FirstName``.``100, Steven``

  indv.FirstName + " " + indv.LastName + " " + indv.Email
  |> Assert.IsNotNullOrEmpty


//*************** QUERY ************************//
[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let employeesFirstName  (runtimeConnStr) =
  let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
 
  query {
      for emp in ctx.Dbo.Employees do
      select emp.FirstName
  } 
  |> Seq.toList 
  |> Assert.IsNotEmpty

[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let employeesFirstNameAsync  (runtimeConnStr) =
    let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
 
    query {
        for emp in ctx.Dbo.Employees do
        select emp.FirstName
    }
    |> Seq.executeQueryAsync 
    |> Async.RunSynchronously 
    |> Assert.IsNotEmpty

// Note that Employees-table should have a Description-field in database, visible as XML-tooltip in your IDE.
// Column-level descriptions work also, but they are not included to exported SQL-scripts by SQL-server.

//Ref issue #92
[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let employeesFirstNameEmptyList  (runtimeConnStr) =
    let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
 
    query {
        for emp in ctx.Dbo.Employees do
        where (emp.EmployeeId > 10000)
        select emp
    } 
    |> Seq.toList 
    |> Assert.IsEmpty

[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let regionsEmptyTable  (runtimeConnStr) =
    let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
 
    query {
        for r in ctx.Dbo.Regions do
        select r
    } 
    |> Seq.toList
    |> Assert.IsNotEmpty

//let tableWithNoKey = 
//    query {
//        for r in ctx.Dbo.Table1 do
//        select r.Col
//    } |> Seq.toList |> Assert.IsNotEmpty
//
//let entity = ctx.Table1.Create()
//entity.Col <- 123uy
//ctx.SubmitUpdates()

[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let salesNamedDavid  (runtimeConnStr) =
    let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
 
    query {
            for emp in ctx.Dbo.Employees do
            join d in ctx.Dbo.Departments on (emp.DepartmentId = d.DepartmentId)
            where (d.DepartmentName |=| [|"Sales";"IT"|] && emp.FirstName =% "David")
            select (d.DepartmentName, emp.FirstName, emp.LastName)
            
    } |> Seq.toList |> Assert.IsNotEmpty

[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let employeesJob  (runtimeConnStr) =
    let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
 
    let dbo = ctx.Dbo
    query {
            for emp in dbo.Employees do
            for manager in emp.``dbo.EMPLOYEES by EMPLOYEE_ID`` do
            join dept in dbo.Departments on (emp.DepartmentId = dept.DepartmentId)
            where ((dept.DepartmentName |=| [|"Sales";"Executive"|]) && emp.FirstName =% "Steve%")
            select (emp.FirstName, emp.LastName, manager.FirstName, manager.LastName)
    } 
    |> Seq.toList
    |> Assert.IsNotEmpty

//Can map SQLEntities to a domain type
[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let topSales5ByCommission(runtimeConnStr) = 
    let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
    query {
        for emp in ctx.Dbo.Employees do
        sortByDescending emp.CommissionPct
        select emp
        take 5
    } 
    |> Seq.map (fun e -> e.MapTo<Employee>())
    |> Seq.toList
    |> Assert.IsNotEmpty

[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let ``test that paging works``  (runtimeConnStr) =
    let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
    
    query {
        for emp in ctx.Dbo.Employees do
        sortByDescending emp.CommissionPct
        select emp
        skip 2
        take 5
    } 
    |> Seq.map (fun e -> e.MapTo<Employee>())
    |> Seq.toList 
    |> Assert.IsNotEmpty
    
[<TestCase(connStr2017)>]
let ``test that paging uses OFFSET insted of CTEs in MSSQL2017``  (runtimeConnStr) =
    
    let checkForPaging = Handler<Common.QueryEvents.SqlEventData>(fun _ sqlEventData -> 
        let offsetString = sprintf "OFFSET %i ROWS FETCH NEXT %i ROWS ONLY" 2 5
        let usesOffset = sqlEventData.Command.Contains offsetString
        Assert.True usesOffset
    )

    // add
    FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent.AddHandler checkForPaging

    ``test that paging works``(runtimeConnStr)
    
    // remove
    FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent.RemoveHandler checkForPaging    
    
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

[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let ``Can customise SQLEntity mapping`` (runtimeConnStr) =
  let ctx = HR.GetDataContext(connectionString = runtimeConnStr)  
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

[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let nestedQueryTest  (runtimeConnStr) =
    let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
 
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


[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let ``simple math operations in query `` (runtimeConnStr) =
    let ctx = HR.GetDataContext(connectionString = runtimeConnStr)

    let itemOf90 = 
        query {
            for p in ctx.Dbo.Departments do
            where (p.DepartmentId - 85 = 5)
            select p.DepartmentId 
        } |> Seq.toList 

    itemOf90 |> Assert.IsNotEmpty
    
    let ``should be empty`` = 
        query {
            for p in ctx.Dbo.Departments do
            where (p.DepartmentId <> 100 &&  (p.DepartmentId - 100 = 100 - p.DepartmentId))
            select p.DepartmentId 
        } |> Seq.toList
    
    ``should be empty`` |> Assert.IsEmpty


[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let canoncicalOpTest  (runtimeConnStr) =
    let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
 
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


[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let ``can successfully update records`` (runtimeConnStr) =
  let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
 
  let antarctica =
    let existingAntarctica =
        query {
            for reg in ctx.Dbo.Regions do
            where (reg.RegionId = 5)
            select reg
        } |> Seq.toList   

    match existingAntarctica with
    | [ant] -> ant
    | _ -> 
        let newRegion = ctx.Dbo.Regions.Create() 
        newRegion.RegionName <- "Antarctica"
        newRegion.RegionId <- 5
        ctx.SubmitUpdates()
        newRegion

  antarctica.RegionName <- "ant"
  ctx.SubmitUpdates()

  let newName = 
    query {
            for reg in ctx.Dbo.Regions do
            where (reg.RegionId = 5)
            select (reg.RegionName)
    }
    |> Seq.head

  let nameWasUpdated = (newName = "ant")
  Assert.True(nameWasUpdated) |> ignore

  antarctica.Delete()
  ctx.SubmitUpdatesAsync() |> Async.RunSynchronously  

  let newRecords = 
    query {
        for reg in ctx.Dbo.Regions do
        where (reg.RegionId = 5)
        select reg
    } |> Seq.toList   

  Assert.IsEmpty(newRecords) |> ignore

//********************** Procedures **************************//

[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let ``can invoke a sproc`` (runtimeConnStr) =
  let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
  let year = 1900 + (System.Random().Next(93))
  ignore <| ctx.Procedures.AddJobHistory.Invoke(100, DateTime(year, 1, 13), DateTime(year+5, 7, 24), "IT_PROG", 60)


[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let ``support for sprocs that return ref cursors`` (runtimeConnStr) =
    let ctx = HR.GetDataContext(connectionString = runtimeConnStr)

    [
      for e in ctx.Procedures.GetEmployees.Invoke().ResultSet do
        yield e.MapTo<Employee>()
    ]
    |> Assert.IsNotEmpty

[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let employeesAsync  (runtimeConnStr) =
    let ctx = HR.GetDataContext(connectionString = runtimeConnStr)

    async {
        let! ia = ctx.Procedures.GetEmployees.InvokeAsync()
        return ia.ResultSet
    } |> Async.RunSynchronously |> Assert.IsNotNull

type Region = {
    RegionId : int
    RegionName : string
    RegionDescription : string
}

[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let ``Support for MARS procs`` (runtimeConnStr) =
    let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
   
    let results = ctx.Procedures.GetLocationsAndRegions.Invoke()
    printfn "%A" results.ColumnValues
    [
      for e in results.ResultSet do
        yield e.ColumnValues |> Seq.toList |> box
              
      for e in results.ResultSet_1 do
        yield e.MapTo<Region>() |> box
    ]
    |> Assert.IsNotEmpty

[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let ``Support for sprocs that return ref cursors and has in parameters`` (runtimeConnStr) =
  let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
 
  let getemployees hireDate =
    let results = (ctx.Procedures.GetEmployeesStartingAfter.Invoke hireDate)
    [
      for e in results.ResultSet do
        yield e.MapTo<Employee>()
    ]

  getemployees (new System.DateTime(1999,4,1))
  |> Assert.IsNotEmpty
  

[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let ``Distinct alias test`` (runtimeConnStr) =
    let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
 
    query {
        for emp in ctx.Dbo.Employees do
        sortBy (emp.FirstName)
        select (emp.FirstName, emp.FirstName)
    } |> Seq.toList |> Assert.IsNotEmpty

[<TestCase(connStr2008R2)>]
[<TestCase(connStr2017)>]
let ``Standard deviation test`` (runtimeConnStr) =
    let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
 
    let salaryStdDev : float = 
      query {
          for emp in ctx.Dbo.Employees do
          select (float emp.Salary)
      } |> Seq.stdDevAsync |> Async.RunSynchronously
    Assert.Greater(salaryStdDev, 0.0)


//******************** Delete all test **********************//
let ``Delte all tests``(runtimeConnStr) = 
  let ctx = HR.GetDataContext(connectionString = runtimeConnStr)
  query {
      for c in ctx.Dbo.Employees do
      where (c.FirstName = "Tuomas")
  } |> Seq.``delete all items from single table`` 
  |> Async.RunSynchronously

#else
#endif
