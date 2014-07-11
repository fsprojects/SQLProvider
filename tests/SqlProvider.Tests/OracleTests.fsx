#r @"..\..\bin\FSharp.Data.SqlProvider.dll"

open System
open FSharp.Data.Sql

[<Literal>]
let connStr = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.56.101)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=pdb1)));User Id=HR;Password=oracle;"

[<Literal>]
let resolutionFolder = __SOURCE_DIRECTORY__
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %s")

let processId = System.Diagnostics.Process.GetCurrentProcess().Id;

type HR = SqlDataProvider<ConnectionString = connStr, DatabaseVendor = Common.DatabaseProviderTypes.ORACLE, ResolutionPath = resolutionFolder, Owner = "HR">
let ctx = HR.GetDataContext()

//***************** Individuals ***********************//
let indv = ctx.``[HR].[EMPLOYEES]``.Individuals.``As FIRST_NAME``.``100, Steven``

indv.FIRST_NAME + " " + indv.LAST_NAME + " " + indv.EMAIL


//*************** QUERY ************************//
let employeesFirstName = 
    query {
        for emp in ctx.``[HR].[EMPLOYEES]`` do
        select (emp.FIRST_NAME, emp.LAST_NAME)
    } |> Seq.toList

let salesNamedDavid = 
    query {
            for emp in ctx.``[HR].[EMPLOYEES]`` do
            join d in ctx.``[HR].[DEPARTMENTS]`` on (emp.DEPARTMENT_ID = d.DEPARTMENT_ID)
            where (d.DEPARTMENT_NAME |=| [|"Sales";"IT"|] && emp.FIRST_NAME =% "David")
            select (d.DEPARTMENT_NAME, emp.FIRST_NAME, emp.LAST_NAME)
            
    } |> Seq.toList

let employeesJob = 
    query {
            for emp in ctx.``[HR].[EMPLOYEES]`` do
            for manager in emp.EMP_MANAGER_FK do
            join dept in ctx.``[HR].[DEPARTMENTS]`` on (emp.DEPARTMENT_ID = dept.DEPARTMENT_ID)
            where ((dept.DEPARTMENT_NAME |=| [|"Sales";"Executive"|]) && emp.FIRST_NAME =% "David")
            select (emp.FIRST_NAME, emp.LAST_NAME, manager.FIRST_NAME, manager.LAST_NAME )
    } |> Seq.toList

let topSales5ByCommission = 
    query {
        for emp in ctx.``[HR].[EMPLOYEES]`` do
        sortByDescending emp.COMMISSION_PCT
        select (emp.EMPLOYEE_ID, emp.FIRST_NAME, emp.LAST_NAME, emp.COMMISSION_PCT)
        take 5
    } |> Seq.toList

//************************ CRUD *************************//


let antartica =
    let result =
        query {
            for reg in ctx.``[HR].[REGIONS]`` do
            where (reg.REGION_ID = 5M)
            select reg
        } |> Seq.toList
    match result with
    | [ant] -> ant
    | _ -> 
        let newRegion = ctx.``[HR].[REGIONS]``.Create() 
        newRegion.REGION_NAME <- "Antartica"
        newRegion.REGION_ID <- 5M
        ctx.SubmitUpdates()
        newRegion

antartica.REGION_NAME <- "ant"
ctx.SubmitUpdates()

antartica.Delete()
ctx.SubmitUpdates()

//********************** Procedures **************************//

ctx.Procedures.ADD_JOB_HISTORY(100M, DateTime(1993, 1, 13), DateTime(1998, 7, 24), "IT_PROG", 60M)

//Support for sprocs with no parameters
ctx.Procedures.SECURE_DML()

//Support for sprocs that return ref cursors
let employees =
    [
      for e in ctx.Procedures.GET_EMPLOYEES() do
        yield e.ColumnValues |> Seq.toList
    ]

//Support for sprocs that return ref cursors and has in parameters
let getemployees hireDate =
    [
      for e in ctx.Procedures.GET_EMPLOYEES_STARTING_AFTER hireDate do
        yield e.ColumnValues |> Seq.toList
    ]

getemployees (new System.DateTime(1999,4,1))

//********************** Functions ***************************//

let fullName = ctx.Functions.EMP_FULLNAME(100M)

//********************** Packaged Procs **********************//

let resultPkg = ctx.Packages.TEST_PACKAGE.INSERT_JOB_HISTORY(100M, DateTime(1993, 1, 13), DateTime(1998, 7, 24), "IT_PROG", 60M)

//********************** Packaged Funcs **********************//

let fullNamPkg = ctx.Packages.TEST_PACKAGE.FULLNAME("Bull", "Colin")