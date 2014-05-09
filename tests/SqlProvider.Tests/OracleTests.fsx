#r @"..\..\bin\FSharp.Data.SqlProvider.dll"

open FSharp.Data.Sql

[<Literal>]
let connStr = "Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.56.101)(PORT=1521)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ORCL)));User Id=HR;Password=oracle;"

[<Literal>]
let resolutionFolder = __SOURCE_DIRECTORY__
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %s")

type HR = SqlDataProvider<ConnectionString = connStr, DatabaseVendor = Common.DatabaseProviderTypes.ORACLE, ResolutionPath = resolutionFolder, Owner = "HR">
let ctx = HR.GetDataContext()


let indv = ctx.``[HR].[EMPLOYEES]``.Individuals.``As FIRST_NAME``.``100, Steven``

indv.FIRST_NAME + " " + indv.LAST_NAME + " " + indv.EMAIL

let employeesFirstName = 
    query {
        for emp in ctx.``[HR].[EMPLOYEES]`` do
        where (emp.FIRST_NAME.IsSome)
        select (emp.FIRST_NAME.Value, emp.LAST_NAME)
    } |> Seq.toList

let salesNamedDavid = 
    query {
            for emp in ctx.``[HR].[EMPLOYEES]`` do
            join d in ctx.``[HR].[DEPARTMENTS]`` on (emp.DEPARTMENT_ID.Value = d.DEPARTMENT_ID)
            where (d.DEPARTMENT_NAME |=| [|"Sales";"IT"|] && emp.FIRST_NAME =% "David")
            select (d.DEPARTMENT_NAME, emp.FIRST_NAME, emp.LAST_NAME)
            
    } |> Seq.toList

let employeesJob = 
    query {
            for emp in ctx.``[HR].[EMPLOYEES]`` do
            for manager in emp.EMP_MANAGER_FK do
            join dept in ctx.``[HR].[DEPARTMENTS]`` on (emp.DEPARTMENT_ID.Value = dept.DEPARTMENT_ID)
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

let whiteSpace = 
    query {
        for ws in ctx.``[HR].[WHITE SPACE]`` do
        where (ws.``COLUMN 2`` > 3M)
        select (ws.``COLUMN 1``)
    } |> Seq.toList

let headCount = ctx.``Stored Procedures``.``HR.HR_API.FIRE_EMPLOYEE``(145M)