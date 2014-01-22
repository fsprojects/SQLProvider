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


let salesNamedDavid = 
    query {
            for emp in ctx.``[HR].[EMPLOYEES]`` do
            join d in ctx.``[HR].[DEPARTMENTS]`` on (emp.DEPARTMENT_ID = d.DEPARTMENT_ID)
            where (d.DEPARTMENT_NAME = "Sales" && emp.FIRST_NAME = "David")
            select (d.DEPARTMENT_NAME, emp.FIRST_NAME, emp.LAST_NAME)
            
    } |> Seq.toList

let employeesJob = 
    query {
            for emp in ctx.``[HR].[EMPLOYEES]`` do
            for manager in emp.EMP_MANAGER_FK do
            join dept in ctx.``[HR].[DEPARTMENTS]`` on (emp.DEPARTMENT_ID = dept.DEPARTMENT_ID)
            where (dept.DEPARTMENT_NAME = "Sales")
            select (emp.FIRST_NAME, emp.LAST_NAME, manager.FIRST_NAME, manager.LAST_NAME )
    } |> Seq.toList
