#I "../../../bin/net451"
#r "../../../bin/net451/FSharp.Data.SqlProvider.dll"
// Postgres Npgsql v.3.2.x has internal reference to System.Threading.Tasks.Extensions.dll:
// #r "../../../packages/scripts/System.Threading.Tasks.Extensions/lib/portable-net45+win8+wp8+wpa81/System.Threading.Tasks.Extensions.dll"
open System
open FSharp.Data.Sql
open System.Data

[<Literal>]
let connStr = "User ID=colinbull;Host=localhost;Port=5432;Database=sqlprovider;"

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + "/../../../packages/scripts/Npgsql/lib/net45"

FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %O")
FSharp.Data.Sql.Common.QueryEvents.LinqExpressionEvent |> Event.add (printfn "Expression: %A")

let processId = System.Diagnostics.Process.GetCurrentProcess().Id;

type HR = SqlDataProvider<Common.DatabaseProviderTypes.POSTGRESQL, connStr, ResolutionPath=resolutionPath, UseOptionTypes=true>
let ctx = HR.GetDataContext()

type Employee = {
    EmployeeId : int32
    FirstName : string
    LastName : string
    HireDate : DateTime
}

type Department = {
    DepartmentId: int
    DepartmentName: string
}

//***************** Individuals ***********************//
let indv = ctx.Public.Employees.Individuals.``As FirstName``.``100, Steven``
printfn "%s %s (%s)" indv.FirstName.Value indv.LastName indv.Email

//*************** QUERY ************************//

type LocationQuery = {
     City : string
     PostalCode : string option
}

let locationBy (loc:LocationQuery) =
    query {
       for location in ctx.Public.Locations do
       where (location.City = loc.City)
       where (location.PostalCode = loc.PostalCode)
       select location
    } |> Seq.toList

let result = locationBy { City = "Tokyo"; PostalCode = Some "1689" }

let employeesFirstNameNoProj =
    query {
        for emp in ctx.Public.Employees do
        select true
    } |> Seq.toList

let employeesFirstNameIdProj =
    query {
        for emp in ctx.Public.Employees do
        select emp
    } |> Seq.toList

let first10employess =
    query {
        for emp in ctx.Public.Employees do
        select emp.EmployeeId
        take 10
    } |> Seq.toList

let skip2first10employess =
    query {
        for emp in ctx.Public.Employees do
        select emp.EmployeeId
        skip 2
        take 10
    } |> Seq.toList

let employeesFirstName =
    query {
        for emp in ctx.Public.Employees do
        select (emp.FirstName, emp.LastName, emp.Email, emp.SalaryHistory)
    } |> Seq.toList

// Note that Employees-table and Email should have a Comment-field in database, visible as XML-tooltip in your IDE.

let employeesSortByName =
    query {
        for emp in ctx.Public.Employees do
        sortBy emp.FirstName
        thenBy emp.LastName
        select (emp.FirstName, emp.LastName)
    } |> Seq.toList

let salesNamedDavid =
    query {
            for emp in ctx.Public.Employees do
            join d in ctx.Public.Departments on (emp.DepartmentId = Some(d.DepartmentId))
            where (d.DepartmentName |=| [|"Sales";"IT"|] && emp.FirstName =% "David")
            select (d.DepartmentName, emp.FirstName, emp.LastName)
    } |> Seq.toList

let employeesJob =
    query {
            for emp in ctx.Public.Employees do
            for manager in emp.``public.employees by employee_id_1`` do
            join dept in ctx.Public.Departments on (emp.DepartmentId = Some(dept.DepartmentId))
            where ((dept.DepartmentName |=| [|"Sales";"Executive"|]) && emp.FirstName =% "David")
            select (emp.FirstName, emp.LastName, manager.FirstName, manager.LastName )
    } |> Seq.toList

//Can map SQLEntities to a domain type
let topSales5ByCommission =
    query {
        for emp in ctx.Public.Employees do
        sortByDescending emp.CommissionPct
        select emp
        take 5
    }
    |> Seq.map (fun e -> e.MapTo<Employee>())
    |> Seq.toList

let canonicalTest =
    query {
            for emp in ctx.Public.Employees do
            join d in ctx.Public.Departments on (emp.DepartmentId.Value+1 = d.DepartmentId+1)
            where (abs(d.LocationId.Value) > 1//.value
                && emp.FirstName.Value + "D" = "DavidD"
                && emp.LastName.Length > 6
                && emp.HireDate.Date.AddYears(-10).Year < 1990
            )
            select (d.DepartmentName, emp.FirstName, emp.LastName, emp.HireDate)
    } |> Seq.toList


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
        for emp in ctx.Public.Countries do
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
            for reg in ctx.Public.Regions do
            where (reg.RegionId = 5)
            select reg
        } |> Seq.toList
    match result with
    | [ant] -> ant
    | _ ->
        let newRegion = ctx.Public.Regions.Create()
        newRegion.RegionName <- Some("Antartica")
        newRegion.RegionId <- 5
        newRegion.RegionAlternateNames <- [| "Antarctica"; "South Pole" |]
        ctx.SubmitUpdates()
        newRegion

antartica.RegionName <- Some("ant")
antartica.RegionAlternateNames <- [| "Antartica"; "Antarctica"; "South Pole" |]
ctx.SubmitUpdates()

antartica.Delete()
ctx.SubmitUpdates()

//********************** Procedures **************************//

let removeIfExists employeeId startDate =
    let existing = query { for x in ctx.Public.JobHistory do
                           where ((x.EmployeeId = employeeId) && (x.StartDate = startDate))
                           headOrDefault }
    if existing <> null then
        existing.Delete()
        ctx.SubmitUpdates()

removeIfExists 100 (DateTime(1993, 1, 13))
ctx.Functions.AddJobHistory.Invoke(100, DateTime(1993, 1, 13), DateTime(1998, 7, 24), "IT_PROG", 60)

//Support for sprocs that return ref cursors
let employees =
    [
      for e in ctx.Functions.GetEmployees.Invoke().ReturnValue do
        yield e.MapTo<Employee>()
    ]

type Region = {
    RegionId : decimal
    RegionName : string
  //  RegionDescription : string
}

//Support for MARS procs
let locations_and_regions =
    let results = ctx.Functions.GetLocationsAndRegions.Invoke()
    [
      for e in results.ReturnValue do
        yield e.ColumnValues |> Seq.toList |> box
      for e in results.ReturnValue do
        yield e.ColumnValues |> Seq.toList |> box
    ]

//Support for sprocs that return ref cursors and has in parameters
let getemployees hireDate =
    let results = (ctx.Functions.GetEmployeesStartingAfter.Invoke hireDate)
    [
      for e in results.ReturnValue do
        yield! e.ColumnValues
    ]

getemployees (new System.DateTime(1999,4,1))

// Support for sprocs that return `table of`
ctx.Functions.GetDepartments.Invoke().ReturnValue
|> Array.map (fun e -> e.MapTo<Department>())
|> printfn "%A"

//********************** Functions ***************************//

let fullName = ctx.Functions.EmpFullname.Invoke(100).ReturnValue

//********************** Type test ***************************//

#r "../../../packages/scripts/Npgsql/lib/net45/Npgsql.dll"

let point (x,y) = NpgsqlTypes.NpgsqlPoint(x,y)
let circle (x,y,r) = NpgsqlTypes.NpgsqlCircle (point (x,y), r)
let path pts = NpgsqlTypes.NpgsqlPath(pts: NpgsqlTypes.NpgsqlPoint [])
let polygon pts = NpgsqlTypes.NpgsqlPolygon(pts: NpgsqlTypes.NpgsqlPoint [])

let tt = ctx.Public.PostgresqlTypes.Create()
//tt.Abstime0 <- Some DateTime.Today
tt.Bigint0 <- Some 100L
tt.Bigserial0 <- 300L
//tt.Bit0 <- Some(true)
tt.Bit0 <- Some(System.Collections.BitArray(10, true))
tt.BitVarying0 <- Some(System.Collections.BitArray([| true; true; false; false |]))
tt.Boolean0 <- Some(true)
tt.Box0 <- Some(NpgsqlTypes.NpgsqlBox(0.0, 1.0, 2.0, 3.0))
//tt.Box0 <- Some(NpgsqlTypes.NpgsqlBox(0.0f, 1.0f, 2.0f, 3.0f))
tt.Bytea0 <- Some([| 1uy; 10uy |])
tt.Character0 <- Some("test")
tt.CharacterVarying0 <- Some("raudpats")
tt.Cid0 <- Some(87u)
tt.Cidr0 <- Some(NpgsqlTypes.NpgsqlInet("0.0.0.0/24"))
tt.Circle0 <- Some(NpgsqlTypes.NpgsqlCircle(0.0, 1.0, 2.0))
//tt.Circle0 <- Some(circle(0.0f, 1.0f, 2.0))
tt.Date0 <- Some(DateTime.Today)
tt.DoublePrecision0 <- Some(100.0)
tt.Inet0 <- Some(System.Net.IPAddress.Any)
tt.Integer0 <- Some(1)
tt.InternalChar0 <- Some('c')
tt.Interval0 <- Some(TimeSpan.FromDays(3.0))
tt.Json0 <- Some("{ }")
tt.Jsonb0 <- Some(@"{ ""x"": [] }")
tt.Line0 <- Some(NpgsqlTypes.NpgsqlLine())
tt.Lseg0 <- Some(NpgsqlTypes.NpgsqlLSeg())
tt.Macaddr0 <- Some(System.Net.NetworkInformation.PhysicalAddress([| 0uy; 0uy; 0uy; 0uy; 0uy; 0uy |]))
tt.Money0 <- Some(100M)
tt.Name0 <- Some("name")
tt.Numeric0 <- Some(99.76M)
tt.Oid0 <- Some(67u)
tt.Path0 <- Some(path [| point (0.0, 0.0); point (0.0, 1.0); point (1.0, 0.0) |])
tt.Point0 <- Some(point (5.0, 5.0))
tt.Polygon0 <- Some(polygon [| point (0.0, 0.0); point (0.0, 1.0); point (1.0, 1.0) |])
tt.Real0 <- Some(0.8f)
tt.Regtype0 <- Some(77u)
tt.Smallint0 <- Some(9000s)
tt.Smallserial0 <- 678s
tt.Serial0 <- 77
tt.Text0 <- Some("kesine")
tt.Time0 <- Some(TimeSpan.FromMinutes(15.0))
tt.Time0 <- Some(TimeSpan.FromMinutes(15.0))
tt.Timetz0 <- Some(DateTimeOffset.Now)
//tt.Timetz0 <- Some(NpgsqlTypes.NpgsqlTimeTZ.Now)
tt.Timestamp0 <- Some(DateTime.Now)
tt.Timestamptz0 <- Some(DateTime.Now)
tt.Tsquery0 <- Some(NpgsqlTypes.NpgsqlTsQuery.Parse("test"))
tt.Tsvector0 <- Some(NpgsqlTypes.NpgsqlTsVector.Parse("test"))
//tt.Unknown0 <- Some(box 13)
tt.Uuid0 <- Some(Guid.NewGuid())
tt.Xid0 <- Some(15u)
tt.Xml0 <- Some("xml")

ctx.SubmitUpdates()

let ttb =
    query {
        for t in ctx.Public.PostgresqlTypes do
        where (t.PostgresqlTypesId = tt.PostgresqlTypesId)
        exactlyOne
    }

open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns

let printTest (exp: Expr) =
    match exp with
    | Call(_,mi,[Value(name,_)]) ->
        let valof x = mi.Invoke(x, [| name |])
        printfn "%A: %A => %A" name (valof tt) (valof ttb)
    | _ -> ()

printTest <@@ tt.Bigint0 @@>
printTest <@@ tt.Bit0 @@>
printTest <@@ tt.Box0 @@>
printTest <@@ tt.Interval0 @@>
printTest <@@ tt.Jsonb0 @@>
