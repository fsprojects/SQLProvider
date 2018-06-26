#if INTERACTIVE
#r @"../../bin/net451/FSharp.Data.SqlProvider.dll"
#r @"../../packages/NUnit/lib/nunit.framework.dll"
#else
module PostgreSQLTests
#endif

#if LOCALBUILD
#else

// Postgres Npgsql v.3.2.x has internal reference to System.Threading.Tasks.Extensions.dll:
// #r "../../packages/scripts/System.Threading.Tasks.Extensions/lib/portable-net45+win8+wp8+wpa81/System.Threading.Tasks.Extensions.dll"
open System
open FSharp.Data.Sql
open System.Data
open NUnit.Framework

#if TRAVIS
let [<Literal>] connStr = "User ID=postgres;Host=localhost;Port=5432;Database=sqlprovider;"
#else
#if APPVEYOR
let [<Literal>] connStr = "User ID=postgres;Password=Password12!;Host=localhost;Port=5432;Database=sqlprovider;"
#else
let [<Literal>] connStr = "User ID=postgres;Password=postgres;Host=localhost;Port=5432;Database=sqlprovider;"
#endif 
#endif

[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + @"/../../packages/scripts/Npgsql/lib/net451"

FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %O")
FSharp.Data.Sql.Common.QueryEvents.LinqExpressionEvent |> Event.add (printfn "Expression: %A")

let processId = System.Diagnostics.Process.GetCurrentProcess().Id;

type HR = 
  SqlDataProvider<
      DatabaseVendor = Common.DatabaseProviderTypes.POSTGRESQL,
      ConnectionString = connStr,
      ResolutionPath=resolutionPath,
      UseOptionTypes=true,
      Owner = "public, other_schema"
  >

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
[<Test>]
let ``Fetch and print an individual's info``() =
  let ctx = HR.GetDataContext()
  let indv = ctx.Public.Employees.Individuals.``As FirstName``.``100, Steven``
  let info = sprintf "%s %s (%s)" indv.FirstName.Value indv.LastName indv.Email
  Assert.AreEqual(info, "Steven King (SKING)")

//*************** QUERY ************************//

type LocationQuery = {
     City : string
     PostalCode : string option
}

[<Test>]
let ``Find Tokyo location`` () =
    let ctx = HR.GetDataContext()
    
    let locationQuery = { City = "Tokyo"; PostalCode = Some "1689" }

    let result = 
      query {
       for location in ctx.Public.Locations do
       where (location.City = locationQuery.City)
       where (location.PostalCode = locationQuery.PostalCode)
       select location
      } |> Seq.toList

    Assert.AreEqual (result.Length, 1)

    let tokyo = result.Head

    Assert.AreEqual(tokyo.LocationId, 1200)
    Assert.AreEqual(tokyo.StreetAddress, Some "2017 Shinjuku-ku")
    Assert.AreEqual(tokyo.PostalCode, Some "1689")
    Assert.AreEqual(tokyo.City, "Tokyo")
    Assert.AreEqual(tokyo.StateProvince, Some "Tokyo Prefecture")
    Assert.AreEqual(tokyo.CountryId, Some "JP") 

[<Test>]
let employeesFirstNameNoProj () = 
    let ctx = HR.GetDataContext()    
    query {
        for emp in ctx.Public.Employees do
        select true
    } |> Seq.toList |> Assert.IsNotEmpty

[<Test>]
let employeesFirstNameIdProj () = 
    let ctx = HR.GetDataContext() 
    query {
        for emp in ctx.Public.Employees do
        select emp
    } |> Seq.toList |> Assert.IsNotEmpty

[<Test>]
let first10employess () = 
    let ctx = HR.GetDataContext() 
    query {
        for emp in ctx.Public.Employees do
        select emp.EmployeeId
        take 10
    } |> Seq.toList |> Assert.IsNotEmpty

[<Test>]
let skip2first10employess () = 
    let ctx = HR.GetDataContext() 
    query {
        for emp in ctx.Public.Employees do
        select emp.EmployeeId
        skip 2
        take 10
    } |> Seq.toList |> Assert.IsNotEmpty

[<Test>]
let employeesFirstName () = 
    let ctx = HR.GetDataContext() 
    query {
        for emp in ctx.Public.Employees do
        select (emp.FirstName, emp.LastName, emp.Email, emp.SalaryHistory)
    } |> Seq.toList |> Assert.IsNotEmpty

// Note that Employees-table and Email should have a Comment-field in database, visible as XML-tooltip in your IDE.

[<Test>]
let employeesSortByName () = 
    let ctx = HR.GetDataContext() 
    query {
        for emp in ctx.Public.Employees do
        sortBy emp.FirstName
        thenBy emp.LastName
        select (emp.FirstName, emp.LastName)
    } |> Seq.toList |> Assert.IsNotEmpty

[<Test>]
let salesNamedDavid () = 
    let ctx = HR.GetDataContext() 
    query {
            for emp in ctx.Public.Employees do
            join d in ctx.Public.Departments on (emp.DepartmentId = Some(d.DepartmentId))
            where (d.DepartmentName |=| [|"Sales";"IT"|] && emp.FirstName =% "David")
            select (d.DepartmentName, emp.FirstName, emp.LastName)
    } |> Seq.toList |> Assert.IsNotEmpty

[<Test>]
let employeesJob () = 
    let ctx = HR.GetDataContext() 
    query {
            for emp in ctx.Public.Employees do
            for manager in emp.``public.employees by employee_id_1`` do
            join dept in ctx.Public.Departments on (emp.DepartmentId = Some(dept.DepartmentId))
            where ((dept.DepartmentName |=| [|"Sales";"Executive"|]) && emp.FirstName =% "David")
            select (emp.FirstName, emp.LastName, manager.FirstName, manager.LastName )
    } |> Seq.toList |> Assert.IsNotEmpty

//Can map SQLEntities to a domain type
[<Test>]
let topSales5ByCommission () = 
    let ctx = HR.GetDataContext() 
    query {
        for emp in ctx.Public.Employees do
        sortByDescending emp.CommissionPct
        select emp
        take 5
    }
    |> Seq.map (fun e -> e.MapTo<Employee>())
    |> Seq.toList |> Assert.IsNotEmpty

[<Test>]
let canonicalTest () = 
    let ctx = HR.GetDataContext() 
    query {
            for emp in ctx.Public.Employees do
            join d in ctx.Public.Departments on (emp.DepartmentId.Value+1 = d.DepartmentId+1)
            where (abs(d.LocationId.Value) > 1//.value
                && emp.FirstName.Value + "D" = "DavidD"
                && emp.LastName.Length > 6
                && emp.HireDate.Date.AddYears(-10).Year < 1990
                && emp.HireDate.AddDays(1.).Subtract(emp.HireDate).Days = 1
            )
            select (d.DepartmentName, emp.FirstName, emp.LastName, emp.HireDate)
    } |> Seq.toList |> Assert.IsNotEmpty
    
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
[<Test>]
let countries () = 
    let ctx = HR.GetDataContext() 
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
    |> Seq.toList |> Assert.IsNotEmpty

//************************ CRUD *************************//

[<Test>]
let ``Reassign optional and array columns`` () =
    let ctx = HR.GetDataContext() 

    let oldNames = [| "Antarctica"; "South Pole" |]
    let newNames = [| "Antartica"; "Antarctica"; "South Pole" |]

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
          newRegion.RegionAlternateNames <- oldNames
          ctx.SubmitUpdates()
          newRegion
        
    Assert.AreEqual(antartica.RegionName, Some("Antartica"))
    Assert.AreEqual(antartica.RegionAlternateNames, oldNames)

    antartica.RegionName <- Some("ant")
    antartica.RegionAlternateNames <- newNames
    ctx.SubmitUpdates()

    Assert.AreEqual(antartica.RegionName, Some("ant"))
    Assert.AreEqual(antartica.RegionAlternateNames, newNames)

    antartica.Delete()
    ctx.SubmitUpdates()

//********************** Procedures **************************//

[<Test>]
let ``Existing item is successfully deleted, then restored``() = 
    let ctx = HR.GetDataContext() 
    let getIfexisting employeeId startDate = 
        query {
          for x in ctx.Public.JobHistory do
          where ((x.EmployeeId = employeeId) && (x.StartDate = startDate))
          headOrDefault 
        }
        
    let removeIfExists employeeId startDate =      
      let current = getIfexisting employeeId startDate 
      if current <> null then
          current.Delete()
          ctx.SubmitUpdates()

    
    let existing = getIfexisting 100 (DateTime(1993, 1, 13))
    Assert.IsNotNull existing

    let deleted = removeIfExists 100 (DateTime(1993, 1, 13))
    Assert.IsNull deleted

    ctx.Functions.AddJobHistory.Invoke(100, DateTime(1993, 1, 13), DateTime(1998, 7, 24), "IT_PROG", 60)
    let existing = getIfexisting 100 (DateTime(1993, 1, 13))
    Assert.IsNotNull existing
    
//Support for sprocs that return ref cursors
[<Test>]
let employees () = 
    let ctx = HR.GetDataContext() 
    [
      for e in ctx.Functions.GetEmployees.Invoke().ReturnValue do
        yield e.MapTo<Employee>()
    ]
    |> Assert.IsNotEmpty

type Region = {
    RegionId : decimal
    RegionName : string
  //  RegionDescription : string
}

//Support for MARS procs
[<Test>]
let locations_and_regions () = 
    let ctx = HR.GetDataContext() 
    let results = ctx.Functions.GetLocationsAndRegions.Invoke()
    [
      for e in results.ReturnValue do
        yield e.ColumnValues |> Seq.toList |> box
      for e in results.ReturnValue do
        yield e.ColumnValues |> Seq.toList |> box
    ]
    |> Assert.IsNotEmpty

//Support for sprocs that return ref cursors and has in parameters
let getemployees hireDate =
    let ctx = HR.GetDataContext() 
    let results = (ctx.Functions.GetEmployeesStartingAfter.Invoke hireDate)
    [
      for e in results.ReturnValue do
        yield! e.ColumnValues
    ]

[<Test>]
let ``Run a sproc with a parameter returning a ref cursor`` () = 
  let result = getemployees (new System.DateTime(1999,4,1))
  Assert.IsNotNull result
  Assert.IsNotEmpty result

// Support for sprocs that return `table of`
[<Test>]
let ``Return HR info`` () =
    let ctx = HR.GetDataContext() 
    let hrInfo = 
      ctx.Functions.GetDepartments.Invoke().ReturnValue
      |> Array.map (fun e -> e.MapTo<Department>())
      |> sprintf "%A"
    Assert.AreEqual(hrInfo, """[|{DepartmentId = 10;
   DepartmentName = "Administration";}; {DepartmentId = 20;
                                         DepartmentName = "Marketing";};
  {DepartmentId = 30;
   DepartmentName = "Purchasing";}; {DepartmentId = 40;
                                     DepartmentName = "Human Resources";};
  {DepartmentId = 50;
   DepartmentName = "Shipping";}; {DepartmentId = 60;
                                   DepartmentName = "IT";};
  {DepartmentId = 70;
   DepartmentName = "Public Relations";}; {DepartmentId = 80;
                                           DepartmentName = "Sales";};
  {DepartmentId = 90;
   DepartmentName = "Executive";}; {DepartmentId = 100;
                                    DepartmentName = "Finance";};
  {DepartmentId = 110;
   DepartmentName = "Accounting";}; {DepartmentId = 120;
                                     DepartmentName = "Treasury";};
  {DepartmentId = 130;
   DepartmentName = "Corporate Tax";}; {DepartmentId = 140;
                                        DepartmentName = "Control And Credit";};
  {DepartmentId = 150;
   DepartmentName = "Shareholder Services";}; {DepartmentId = 160;
                                               DepartmentName = "Benefits";};
  {DepartmentId = 170;
   DepartmentName = "Manufacturing";}; {DepartmentId = 180;
                                        DepartmentName = "Construction";};
  {DepartmentId = 190;
   DepartmentName = "Contracting";}; {DepartmentId = 200;
                                      DepartmentName = "Operations";};
  {DepartmentId = 210;
   DepartmentName = "IT Support";}; {DepartmentId = 220;
                                     DepartmentName = "NOC";};
  {DepartmentId = 230;
   DepartmentName = "IT Helpdesk";}; {DepartmentId = 240;
                                      DepartmentName = "Government Sales";};
  {DepartmentId = 250;
   DepartmentName = "Retail Sales";}; {DepartmentId = 260;
                                       DepartmentName = "Recruiting";};
  {DepartmentId = 270;
   DepartmentName = "Payroll";}|]""")

//********************** Functions ***************************//

[<Test>]
let fullName () = 
    let ctx = HR.GetDataContext() 
    ctx.Functions.EmpFullname.Invoke(100).ReturnValue
    |> Assert.IsNotNullOrEmpty

//********************** Type test ***************************//
let point (x,y) = NpgsqlTypes.NpgsqlPoint(x,y)
let circle (x,y,r) = NpgsqlTypes.NpgsqlCircle (point (x,y), r)
let path pts = NpgsqlTypes.NpgsqlPath(pts: NpgsqlTypes.NpgsqlPoint [])
let polygon pts = NpgsqlTypes.NpgsqlPolygon(pts: NpgsqlTypes.NpgsqlPoint [])

open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns

[<Test>]
let ``Create and print PostgreSQL specific types``() = 

  let ctx = HR.GetDataContext() 
  let tt = ctx.Public.PostgresqlTypes.Create()

  //tt.Abstime0 <- Some DateTime.Today
  tt.Bigint0 <- Some 100L
  tt.Bigserial0 <- 300L
  //tt.Bit0 <- Some(true)
  tt.Bit0 <- Some(System.Collections.BitArray(10, true))
  tt.BitVarying0 <- Some(System.Collections.BitArray([| true; true; false; false |]))
  tt.Boolean0 <- Some(true)
  //tt.Box0 <- Some(NpgsqlTypes.NpgsqlBox(0.0f, 1.0f, 2.0f, 3.0f))
  tt.Bytea0 <- Some([| 1uy; 10uy |])
  tt.Character0 <- Some("test")
  tt.CharacterVarying0 <- Some("raudpats")
  tt.Cid0 <- Some(87u)
  //tt.Circle0 <- Some(circle(0.0f, 1.0f, 2.0))
  tt.Date0 <- Some(DateTime.Today)
  tt.DoublePrecision0 <- Some(100.0)
  tt.Inet0 <- Some(System.Net.IPAddress.Any)
  tt.Integer0 <- Some(1)
  tt.InternalChar0 <- Some('c')
  tt.Interval0 <- Some(TimeSpan.FromDays(3.0))
  tt.Json0 <- Some("{ }")
  tt.Jsonb0 <- Some(@"{ ""x"": [] }")
  tt.Macaddr0 <- Some(System.Net.NetworkInformation.PhysicalAddress([| 0uy; 0uy; 0uy; 0uy; 0uy; 0uy |]))
  tt.Money0 <- Some(100M)
  tt.Name0 <- Some("name")
  tt.Numeric0 <- Some(99.76M)
  tt.Oid0 <- Some(67u)
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
  //tt.Unknown0 <- Some(box 13)
  tt.Uuid0 <- Some(Guid.NewGuid())
  tt.Xid0 <- Some(15u)
  tt.Xml0 <- Some("xml")


  // Mapping SQL to types originating in Npgsql currently does not work due to type provider SDK issues.
  // See: https://github.com/fsprojects/FSharp.TypeProviders.SDK/issues/173
  //
  //tt.Box0 <- Some(NpgsqlTypes.NpgsqlBox(0.0, 1.0, 2.0, 3.0))
  //tt.Cidr0 <- Some(NpgsqlTypes.NpgsqlInet("0.0.0.0/24"))
  //tt.Circle0 <- Some(NpgsqlTypes.NpgsqlCircle(0.0, 1.0, 2.0))
  //tt.Line0 <- Some(NpgsqlTypes.NpgsqlLine())
  //tt.Lseg0 <- Some(NpgsqlTypes.NpgsqlLSeg())
  //tt.Path0 <- Some(path [| point (0.0, 0.0); point (0.0, 1.0); point (1.0, 0.0) |])
  //tt.Point0 <- Some(point (5.0, 5.0))
  //tt.Polygon0 <- Some(polygon [| point (0.0, 0.0); point (0.0, 1.0); point (1.0, 1.0) |])
  //tt.Tsquery0 <- Some(NpgsqlTypes.NpgsqlTsQuery.Parse("test"))
  //tt.Tsvector0 <- Some(NpgsqlTypes.NpgsqlTsVector.Parse("test"))
  
  ctx.SubmitUpdates()
    
  let ttb = query {
      for t in ctx.Public.PostgresqlTypes do
      where (t.PostgresqlTypesId = tt.PostgresqlTypesId)
      exactlyOne
  }
  Assert.IsNotNull ttb
    
  let printTest (exp: Expr) =
      match exp with
      | Call(_,mi,[Value(name,_)]) ->
          let valof x = mi.Invoke(x, [| name |])
          sprintf "%A: %A => %A" name (valof tt) (valof ttb)
      | _ ->
        sprintf "Invalid expression: %A" exp

  let bigint0 = printTest <@@ tt.Bigint0 @@>    
  let bit0    = printTest <@@ tt.Bit0 @@>       
  let box0    = printTest <@@ tt.Box0 @@>       
  let interva = printTest <@@ tt.Interval0 @@>  
  let jsonb0  = printTest <@@ tt.Jsonb0 @@>     

  Assert.AreEqual(bigint0, """"bigint_0": Some 100L => Some 100L""")
  Assert.AreEqual(bit0   , """"bit_0": Some (seq [true; true; true; true; ...]) => Some (seq [true; true; true; true; ...])""")
  Assert.AreEqual(box0   , """"box_0": <null> => <null>""")
  Assert.AreEqual(interva, """"interval_0": Some 3.00:00:00 => Some 3.00:00:00""")
  Assert.AreEqual(jsonb0 , """"jsonb_0": Some "{ "x": [] }" => Some "{"x": []}"      """.TrimEnd())

//********************** Multiple schemas ***************************//


[<Test>]
let ``Access a different schema than the default one``() = 
  let ctx = HR.GetDataContext()
  
  let testRow = ctx.OtherSchema.TableInOtherSchema.Create()
  testRow.ColumnInOtherSchema <- 42  
  ctx.SubmitUpdates()

//********************** Upsert ***************************//


[<Test>]
let ``Upsert on table with single primary key``() = 
  let ctx = HR.GetDataContext()

  let readGermany =
    query { 
      for country in ctx.Public.Countries do 
      where (country.CountryId = "DE") 
      select country.CountryName.Value
    }
    
  let oldGermany = Seq.head readGermany
  let newGermany = "West " + oldGermany

  let wg = ctx.Public.Countries.Create()
  wg.CountryId <- "DE"
  wg.CountryName <- Some newGermany
  wg.RegionId <- Some 1
  wg.OnConflict <- Common.OnConflict.Update
  ctx.SubmitUpdates()

  let newGermanyDb = Seq.head readGermany

  Assert.AreEqual(newGermany, newGermanyDb)

[<Test>]
let ``Upsert on table with composite primary key``() = 
  let ctx = HR.GetDataContext()
  
  let employeeId, startDate, jobId, oldEndDate =
    query { 
      for jobHistory in ctx.Public.JobHistory do 
      select (jobHistory.EmployeeId, jobHistory.StartDate, jobHistory.JobId, jobHistory.EndDate)
    }
    |> Seq.head
  
  let newEndDate = oldEndDate.AddDays(1.0)
    
  let jh = ctx.Public.JobHistory.Create()
  jh.EmployeeId <- employeeId
  jh.StartDate <- startDate
  jh.EndDate <- newEndDate
  jh.JobId <- jobId

  jh.OnConflict <- Common.OnConflict.Update
  ctx.SubmitUpdates()

  let newEndDateDb =
    query { 
      for jobHistory in ctx.Public.JobHistory do 
      where (jobHistory.EmployeeId = employeeId)
      where (jobHistory.StartDate = startDate)
      select (jobHistory.EndDate)
    }
    |> Seq.head

  Assert.AreEqual(newEndDate, newEndDateDb)

[<Test>]
let ``Upsert with DO NOTHING leaves data unchanged``() = 
  let ctx = HR.GetDataContext()
  
  let readGermanyRegion =
    query { 
      for country in ctx.Public.Countries do 
      where (country.CountryId = "DE") 
      select country.RegionId.Value
    }

  let oldGermanyRegion = Seq.head readGermanyRegion
  let newGermanyRegionThatShouldNotBeSet = oldGermanyRegion + 1

  let wg = ctx.Public.Countries.Create()
  wg.CountryId <- "DE"
  wg.CountryName <- Some "Germany"
  wg.RegionId <- Some newGermanyRegionThatShouldNotBeSet
  wg.OnConflict <- Common.OnConflict.DoNothing 
  ctx.SubmitUpdates()

  let newGermanyRegionDb = Seq.head readGermanyRegion

  Assert.AreEqual(oldGermanyRegion, newGermanyRegionDb)

#endif