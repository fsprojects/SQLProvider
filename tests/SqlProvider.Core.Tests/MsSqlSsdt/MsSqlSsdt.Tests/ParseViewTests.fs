module ParseViewTests
open NUnit.Framework
open FSharp.Data.Sql.Providers
open FSharp.Data.Sql.Providers.MSSqlServerSsdt
open Utils

let employeeDetailsTableSql =
    "CREATE TABLE [dbo].[EmployeeDetails] (  
        [EmpId] [int] IDENTITY(1,1) NOT NULL,  
        [EmpName] [nvarchar](50) NOT NULL,  
        [EmpCity] [nvarchar](50) NOT NULL,  
        [EmpSalary] [int] NOT NULL,  
        CONSTRAINT [PK_EmployeeDetails] PRIMARY KEY CLUSTERED ([EmpId] ASC),
        CONSTRAINT [FK_EmployeeContact_EmployeeDetails] FOREIGN KEY ([EmpId]) REFERENCES [dbo].[EmployeeContact] ([Id])
    )"

let employeeContactTableSql =
    "CREATE TABLE [dbo].[EmployeeContact](  
        [EmpId] [int] NOT NULL,  
        [MobileNo] [nvarchar](50) NOT NULL,
        CONSTRAINT [PK_EmployeeContact] PRIMARY KEY CLUSTERED ([EmpId] ASC)
    )"

let employeeViewSql =
    "CREATE VIEW [dbo].[v_Employee]
    AS  
    SELECT EmployeeDetails.EmpId, EmpName, EmployeeDetails.EmpSalary, EmployeeContact.MobileNo as MobilePhone
    FROM [dbo].EmployeeDetails   
    LEFT OUTER JOIN [dbo].EmployeeContact ON
    dbo.EmployeeDetails.Emp_Id = dbo.EmployeeContact.EmpId
    WHERE dbo.EmployeeDetails.EmpId > 2"

[<Test>]
let ``Print Employee View AST`` () =
    Utils.printSchemaXml employeeViewSql

[<Test>]
let ``Print Tbl1``() =
    Utils.printTableModel employeeDetailsTableSql

[<Test>]
let ``Print Tbl2``() =
    Utils.printTableModel employeeContactTableSql

let viewModel =
    let tbl1 = Utils.xmlToTableModel employeeDetailsTableSql
    let tbl2 = Utils.xmlToTableModel employeeContactTableSql

    employeeViewSql
    |> Utils.sqlToSchemaXml
    |> MSSqlServerSsdt.parseViewSchemaXml [tbl1; tbl2]

[<Test>]
let ``Print Employee View Model`` () =
    viewModel |> printfn "%A"

[<Test>]
let ``Verify View Columns`` () =
    let expected = ["EmpId"; "EmpName"; "EmpSalary"; "MobilePhone"]
    Assert.AreEqual(
        expected,
        viewModel.Columns |> List.map (fun c -> c.Name)
    )

[<Test>]
let ``Convert SssdColumn to Column``() =
    let tbl1 = Utils.xmlToTableModel employeeDetailsTableSql

    let cols = 
        tbl1.Columns
        |> List.map (MSSqlServerSsdt.ssdtColumnToColumn tbl1)

    cols |> printfn "%A"

    Assert.IsFalse(cols |> List.exists (fun c -> c = None))
