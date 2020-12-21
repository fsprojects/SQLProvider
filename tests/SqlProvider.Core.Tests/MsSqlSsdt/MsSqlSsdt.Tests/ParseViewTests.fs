module ParseViewTests
open NUnit.Framework
open FSharp.Data.Sql.Providers
open FSharp.Data.Sql.Providers.MSSqlServerSsdt
open Utils

let employeeDetailsTableSql =
    "CREATE TABLE [dbo].[Employee_Details]
    (  
        [Emp_Id] [int] IDENTITY(1,1) NOT NULL,  
        [Emp_Name] [nvarchar](50) NOT NULL,  
        [Emp_City] [nvarchar](50) NOT NULL,  
        [Emp_Salary] [int] NOT NULL,  
     CONSTRAINT [PK_Employee_Details] PRIMARY KEY CLUSTERED   
       (  
        [Emp_Id] ASC  
       )"

let employeeContactTableSql =
    "CREATE TABLE [dbo].[Employee_Contact]
    (  
        [Emp_Id] [int] NOT NULL,  
        [MobileNo] [nvarchar](50) NOT NULL  
    ) ON [PRIMARY]"

let employeeViewSql =
    "CREATE VIEW [dbo].[v_Employee]
    AS  
    SELECT Employee_Details.Emp_Id, Emp_Name, [DBO].employee_details.emp_salary, [dbo].Employee_Contact.MobileNo
    FROM [dbo].Employee_Details   
    LEFT OUTER JOIN [dbo].Employee_Contact ON
    dbo.Employee_Details.Emp_Id = dbo.Employee_Contact.Emp_Id  
    WHERE dbo.Employee_Details.Emp_Id > 2"

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
    let expected = ["Emp_Id"; "Emp_Name"; "Emp_Salary"; "MobileNo"]
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
