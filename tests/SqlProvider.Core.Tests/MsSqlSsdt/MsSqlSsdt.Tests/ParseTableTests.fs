module ParseTableTests
open NUnit.Framework
open FSharp.Data.Sql.Providers.MSSqlServerSsdt
open Utils

let enrollmentTableScript = 
    "CREATE TABLE [dbo].[Enrollment] (
    [EnrollmentID] INT IDENTITY (1, 1) NOT NULL,
    [Grade]        DECIMAL(3, 2) NULL,
    [CourseID]     INT NOT NULL,
    [StudentID]    INT NOT NULL,
    PRIMARY KEY CLUSTERED ([EnrollmentID] ASC),
    CONSTRAINT [FK_dbo.Enrollment_dbo.Course_CourseID] FOREIGN KEY ([CourseID]) 
        REFERENCES [dbo].[Course] ([CourseID]) ON DELETE CASCADE,
    CONSTRAINT [FK_dbo.Enrollment_dbo.Student_StudentID] FOREIGN KEY ([StudentID]) 
        REFERENCES [dbo].[Student] ([StudentID]) ON DELETE CASCADE
    )"

[<Test>]
let ``Print Enrollment Table AST`` () =
    enrollmentTableScript |> printSchemaXml

[<Test>]
let ``Print Enrollment Table Model`` () =
    enrollmentTableScript |> printTableModel

[<Test>]
let ``Verify Enrollment Table Columns``() =
    let tbl = enrollmentTableScript |> xmlToTableModel
    Assert.AreEqual(
        [{ Name = "EnrollmentID"
           DataType = "INT"
           AllowNulls = false
           Identity = Some { IdentitySpec.Seed = 1; IdentitySpec.Increment = 1 }
           HasDefault = false }
         { Name = "Grade"
           DataType = "DECIMAL"
           AllowNulls = true
           Identity = None
           HasDefault = false }
         { Name = "CourseID"
           DataType = "INT"
           AllowNulls = false
           Identity = None
           HasDefault = false }
         { Name = "StudentID"
           DataType = "INT"
           AllowNulls = false
           Identity = None
           HasDefault = false }]
        , tbl.Columns
    )

[<Test>]
let ``Verify Enrollment PKs``() =
    let tbl = enrollmentTableScript |> xmlToTableModel
    Assert.AreEqual(
        Some ["EnrollmentID"]
        , tbl.PrimaryKey |> Option.map (fun pk -> pk.Columns)
    )
    
[<Test>]
let ``Verify Enrollment FKs``() =
    let tbl = enrollmentTableScript |> xmlToTableModel    
    Assert.AreEqual(
        [{ Name = "FK_dbo.Enrollment_dbo.Course_CourseID"
           Columns = ["CourseID"]
           References = { Schema = "dbo"; Table = "Course"; Columns = ["CourseID"] } }
         { Name = "FK_dbo.Enrollment_dbo.Student_StudentID"
           Columns = ["StudentID"]
           References = { Schema = "dbo"; Table = "Student"; Columns = ["StudentID"] } } ]
        , tbl.ForeignKeys
    )
