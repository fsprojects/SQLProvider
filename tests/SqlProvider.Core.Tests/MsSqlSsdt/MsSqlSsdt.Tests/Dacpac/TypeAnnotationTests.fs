module Dacpac.TypeAnnotationTests
open NUnit.Framework
open FSharp.Data.Sql.Providers
open FSharp.Data.Sql.Providers.MSSqlServerSsdt

let sql = """SELECT dbo.Projects.Name AS ProjectName, dbo.Projects.[ProjectNumber] /* int null */, dbo.Projects.ProjectType,
dbo.Projects.LOD, dbo.Projects.[Division], dbo.Projects.IsActive, dbo.ProjectTaskCategories.Name AS Category, 
dbo.ProjectTasks.Name AS TaskName, dbo.ProjectTasks.CostCode, dbo.ProjectTasks.Sort, dbo.TimeEntries.Username, dbo.TimeEntries.Email, dbo.TimeEntries.Date, 
COALESCE (dbo.TimeEntries.Hours, 0) AS Hours /*FLOAT NOT NULL*/,
dbo.TimeEntries.Created/* DatetimeOffset Not Null*/, dbo.TimeEntries.Updated, dbo.Users.EmployeeId"""

[<Test>]
let ``Should find annotation``() =    
    let results = MSSqlServerSsdt.parseCommentAnnotations sql
    printfn "Results: %A" results

    Assert.AreEqual(3, results.Length, "Annotation result count.")

    // #1: dbo.Projects.[ProjectNumber] /* int null */
    // #2: Hours /*FLOAT NOT NULL*/
    // #3: Created/* DatetimeOffset Not Null*/

    let expected = [
        { Column = "ProjectNumber"; DataType = "int"; Nullability = "null" |> Some }
        { Column = "Hours"; DataType = "FLOAT"; Nullability = "NOT NULL"  |> Some }
        { Column = "Created"; DataType = "DatetimeOffset"; Nullability = "Not Null" |> Some }
    ]
    Assert.AreEqual(expected, results)

[<Test>]
let ``Verify float annotation in local dacpac``() =
    let schema = 
        @"C:\_mdsk\CEI.BimHub\Product\Source\CEI.BimHub.DB\bin\Debug\CEI.BimHub.DB.dacpac"
        |> UnzipTests.extractModelXml
        |> MSSqlServerSsdt.parseXml

    let view = schema.Tables |> List.find (fun t -> t.Name = "v_AllTasksLog")
    let c = view.Columns |> List.find (fun c -> c.Name = "Hours")
    Assert.AreEqual("FLOAT", c.DataType)
