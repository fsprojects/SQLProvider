CREATE VIEW [dbo].[v_Employee]
    AS  
    SELECT EmployeeDetails.EmpId, EmpName, EmployeeDetails.EmpSalary, EmployeeContact.MobileNo as MobilePhone
    FROM [dbo].EmployeeDetails   
    LEFT OUTER JOIN [dbo].EmployeeContact ON
    dbo.EmployeeDetails.Emp_Id = dbo.EmployeeContact.EmpId
    WHERE dbo.EmployeeDetails.EmpId > 2
