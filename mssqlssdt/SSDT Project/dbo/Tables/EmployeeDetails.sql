CREATE TABLE [dbo].[EmployeeDetails] (  
    [EmpId] [int] IDENTITY(1,1) NOT NULL,  
    [EmpName] [nvarchar](50) NOT NULL,  
    [EmpCity] [nvarchar](50) NOT NULL,  
    [EmpSalary] [int] NOT NULL,  
    CONSTRAINT [PK_EmployeeDetails] PRIMARY KEY CLUSTERED ([EmpId] ASC),
    CONSTRAINT [FK_EmployeeContact_EmployeeDetails] FOREIGN KEY ([EmpId]) REFERENCES [dbo].[EmployeeContact] ([Id])
)
