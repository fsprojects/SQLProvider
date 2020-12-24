CREATE TABLE [dbo].[EmployeeContact](  
    [EmpId] [int] NOT NULL,  
    [MobileNo] [nvarchar](50) NOT NULL,
    CONSTRAINT [PK_EmployeeContact] PRIMARY KEY CLUSTERED ([EmpId] ASC)
)