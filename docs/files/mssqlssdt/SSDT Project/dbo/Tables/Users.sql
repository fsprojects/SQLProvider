CREATE TABLE [dbo].[Users] (
    [Email]      VARCHAR (100) NOT NULL,
    [Username]   VARCHAR (100) NOT NULL,
    [EmployeeId] VARCHAR (100) NULL,
    [IsAdmin]    BIT           NOT NULL,
    [IsEnabled]  BIT           NOT NULL,
    CONSTRAINT [PK_Users] PRIMARY KEY CLUSTERED ([Email] ASC)
);





