CREATE TABLE [dbo].[Projects] (
    [Id]            UNIQUEIDENTIFIER NOT NULL,
    [Name]          VARCHAR (100)    NOT NULL,
    [ProjectNumber] VARCHAR (50)     NULL,
    [ProjectType]   VARCHAR (50)     NULL,
    [IsActive]      BIT              CONSTRAINT [DF_Projects_IsActive] DEFAULT ((0)) NOT NULL,
    [IsDeleted]     BIT              CONSTRAINT [DF_Projects_IsDeleted] DEFAULT ((0)) NOT NULL,
    CONSTRAINT [PK_Projects] PRIMARY KEY CLUSTERED ([Id] ASC)
);







