CREATE TABLE [dbo].[ProjectTaskCategories] (
    [Id]        UNIQUEIDENTIFIER NOT NULL,
    [ParentId]  UNIQUEIDENTIFIER NULL,
    [ProjectId]  UNIQUEIDENTIFIER NOT NULL,
    [Name]      VARCHAR (300)    NOT NULL,
    [SortOrder] INT              NOT NULL DEFAULT 0,
    CONSTRAINT [PK_ProjectTaskCategories] PRIMARY KEY CLUSTERED ([Id] ASC),
    CONSTRAINT [FK_ProjectTaskCategories_Projects] FOREIGN KEY ([ProjectId]) REFERENCES [dbo].[Projects] ([Id]),
    CONSTRAINT [FK_ProjectTaskCategories_ProjectTaskCategories] FOREIGN KEY ([ParentId]) REFERENCES [dbo].[ProjectTaskCategories] ([Id])
);
