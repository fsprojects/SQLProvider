CREATE TABLE [dbo].[ProjectTasks] (
    [Id]             UNIQUEIDENTIFIER NOT NULL,
    [ProjectId]      UNIQUEIDENTIFIER NOT NULL,
    [ProjectTaskCategoryId] UNIQUEIDENTIFIER NOT NULL ,
    [Name]           VARCHAR (300)    NOT NULL,
    [CostCode] VARCHAR(20) NOT NULL, 
    [Sort]           INT              CONSTRAINT [DF__ProjectTas__Sort__534D60F1] DEFAULT ((0)) NOT NULL,
    CONSTRAINT [PK_ProjectTasks] PRIMARY KEY CLUSTERED ([Id] ASC),
    CONSTRAINT [FK_ProjectTasks_Projects] FOREIGN KEY ([ProjectId]) REFERENCES [dbo].[Projects] ([Id]),
    CONSTRAINT [FK_ProjectTasks_ProjectTaskCategories] FOREIGN KEY ([ProjectTaskCategoryId]) REFERENCES [dbo].[ProjectTaskCategories] ([Id])
);
