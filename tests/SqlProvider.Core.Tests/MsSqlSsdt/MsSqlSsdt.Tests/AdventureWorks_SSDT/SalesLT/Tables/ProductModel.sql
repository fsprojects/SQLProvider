CREATE TABLE [SalesLT].[ProductModel] (
    [ProductModelID]     INT                                                         IDENTITY (1, 1) NOT NULL,
    [Name]               [dbo].[Name]                                                NOT NULL,
    [CatalogDescription] XML(CONTENT [SalesLT].[ProductDescriptionSchemaCollection]) NULL,
    [rowguid]            UNIQUEIDENTIFIER                                            CONSTRAINT [DF_ProductModel_rowguid] DEFAULT (newid()) ROWGUIDCOL NOT NULL,
    [ModifiedDate]       DATETIME                                                    CONSTRAINT [DF_ProductModel_ModifiedDate] DEFAULT (getdate()) NOT NULL,
    CONSTRAINT [PK_ProductModel_ProductModelID] PRIMARY KEY CLUSTERED ([ProductModelID] ASC),
    CONSTRAINT [AK_ProductModel_Name] UNIQUE NONCLUSTERED ([Name] ASC),
    CONSTRAINT [AK_ProductModel_rowguid] UNIQUE NONCLUSTERED ([rowguid] ASC)
);


GO

CREATE PRIMARY XML INDEX [PXML_ProductModel_CatalogDescription]
    ON [SalesLT].[ProductModel]([CatalogDescription])
    WITH (PAD_INDEX = OFF);


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Default constraint value of GETDATE()', @level0type = N'SCHEMA', @level0name = N'SalesLT', @level1type = N'TABLE', @level1name = N'ProductModel', @level2type = N'CONSTRAINT', @level2name = N'DF_ProductModel_ModifiedDate';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Unique nonclustered constraint. Used to support replication samples.', @level0type = N'SCHEMA', @level0name = N'SalesLT', @level1type = N'TABLE', @level1name = N'ProductModel', @level2type = N'CONSTRAINT', @level2name = N'AK_ProductModel_rowguid';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Primary key (clustered) constraint', @level0type = N'SCHEMA', @level0name = N'SalesLT', @level1type = N'TABLE', @level1name = N'ProductModel', @level2type = N'CONSTRAINT', @level2name = N'PK_ProductModel_ProductModelID';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Unique nonclustered constraint.', @level0type = N'SCHEMA', @level0name = N'SalesLT', @level1type = N'TABLE', @level1name = N'ProductModel', @level2type = N'CONSTRAINT', @level2name = N'AK_ProductModel_Name';


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Default constraint value of NEWID()', @level0type = N'SCHEMA', @level0name = N'SalesLT', @level1type = N'TABLE', @level1name = N'ProductModel', @level2type = N'CONSTRAINT', @level2name = N'DF_ProductModel_rowguid';


GO

