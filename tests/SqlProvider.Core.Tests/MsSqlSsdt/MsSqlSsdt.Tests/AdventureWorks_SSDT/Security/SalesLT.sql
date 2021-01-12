CREATE SCHEMA [SalesLT]
    AUTHORIZATION [dbo];


GO

EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Contains objects related to products, customers, sales orders, and sales territories.', @level0type = N'SCHEMA', @level0name = N'SalesLT';


GO

