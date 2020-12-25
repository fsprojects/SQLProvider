module ParseStoredProcTests
open NUnit.Framework
open FSharp.Data.Sql.Providers
open FSharp.Data.Sql.Providers.MSSqlServerSsdt
open Utils

let uspLogError =
    "CREATE PROCEDURE [dbo].[uspLogError] 
        @ErrorLogID [int] = 0 OUTPUT -- contains the ErrorLogID of the row inserted
    AS                               -- by uspLogError in the ErrorLog table
    BEGIN
        SET NOCOUNT ON;
        SET @ErrorLogID = 0;
        BEGIN TRY
            IF ERROR_NUMBER() IS NULL
                RETURN;
            IF XACT_STATE() = -1
            BEGIN
                PRINT 'Cannot log error since the current transaction is in an uncommittable state.';
                RETURN;
            END    
            INSERT [dbo].[ErrorLog] ([UserName], [ErrorNumber], [ErrorSeverity], [ErrorState], [ErrorProcedure], [ErrorLine], [ErrorMessage]) 
            VALUES (CONVERT(sysname, CURRENT_USER), ERROR_NUMBER(),ERROR_SEVERITY(),ERROR_STATE(),ERROR_PROCEDURE(),ERROR_LINE(),ERROR_MESSAGE());
            SET @ErrorLogID = @@IDENTITY;
        END TRY
        BEGIN CATCH
            PRINT 'An error occurred in stored procedure uspLogError: ';
            EXECUTE [dbo].[uspPrintError];
            RETURN -1;
        END CATCH
    END;"

let selectAllCustomers =
    "CREATE PROCEDURE SelectAllCustomers @City nvarchar(30), @PostalCode nvarchar(10)
    AS
    SELECT * FROM Customers WHERE City = @City AND PostalCode = @PostalCode"

[<Test>]
let ``Print LogError AST``() =
    Utils.printSchemaXml uspLogError

[<Test>]
let ``Print LogError Model``() =
    Utils.printStoredProcModel uspLogError

[<Test>]
let ``Print SelectAllCustomers Model``() =
    Utils.printStoredProcModel selectAllCustomers


