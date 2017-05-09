/*
Generated at 09/05/2017 10:13:21 by Interbase DataPump v 3.4
Please visit IB DataPump home/support page at http://www.clevercomponents.com

To execute this script:
1. Run IBConsole (or simular Interbase/Firebird tool which can execute srcipts)
2. Go to menu Tools->Interactive SQL 
3. Now in Interactive SQL window menu Query->Load Script select script created by IB DataPump
4. Once script loaded go to menu Query->Execute.
*/

 /* Select Option - select * from table */
 /* Option "Copy Indexes and Primary Constraints" is ON */
 /* Option "Create Generators For AutoInc Fields" is ON */

/* For Interbase versions less than 6.0 you might need to delete next line. */
SET SQL DIALECT 1;

CREATE DATABASE 'D:\Gibran\Projetos\GitRep\SQLProvider\tests\SqlProvider.Tests\scripts\NORTHWINDFBD1.FDB' USER 'SYSDBA' PASSWORD 'masterkey';

CREATE DOMAIN T_YESNO AS CHAR(1) DEFAULT 'N' CHECK((VALUE IS NULL) OR (VALUE IN ('N','Y')));

/* Original table name is "Categories" */
CREATE TABLE CATEGORIES (
  CATEGORYID INTEGER /* "CategoryID" */ ,
  CATEGORYNAME VARCHAR(15) /* "CategoryName" */ ,
  DESCRIPTION VARCHAR(255) /* "Description" */ ,
  PICTURE BLOB /* "Picture" */
);

    /* Indexes for table "Categories" */
    /* Found Index "sqlite_autoindex_0", Options [ixUnique] */
    CREATE UNIQUE INDEX SQLITE_AUTOINDEX_0 ON CATEGORIES CATEGORYID;
    /* Found Index "IX_Categories_CategoryName", Options [] */
    CREATE INDEX IX_CATEGORIES_CATEGORYNAME ON CATEGORIES CATEGORYNAME;

    /* Generators for AutoInc fields for table "Categories" */
    /* SELECT  max(Categories.CategoryID) FROM Categories */

    CREATE GENERATOR GEN_CATEGORIES_CATEGORYID;
        SET GENERATOR GEN_CATEGORIES_CATEGORYID TO 8;

    SET TERM ^;
    CREATE TRIGGER TRIG_CATEGORIES_BI FOR CATEGORIES BEFORE INSERT
    AS BEGIN
      IF(NEW.CATEGORYID IS NULL) THEN NEW.CATEGORYID = GEN_ID(GEN_CATEGORIES_CATEGORYID,1);
    END ^
    SET TERM ;^

/* Original table name is "Customers" */
CREATE TABLE CUSTOMERS (
  CUSTOMERID VARCHAR(5) /* "CustomerID" */ ,
  COMPANYNAME VARCHAR(40) /* "CompanyName" */ ,
  CONTACTNAME VARCHAR(30) /* "ContactName" */ ,
  CONTACTTITLE VARCHAR(30) /* "ContactTitle" */ ,
  ADDRESS VARCHAR(60) /* "Address" */ ,
  CITY VARCHAR(15) /* "City" */ ,
  REGION VARCHAR(15) /* "Region" */ ,
  POSTALCODE VARCHAR(10) /* "PostalCode" */ ,
  COUNTRY VARCHAR(15) /* "Country" */ ,
  PHONE VARCHAR(24) /* "Phone" */ ,
  FAX VARCHAR(24) /* "Fax" */
);

    /* Indexes for table "Customers" */
    /* Found Index "IX_Customers_Region", Options [] */
    CREATE INDEX IX_CUSTOMERS_REGION ON CUSTOMERS REGION;
    /* Found Index "IX_Customers_PostalCode", Options [] */
    CREATE INDEX IX_CUSTOMERS_POSTALCODE ON CUSTOMERS POSTALCODE;
    /* Found Index "IX_Customers_CompanyName", Options [] */
    CREATE INDEX IX_CUSTOMERS_COMPANYNAME ON CUSTOMERS COMPANYNAME;
    /* Found Index "IX_Customers_City", Options [] */
    CREATE INDEX IX_CUSTOMERS_CITY ON CUSTOMERS CITY;
    /* Found Index "sqlite_autoindex_Customers_1", Options [ixUnique] */
    CREATE UNIQUE INDEX SQLITE_AUTOINDEX_CUSTOMERS_1 ON CUSTOMERS CUSTOMERID;

/* Original table name is "Employees" */
CREATE TABLE EMPLOYEES (
  EMPLOYEEID INTEGER /* "EmployeeID" */ ,
  LASTNAME VARCHAR(20) /* "LastName" */ ,
  FIRSTNAME VARCHAR(10) /* "FirstName" */ ,
  TITLE VARCHAR(30) /* "Title" */ ,
  TITLEOFCOURTESY VARCHAR(25) /* "TitleOfCourtesy" */ ,
  BIRTHDATE DATE /* "BirthDate" */ ,
  HIREDATE DATE /* "HireDate" */ ,
  ADDRESS VARCHAR(60) /* "Address" */ ,
  CITY VARCHAR(15) /* "City" */ ,
  REGION VARCHAR(15) /* "Region" */ ,
  POSTALCODE VARCHAR(10) /* "PostalCode" */ ,
  COUNTRY VARCHAR(15) /* "Country" */ ,
  HOMEPHONE VARCHAR(24) /* "HomePhone" */ ,
  EXTENSION CHAR(4) /* "Extension" */ ,
  PHOTO VARCHAR(255) /* "Photo" */ ,
  NOTES VARCHAR(255) /* "Notes" */ ,
  PHOTOPATH VARCHAR(255) /* "PhotoPath" */
);

    /* Indexes for table "Employees" */
    /* Found Index "sqlite_autoindex_0", Options [ixUnique] */
    /* !  Warning: Duplicated Index Name Found - sqlite_autoindex_0 , Replaced To SQLITE_AUTOINDEX_01 */
    CREATE UNIQUE INDEX SQLITE_AUTOINDEX_01 ON EMPLOYEES EMPLOYEEID;
    /* Found Index "IX_Employees_PostalCode", Options [] */
    CREATE INDEX IX_EMPLOYEES_POSTALCODE ON EMPLOYEES POSTALCODE;
    /* Found Index "IX_Employees_LastName", Options [] */
    CREATE INDEX IX_EMPLOYEES_LASTNAME ON EMPLOYEES LASTNAME;

    /* Generators for AutoInc fields for table "Employees" */
    /* SELECT  max(Employees.EmployeeID) FROM Employees */

    CREATE GENERATOR GEN_EMPLOYEES_EMPLOYEEID;
        SET GENERATOR GEN_EMPLOYEES_EMPLOYEEID TO 9;

    SET TERM ^;
    CREATE TRIGGER TRIG_EMPLOYEES_BI FOR EMPLOYEES BEFORE INSERT
    AS BEGIN
      IF(NEW.EMPLOYEEID IS NULL) THEN NEW.EMPLOYEEID = GEN_ID(GEN_EMPLOYEES_EMPLOYEEID,1);
    END ^
    SET TERM ;^

/* Original table name is "EmployeesTerritories" */
CREATE TABLE EMPLOYEESTERRITORIES (
  EMPLOYEEID INTEGER /* "EmployeeID" */ ,
  TERRITORYID INTEGER /* "TerritoryID" */
);

    /* Indexes for table "EmployeesTerritories" */
    /* Found Index "sqlite_autoindex_EmployeesTerritories_1", Options [ixUnique] */
    CREATE UNIQUE INDEX SQLITE_AUTOINDEX_EMPLOYEESTERRITORIES_1 ON EMPLOYEESTERRITORIES EMPLOYEEID;

/* Original table name is "InternationalOrders" */
CREATE TABLE INTERNATIONALORDERS (
  ORDERID INTEGER /* "OrderID" */ ,
  CUSTOMSDESCRIPTION VARCHAR(100) /* "CustomsDescription" */ ,
  EXCISETAX VARCHAR(255) /* "ExciseTax" */
);

    /* Indexes for table "InternationalOrders" */
    /* Found Index "sqlite_autoindex_0", Options [ixUnique] */
    /* !  Warning: Duplicated Index Name Found - sqlite_autoindex_0 , Replaced To SQLITE_AUTOINDEX_02 */
    CREATE UNIQUE INDEX SQLITE_AUTOINDEX_02 ON INTERNATIONALORDERS ORDERID;

/* Original table name is "OrderDetails" */
CREATE TABLE ORDERDETAILS (
  ORDERID INTEGER /* "OrderID" */ ,
  PRODUCTID INTEGER /* "ProductID" */ ,
  UNITPRICE VARCHAR(255) /* "UnitPrice" */ ,
  QUANTITY SMALLINT /* "Quantity" */ ,
  DISCOUNT FLOAT /* "Discount" */
);

    /* Indexes for table "OrderDetails" */
    /* Found Index "IX_OrderDetails_ProductsOrder_Details", Options [] */
    CREATE INDEX IX_ORDERDETAILS_PRODUCTSORDER_DETAILS ON ORDERDETAILS PRODUCTID;
    /* Found Index "IX_OrderDetails_ProductID", Options [] */
    CREATE INDEX IX_ORDERDETAILS_PRODUCTID ON ORDERDETAILS PRODUCTID;
    /* Found Index "IX_OrderDetails_OrdersOrder_Details", Options [] */
    CREATE INDEX IX_ORDERDETAILS_ORDERSORDER_DETAILS ON ORDERDETAILS ORDERID;
    /* Found Index "IX_OrderDetails_OrderID", Options [] */
    CREATE INDEX IX_ORDERDETAILS_ORDERID ON ORDERDETAILS ORDERID;
    /* Found Index "sqlite_autoindex_OrderDetails_1", Options [ixUnique] */
    CREATE UNIQUE INDEX SQLITE_AUTOINDEX_ORDERDETAILS_1 ON ORDERDETAILS ORDERID;

/* Original table name is "Orders" */
CREATE TABLE ORDERS (
  ORDERID INTEGER /* "OrderID" */ ,
  CUSTOMERID VARCHAR(5) /* "CustomerID" */ ,
  EMPLOYEEID INTEGER /* "EmployeeID" */ ,
  ORDERDATE DATE /* "OrderDate" */ ,
  REQUIREDDATE DATE /* "RequiredDate" */ ,
  SHIPPEDDATE DATE /* "ShippedDate" */ ,
  FREIGHT VARCHAR(255) /* "Freight" */ ,
  SHIPNAME VARCHAR(40) /* "ShipName" */ ,
  SHIPADDRESS VARCHAR(60) /* "ShipAddress" */ ,
  SHIPCITY VARCHAR(15) /* "ShipCity" */ ,
  SHIPREGION VARCHAR(15) /* "ShipRegion" */ ,
  SHIPPOSTALCODE VARCHAR(10) /* "ShipPostalCode" */ ,
  SHIPCOUNTRY VARCHAR(15) /* "ShipCountry" */
);

    /* Indexes for table "Orders" */
    /* Found Index "sqlite_autoindex_0", Options [ixUnique] */
    /* !  Warning: Duplicated Index Name Found - sqlite_autoindex_0 , Replaced To SQLITE_AUTOINDEX_03 */
    CREATE UNIQUE INDEX SQLITE_AUTOINDEX_03 ON ORDERS ORDERID;
    /* Found Index "IX_Orders_ShipPostalCode", Options [] */
    CREATE INDEX IX_ORDERS_SHIPPOSTALCODE ON ORDERS SHIPPOSTALCODE;
    /* Found Index "IX_Orders_ShippedDate", Options [] */
    CREATE INDEX IX_ORDERS_SHIPPEDDATE ON ORDERS SHIPPEDDATE;
    /* Found Index "IX_Orders_OrderDate", Options [] */
    CREATE INDEX IX_ORDERS_ORDERDATE ON ORDERS ORDERDATE;
    /* Found Index "IX_Orders_EmployeesOrders", Options [] */
    CREATE INDEX IX_ORDERS_EMPLOYEESORDERS ON ORDERS EMPLOYEEID;
    /* Found Index "IX_Orders_EmployeeID", Options [] */
    CREATE INDEX IX_ORDERS_EMPLOYEEID ON ORDERS EMPLOYEEID;
    /* Found Index "IX_Orders_CustomersOrders", Options [] */
    CREATE INDEX IX_ORDERS_CUSTOMERSORDERS ON ORDERS CUSTOMERID;
    /* Found Index "IX_Orders_CustomerID", Options [] */
    CREATE INDEX IX_ORDERS_CUSTOMERID ON ORDERS CUSTOMERID;

    /* Generators for AutoInc fields for table "Orders" */
    /* SELECT  max(Orders.OrderID) FROM Orders */

    CREATE GENERATOR GEN_ORDERS_ORDERID;
        SET GENERATOR GEN_ORDERS_ORDERID TO 11077;

    SET TERM ^;
    CREATE TRIGGER TRIG_ORDERS_BI FOR ORDERS BEFORE INSERT
    AS BEGIN
      IF(NEW.ORDERID IS NULL) THEN NEW.ORDERID = GEN_ID(GEN_ORDERS_ORDERID,1);
    END ^
    SET TERM ;^

/* Original table name is "PreviousEmployees" */
CREATE TABLE PREVIOUSEMPLOYEES (
  EMPLOYEEID INTEGER /* "EmployeeID" */ ,
  LASTNAME VARCHAR(20) /* "LastName" */ ,
  FIRSTNAME VARCHAR(10) /* "FirstName" */ ,
  TITLE VARCHAR(30) /* "Title" */ ,
  TITLEOFCOURTESY VARCHAR(25) /* "TitleOfCourtesy" */ ,
  BIRTHDATE DATE /* "BirthDate" */ ,
  HIREDATE DATE /* "HireDate" */ ,
  ADDRESS VARCHAR(60) /* "Address" */ ,
  CITY VARCHAR(15) /* "City" */ ,
  REGION VARCHAR(15) /* "Region" */ ,
  POSTALCODE VARCHAR(10) /* "PostalCode" */ ,
  COUNTRY VARCHAR(15) /* "Country" */ ,
  HOMEPHONE VARCHAR(24) /* "HomePhone" */ ,
  EXTENSION CHAR(4) /* "Extension" */ ,
  PHOTO VARCHAR(255) /* "Photo" */ ,
  NOTES VARCHAR(255) /* "Notes" */ ,
  PHOTOPATH VARCHAR(255) /* "PhotoPath" */
);

    /* Indexes for table "PreviousEmployees" */
    /* Found Index "sqlite_autoindex_0", Options [ixUnique] */
    /* !  Warning: Duplicated Index Name Found - sqlite_autoindex_0 , Replaced To SQLITE_AUTOINDEX_04 */
    CREATE UNIQUE INDEX SQLITE_AUTOINDEX_04 ON PREVIOUSEMPLOYEES EMPLOYEEID;

/* Original table name is "Products" */
CREATE TABLE PRODUCTS (
  PRODUCTID INTEGER /* "ProductID" */ ,
  PRODUCTNAME VARCHAR(40) /* "ProductName" */ ,
  SUPPLIERID INTEGER /* "SupplierID" */ ,
  CATEGORYID INTEGER /* "CategoryID" */ ,
  QUANTITYPERUNIT VARCHAR(20) /* "QuantityPerUnit" */ ,
  UNITPRICE VARCHAR(255) /* "UnitPrice" */ ,
  UNITSINSTOCK SMALLINT /* "UnitsInStock" */ ,
  UNITSONORDER SMALLINT /* "UnitsOnOrder" */ ,
  REORDERLEVEL SMALLINT /* "ReorderLevel" */ ,
  DISCONTINUED T_YESNO /* "Discontinued" */ ,
  DISCONTINUEDDATE DATE /* "DiscontinuedDate" */
);

    /* Indexes for table "Products" */
    /* Found Index "sqlite_autoindex_0", Options [ixUnique] */
    /* !  Warning: Duplicated Index Name Found - sqlite_autoindex_0 , Replaced To SQLITE_AUTOINDEX_05 */
    CREATE UNIQUE INDEX SQLITE_AUTOINDEX_05 ON PRODUCTS PRODUCTID;
    /* Found Index "IX_Products_SuppliersProducts", Options [] */
    CREATE INDEX IX_PRODUCTS_SUPPLIERSPRODUCTS ON PRODUCTS SUPPLIERID;
    /* Found Index "IX_Products_SupplierID", Options [] */
    CREATE INDEX IX_PRODUCTS_SUPPLIERID ON PRODUCTS SUPPLIERID;
    /* Found Index "IX_Products_ProductName", Options [] */
    CREATE INDEX IX_PRODUCTS_PRODUCTNAME ON PRODUCTS PRODUCTNAME;
    /* Found Index "IX_Products_CategoryID", Options [] */
    CREATE INDEX IX_PRODUCTS_CATEGORYID ON PRODUCTS CATEGORYID;
    /* Found Index "IX_Products_CategoriesProducts", Options [] */
    CREATE INDEX IX_PRODUCTS_CATEGORIESPRODUCTS ON PRODUCTS CATEGORYID;

    /* Generators for AutoInc fields for table "Products" */
    /* SELECT  max(Products.ProductID) FROM Products */

    CREATE GENERATOR GEN_PRODUCTS_PRODUCTID;
        SET GENERATOR GEN_PRODUCTS_PRODUCTID TO 77;

    SET TERM ^;
    CREATE TRIGGER TRIG_PRODUCTS_BI FOR PRODUCTS BEFORE INSERT
    AS BEGIN
      IF(NEW.PRODUCTID IS NULL) THEN NEW.PRODUCTID = GEN_ID(GEN_PRODUCTS_PRODUCTID,1);
    END ^
    SET TERM ;^

/* Original table name is "Regions" */
CREATE TABLE REGIONS (
  REGIONID INTEGER /* "RegionID" */ ,
  REGIONDESCRIPTION VARCHAR(50) /* "RegionDescription" */
);

    /* Indexes for table "Regions" */
    /* Found Index "sqlite_autoindex_0", Options [ixUnique] */
    /* !  Warning: Duplicated Index Name Found - sqlite_autoindex_0 , Replaced To SQLITE_AUTOINDEX_06 */
    CREATE UNIQUE INDEX SQLITE_AUTOINDEX_06 ON REGIONS REGIONID;

/* Original table name is "sqlite_sequence" */
CREATE TABLE SQLITE_SEQUENCE (
  NAME VARCHAR(255) /* "name" */ ,
  SEQ INTEGER /* "seq" */
);

/* Original table name is "Suppliers" */
CREATE TABLE SUPPLIERS (
  SUPPLIERID INTEGER /* "SupplierID" */ ,
  COMPANYNAME VARCHAR(40) /* "CompanyName" */ ,
  CONTACTNAME VARCHAR(30) /* "ContactName" */ ,
  CONTACTTITLE VARCHAR(30) /* "ContactTitle" */ ,
  ADDRESS VARCHAR(60) /* "Address" */ ,
  CITY VARCHAR(15) /* "City" */ ,
  REGION VARCHAR(15) /* "Region" */ ,
  POSTALCODE VARCHAR(10) /* "PostalCode" */ ,
  COUNTRY VARCHAR(15) /* "Country" */ ,
  PHONE VARCHAR(24) /* "Phone" */ ,
  FAX VARCHAR(24) /* "Fax" */ ,
  HOMEPAGE VARCHAR(255) /* "HomePage" */
);

    /* Indexes for table "Suppliers" */
    /* Found Index "sqlite_autoindex_0", Options [ixUnique] */
    /* !  Warning: Duplicated Index Name Found - sqlite_autoindex_0 , Replaced To SQLITE_AUTOINDEX_07 */
    CREATE UNIQUE INDEX SQLITE_AUTOINDEX_07 ON SUPPLIERS SUPPLIERID;
    /* Found Index "IX_Suppliers_PostalCode", Options [] */
    CREATE INDEX IX_SUPPLIERS_POSTALCODE ON SUPPLIERS POSTALCODE;
    /* Found Index "IX_Suppliers_CompanyName", Options [] */
    CREATE INDEX IX_SUPPLIERS_COMPANYNAME ON SUPPLIERS COMPANYNAME;

    /* Generators for AutoInc fields for table "Suppliers" */
    /* SELECT  max(Suppliers.SupplierID) FROM Suppliers */

    CREATE GENERATOR GEN_SUPPLIERS_SUPPLIERID;
        SET GENERATOR GEN_SUPPLIERS_SUPPLIERID TO 29;

    SET TERM ^;
    CREATE TRIGGER TRIG_SUPPLIERS_BI FOR SUPPLIERS BEFORE INSERT
    AS BEGIN
      IF(NEW.SUPPLIERID IS NULL) THEN NEW.SUPPLIERID = GEN_ID(GEN_SUPPLIERS_SUPPLIERID,1);
    END ^
    SET TERM ;^

/* Original table name is "Territories" */
CREATE TABLE TERRITORIES (
  TERRITORYID INTEGER /* "TerritoryID" */ ,
  TERRITORYDESCRIPTION VARCHAR(50) /* "TerritoryDescription" */ ,
  REGIONID INTEGER /* "RegionID" */
);

    /* Indexes for table "Territories" */
    /* Found Index "sqlite_autoindex_0", Options [ixUnique] */
    /* !  Warning: Duplicated Index Name Found - sqlite_autoindex_0 , Replaced To SQLITE_AUTOINDEX_08 */
    CREATE UNIQUE INDEX SQLITE_AUTOINDEX_08 ON TERRITORIES TERRITORYID;


/* !  Warnings - 8 */
