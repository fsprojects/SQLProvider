#SQL Type Provider
[Project Home](https://github.com/fsprojects/SQLProvider)
***NOTE: test code examples, tool-tip capability in projectscaffold***

Features & Capabilities:
* erasing
    * doesn't generate code; more lightweight
* feeble
* individuals - things set as static values (such as configuration) can be set, 
  so exceptions can be thrown if data does not match the expected


##Outline of Docs

* Overview
    * more like ORM
    * All mutating queries are transactional
    * supports identity columns
* Basic Setup / Configuration
    * Special Cases:
        * Special case for Oracle
        * SQLite
            * Mixed-mode assembly
            * Special version
* Basic Queries
    * Caveats:
        * Primary key required for update, delete
        * Relationships aren't "intelligent" - have to create parent-child 
          manually
* Custom Operators
    * IN finds one match in a set
    * Note for slow querying: Using queries isn't required, *not* using the 
      queries will select all rows from the table
    * (look up info on !! operator for joins)
* LINQ support
        * features aren't the same as the MS SQL to LINQ Provider
        * MS version uses sqlmetal, leveraging LINQ2SQL - this one does not as 
          it only supports a few of the query CE keywords
* Stored Procedures
    * [Ross McKinley's Blog entry on CRUD and SP's *note check against library for accuracy*](http://pinksquirrellabs.com/post/2014/05/18/CRUD-Operations-and-Experimental-ODBC-support-in-the-SQLProvider.aspx)
        * Level of support varies based on DB type
* Database-Specific
    * MSSQL
    * Oracle
    * SQLite
    * PostgreSQL
    * MySQL
    * ODBC *experimental* (only supports SELECT & WHERE)

## TODO

Items mentioned in the documentation, however, need further documentation:
* static parameters in creating static type alias
* querying.fsx
* constraints-relationships.fsx
