(*** hide ***)
#I @"../../files/sqlite"
(*** hide ***)
#I "../../../bin"
(*** hide ***)
[<Literal>]
let connectionString = "Microsoft.ACE.OLEDB.12.0;Data Source=" + __SOURCE_DIRECTORY__ + @"\northwindEF.db;Version=3"
[<Literal>]
let connectionString2 = "Microsoft.ACE.OLEDB.12.0;Data Source=MyTest"

(*** hide ***)
[<Literal>]
let resolutionPath = __SOURCE_DIRECTORY__ + @"..\..\..\files\sqlite"
#r "FSharp.Data.SqlProvider.dll"
open FSharp.Data.Sql
(**


# ODBC

Configuring DSN on Windows ODBC Data Source Administrator server:
Control Panel -> Administrative Tools -> Data Sources (ODBC)
(Launch: c:\windows\syswow64\odbcad32.exe)
And add your driver to DSN.

*)

