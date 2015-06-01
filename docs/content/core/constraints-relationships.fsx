﻿(*** hide ***)
#I @"../../files/sqlite"
(*** hide ***)
#I "../../../bin"

(*

# Constraints & Relationships

A typical relational database will have many connected tables and views 
through foreign key constraints.  The SQL provider is able to show you these 
constraints on entities.  They appear as properties named the same as the 
constraint in the database.

You can gain access to these child or parent entities by simply enumerating 
the property in question.
*)

#r "FSharp.Data.SqlProvider.dll"
open FSharp.Data.Sql
[<Literal>]
let connectionString =
    "Data Source=" + __SOURCE_DIRECTORY__ + @"\northwindEF.db;Version=3"
[<Literal>]
let resolutionPath =
    __SOURCE_DIRECTORY__ + @"..\..\..\files\sqlite"
let sqlite  = SqlDataProvider <I think pg has good docs on their 
                connectionString,
                Common.DatabaseProviderTypes.SQLITE,
                resolutionPath,
                3000,
                true >
//let ctx     = sqlite