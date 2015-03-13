#I "../SQLProvider"

#r "System.Transactions"
#r "System.Runtime.Serialization"
#load "Operators.fs"
#load "SchemaProjections.fs"
#load "SqlSchema.fs"
#load "DataTable.fs"
#load "SqlRuntime.Patterns.fs"
#load "SqlHelpers.fs"
#load "SqlRuntime.Common.fs"
#load "Providers.Postgresql.fs"

open System
open FSharp.Data.Sql
open FSharp.Data.Sql.Providers

fsi.AddPrintTransformer(fun (x:Type) -> x.FullName |> box)
let connectionString = "User ID=postgres;Password=password;Host=POSTGRESQL;Port=9090;Database=hr;"
PostgreSQL.resolutionPath <- @"D:\Downloads\Npgsql-2.1.3-net40"

let connection = PostgreSQL.createConnection connectionString

PostgreSQL.createTypeMappings()

PostgreSQL.typeMappings
|> List.filter (fun m -> m.ClrType = "System.DateTime")

PostgreSQL.findDbType "varchar"

PostgreSQL.connect connection (PostgreSQL.getSchema "Tables" [|"hr";"public"|])
|> DataTable.printDataTable

PostgreSQL.connect connection PostgreSQL.getSprocs
//|> DataTable.printDataTable

PostgreSQL.connect connection (SqlHelpers.executeSqlAsDataTable PostgreSQL.createCommand "
            SELECT c.COLUMN_NAME,c.DATA_TYPE, c.character_maximum_length, c.numeric_precision, c.is_nullable
                                                ,CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'PRIMARY KEY' ELSE '' END AS KeyType
                                    FROM INFORMATION_SCHEMA.COLUMNS c
                                    LEFT JOIN (
                                                SELECT ku.TABLE_CATALOG,ku.TABLE_SCHEMA,ku.TABLE_NAME,ku.COLUMN_NAME
                                                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                                                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                                                    ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                                                    AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                                            )   pk 
                                    ON  c.TABLE_CATALOG = pk.TABLE_CATALOG 
                                                AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA
                                                AND c.TABLE_NAME = pk.TABLE_NAME
                                                AND c.COLUMN_NAME = pk.COLUMN_NAME
                                    WHERE c.TABLE_SCHEMA = 'public' AND c.TABLE_NAME = 'employees'
                                    ORDER BY c.TABLE_SCHEMA,c.TABLE_NAME, c.ORDINAL_POSITION
            ")
|> DataTable.printDataTable





