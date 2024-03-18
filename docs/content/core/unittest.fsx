(*** hide ***)
#r "../../../bin/netstandard2.0/FSharp.Data.SqlProvider.dll"
(*** hide ***)
let [<Literal>] resolutionPath = __SOURCE_DIRECTORY__ + @"/../../files/sqlite" 
(*** hide ***)
let [<Literal>] connectionString = "Data Source=" + __SOURCE_DIRECTORY__ + @"\..\northwindEF.db;Version=3;Read Only=false;FailIfMissing=True;"

open FSharp.Data.Sql
(*** hide ***)
type sql  = SqlDataProvider<
                Common.DatabaseProviderTypes.SQLITE,
                connectionString,
                SQLiteLibrary=Common.SQLiteLibrary.SystemDataSQLite,
                ResolutionPath = resolutionPath,
                CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL
            >
(*** hide ***)
open FSharp.Data.Sql
(*** hide ***)
let assert_equal x y = ()
(*** hide ***)
let assert_contains x y = ()
(** 

# SQLProvider allows you to unit-test your SQL-logic

That's a clear advantage on large-scale projects, where there are multiple developers and
the SQL-queries grow more complex over time.

1. Debugging. Faster to debug unit-test than spin the full environment again and again.
2. Refactoring: To ensure what the original functionality is actually doing before you modify.


## Why to unit-test?

F# strong typing provides safety over raw SQL: Instead of your customer finding an issue, your code will not compile if the database shape is wrong,
for example someone removed an important column.

SQLProvider does parametrized SQL, you can watch the executed SQL, and you can even open the parameterized SQL parameters for easier debugging:
*)
FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent 
|> Event.add (fun e -> System.Console.WriteLine (e.ToRawSqlWithParamInfo()))
(**

But unit-testing is good addition in scenarios where:

- Your database is very dynamic, and data is changing all the time
- You want to ensure the logic working over period of time
- You have a big project where build-time takes long
- You want Continuous Integration, but your test-data or database is not stable.


## How?

There are 2 helper functions to mock the database connection:

- `FSharp.Data.Sql.Common.OfflineTools.CreateMockEntities<'T>` - With this you can mock a single table.
- `FSharp.Data.Sql.Common.OfflineTools.CreateMockSqlDataContext<'T>` - With this you can mock a context with multiple tables

You just feed an anonymous records like they would be database rows. 
You don't need to add all the columns, just the ones you use in your query.
But you can add extra-columns for easier asserts.


### Example, executable business logic


*)

open System

type OrderDateFilter =
| OrderDate
| ShippedDate
| Either

let someProductionFunction (ctx:sql.dataContext) (orderType:OrderDateFilter) (untilDate:System.DateTime) =
    task {
        let ignoreOrderDate, ignoreShippedDate =
            match orderType with
            | OrderDate -> false, true
            | ShippedDate -> true, false
            | Either -> false, false

        let tomorrow = untilDate.AddDays(1.).Date
        let someLegacyCondition = 0 // we don't need this anymore

        let itms = 
            query {
                for order in ctx.Main.Orders do
                join cust in ctx.Main.Customers on (order.CustomerId = cust.CustomerId)
                where ((cust.City = "London" || cust.City = "Paris" ) && (
                    (ignoreOrderDate || order.OrderDate < tomorrow) && (someLegacyCondition < 15)) &&
                    (ignoreShippedDate || order.ShippedDate < tomorrow) &&
                    cust.CustomerId <> null && order.Freight > 10m 
                )
                select (cust.PostalCode, order.Freight)
            }
        let! res = itms |> Array.executeQueryAsync

        //maybe some post-processing here...
        return res
    }

(**

### Example, unit-test part

Note: CustomerID, not CustomerId. These are DB-field-names, not nice LINQ names.

*)
let ``mock for unit-testing: datacontext``() =
    task {
        let sampleDataMap =
            [ "main.Customers",
                [|  {| CustomerID = "1"; City = "Paris";  PostalCode = "75000";  Description = "This is good";    |} 
                    {| CustomerID = "2"; City = "London"; PostalCode = "E143AB"; Description = "This is good";    |}
                    {| CustomerID = "3"; City = "Espoo";  PostalCode = "02600";  Description = "Ignore this guy"; |}
                |] :> obj
                "main.Orders",
                [|  {| CustomerID = "1"; OrderDate = DateTime(2020,01,01); ShippedDate = DateTime(2020,01,04); Freight =  4m;|}
                    {| CustomerID = "1"; OrderDate = DateTime(2021,02,11); ShippedDate = DateTime(2021,02,12); Freight = 22m;|}
                    {| CustomerID = "2"; OrderDate = DateTime(2022,03,15); ShippedDate = DateTime(2022,03,22); Freight = 20m;|}
                    {| CustomerID = "2"; OrderDate = DateTime(2024,02,03); ShippedDate = DateTime(2024,02,17); Freight = 50m;|}
                    {| CustomerID = "3"; OrderDate = DateTime(2024,02,03); ShippedDate = DateTime(2024,02,17); Freight = 15m;|}
                |] :> obj

                ] |> Map.ofList
        let mockContext = FSharp.Data.Sql.Common.OfflineTools.CreateMockSqlDataContext<sql.dataContext> sampleDataMap

        let! res = someProductionFunction mockContext OrderDateFilter.OrderDate (DateTime(2024,02,04))
        //val res: (string * decimal) list =
        //  [("75000", 22M); ("E143AB", 20M); ("E143AB", 50M)]        

        assert_equal 3 res.Length
        assert_contains ("75000", 22M) res
        assert_contains ("E143AB", 20M) res
        assert_contains ("E143AB", 50M) res
    }

(**

CreateMockSqlDataContext takes a `Map<string,obj>` where the `string` is the table name as in database, and `obj` is an array of anonymous records.
The mock is meant to help creating data-context objects to enable easier testing of your LINQ-logic, not to test SQLProvider itself.

There are some limitations with the SQLProvider mock a DB-context:

- The mock-context will not connect the DB, and you can't save entity modifications. SubmitUpdates() will do nothing.
- SQLProvider custom operators (like `x |=| xs` and `y %<> "A%"`) are not supported. So you have to use LINQ-ones (e.g. `xs.Contains x` and `not y.StartsWith "A"`) that do work in SQLProvider as well.
- You can call database-table `.Create` methods to create new instances (it doesn't connect the database). You can call update entity columns `x.Col <- Some "hi"`, but it doesn't really do anything.
- You cannot call stored procedures.
- Names are database names, and they are case-sensitive. If you miss a table, in your mock, there will be clear error. If you mistyped anonymous record column name, you will probably just get a zero-result or ValueNone.Value-error or some other unwanted behaviour.

If you are running off-line solution like SSDT or ContextSchemaPath, you should be able to run also these unit-tests with your CI.

*)
