(**

## Version control instructions

Git information is in a separate [document](https://fsprojects.github.io/SQLProvider/core/contributing.html).

## The environment

Databases that you should need for development:

 - Demo-data database scripts are at: [/src/DatabaseScripts/](https://github.com/fsprojects/SQLProvider/tree/master/src/DatabaseScripts)
 - Access database is at: [/docs/files/msaccess/Northwind.MDB](https://github.com/fsprojects/SQLProvider/blob/master/docs/files/msaccess/Northwind.MDB)
 - SQLite database is at: [/tests/SqlProvider.Tests/db/northwindEF.db](https://github.com/fsprojects/SQLProvider/blob/master/tests/SqlProvider.Tests/db/northwindEF.db)
 
Even though our test run will run modifications to the test databases, don't check in these `*.mdb` and `*.db` files with your commit, to avoid bad merge-cases.

### Solution structure

We use Fake and Paket. You have to run `build.cmd` on Windows (or `sh ./build.sh` on Mac/Linux) before opening the solutions.

The main source solution is `SQLProvider.sln`.
The unit tests are located in another one, `SQLProvider.Tests.sln`, and when you open the solution, it will lock the `bin\net48\FSharp.Data.SqlProvider.dll`, and after that you can't build the main solution.

 - To debug design-time features you "Attach to process" the main solution debugger to another instance of Visual Studio running the tests solution.
 - To debug runtime you attach it to e.g. fsi.exe and run the code in interactive.

#### Workarounds for "file in use" (issue #172)

 - Debugging execution: Have all the test-files closed in your test-project when you open it with VS. Then you can run tests from the tests from Tests Explorer window (and even debug them if you open the files to that instance of VS from src\SqlProvider).
 - Or you can open tests with some other editor than Visual Studio 2015

### Referenced Files

 - Documentation is located at [SQLProvider/docs/content/core](https://github.com/fsprojects/SQLProvider/tree/master/docs/content/core) and it's converted directly to `*.html` help files by the build-script.
 - `src/ProvidedTypes.fsi`, `src/ProvidedTypes.fs` and `src/Code/ExpressionOptimizer.fs` are coming from other repositoried restored by first build. Location is at [packet.dependencies](https://github.com/fsprojects/SQLProvider/blob/master/paket.dependencies). Don't edit them manually.

### Test cases

There are database specific test files as scripts in the test solution, [/tests/SqlProvider.Tests/scripts/](https://github.com/fsprojects/SQLProvider/tree/master/tests/SqlProvider.Tests/scripts), but also one generic [/tests/SqlProvider.Tests/QueryTests.fs](https://github.com/fsprojects/SQLProvider/blob/master/tests/SqlProvider.Tests/QueryTests.fs) which is running all the SQLite tests in the build script.

## High level description of the provider


### Context and design time

You have a source code like:

*)

type sql = SqlDataProvider<...params...>
let dc = sql.GetDataContext()

(**

What will first happen in the design-time, is that this will call `createTypes` of [SqlDesignTime.fs](https://github.com/fsprojects/SQLProvider/blob/master/src/SQLProvider/SqlDesignTime.fs) and create (as lazily as possible) the database schema types (the shape of the database). These methods are added to the `sql.datacontext` and are stored to concurrent dictionaries. Visual Studio will do a lot of background processing so thread-safety is important here.

`GetDataContext()` will return a dynamic class called dataContext which will on design-time call class SqlDataContext in file [SqlRuntime.DataContext.fs](https://github.com/fsprojects/SQLProvider/blob/master/src/SQLProvider/SqlRuntime.DataContext.fs) through interface `ISqlDataContext`. SqlDataContext uses ProviderBuilder to create database specific providers, fairly well documented `ISqlProvider` in file [SqlRuntime.Common.fs](https://github.com/fsprojects/SQLProvider/blob/master/src/SQLProvider/SqlRuntime.Common.fs).

### Querying

The entity-items themselves are rows in the database data and they are modelled as dynamic sub-classes of `SqlEntity`, base-class in file [SqlRuntime.Common.fs](https://github.com/fsprojects/SQLProvider/blob/master/src/SQLProvider/SqlRuntime.Common.fs) which can be basically think of as wrapper for `Dictionary<string,obj>` (a column name, and the value). SqlEntity is used for all-kind of result-data actually, so the data columns may not correspond to the actual data values. Mostly the results of the data are shaped as `SqlQueryable<SqlEntity>`, or `SqlQueryable<'T>` which is a SQLProvider's class for `IQueryable<'T>` items.

*)

query {
    for cust in dbc.Main.Customers do
    where ("ALFKI" = cust.CustomerId)
    select cust
} |> Seq.toArray

(**

This query is translated to a LINQ-expression-tree through Microsoft.FSharp.Linq.QueryFSharpBuilder. That will call `IQueryable<'T>`'s member Provider to execute two things for the LINQ-expression-tree: first `CreateQuery` and later `Execute`.

### Parsing the LINQ-expression-tree

`CreateQuery` will hit our `SqlQueryable<...>`'s Provider (IQueryProvider) property. LINQ-expression-trees can be kind of recursive type structures, so we it will call CreateQuery for each linq-method. We get the expression-tree as parameter, and parse that with (multi-layer-) active patterns.

Our example the LINQ-expression tree is:

```csharp
.Call System.Linq.Queryable.Where(
    .Constant<System.Linq.IQueryable`1[SqlEntity]>(SqlQueryable`1[SqlEntity]),
    '(.Lambda #Lambda1<System.Func`2[SqlEntity,Boolean]>))
.Lambda #Lambda1<System.Func`2[SqlEntity,Boolean]>(SqlEntity $cust) {
    .Call $cust.GetColumn("CustomerID") == "ALFKI"
}
```

so it would hit this in [SqlRuntime.Linq.fs](https://github.com/fsprojects/SQLProvider/blob/master/src/SQLProvider/SqlRuntime.Linq.fs):

```fsharp
| MethodCall(None, (MethodWithName "Where" as meth), [ SourceWithQueryData source; OptionalQuote qual ]) ->
```

because the LINQ-expression-tree has `ExpressionType.Call` named "Where" with source of IWithSqlService (which is the SqlQueryable<SqlEntity>).
What happens then is parsing of the Where-query. Where-queries are nested structures having known conditions (modelled with pattern `Condition`). If the conditions are having `SqlColumnGet`s, a pattern that says that it's `SqlEntity` with method `GetColumn`, we know that it has to be part of SQL-clause. 

We collect all the known patterns to `IWithSqlService`s field SqlExpression, being a type `SqlExp`, our non-complete known recursive model-tree of SQL clauses.
  
![Operations that can be done on .NET side vs. Operations translated to SQL](https://raw.githubusercontent.com/fsprojects/SQLProvider/master/docs/files/linq-ast.png "Operations that can be done on .NET side vs. Operations translated to SQL")

### Execution of the query

Eventually there also comes the call `executeQuery` (or `executeQueryScalar` for SQL-queries that will return a single value like count), either by enumeration of our IQueryable or at the end of LINQ-expression-tree. That will call `QueryExpressionTransformer.convertExpression`. What happens there (in 
[SqlRuntime.Linq.fs](https://github.com/fsprojects/SQLProvider/blob/master/src/SQLProvider/SqlRuntime.Linq.fs)):

 - We create a projection-lambda. This is described in detail below.
 - We convert our `SqlExp` to real SQL-clause with `QueryExpressionTransformer.convertExpression` calling provider's `GenerateQueryText`-method. Each provider may have some differences in their SQL-syntax.
 - We gather the results as `IEnumerable<SqlEntity>` (or a single return value like count).
 - We execute the projection-lambda to the results.

In our example the whole cust object was selected.
For security reasons we don't do `SELECT *` but we actually list the columns that are there at compile time.

The `TupleIndex` of IWithSqlService is a way to collect joined tables to match the sql-aliasses, here the `[cust]`.

```sql
SELECT [cust].[Address] as 'Address', 
       [cust].[City] as 'City',
       [cust].[CompanyName] as 'CompanyName',
	   [cust].[ContactName] as 'ContactName',
	   [cust].[ContactTitle] as 'ContactTitle',
	   [cust].[Country] as 'Country',
	   [cust].[CustomerID] as 'CustomerID',
	   [cust].[Fax] as 'Fax',
	   [cust].[Phone] as 'Phone',
	   [cust].[PostalCode] as 'PostalCode',
	   [cust].[Region] as 'Region' 
FROM main.Customers as [cust] 
WHERE (([cust].[CustomerID]= @param1))

-- params @param1 - "ALFKI";
```

### Projection-lambda
 
Now, if the select-clause would have been complex:

*)

query {
    for emp in dc.Main.Employees do
    select (emp.BirthDate.DayOfYear + 3)
} |> Seq.toArray

(**

We don't know the function of DayOfYear for each different SQL-providers (Oracle/MSSQL/Odbc/...), but we still want tihs code to work. The LINQ-expression-tree for this query is:

```csharp
.Call System.Linq.Queryable.Select(
    .Constant<System.Linq.IQueryable`1[SqlEntity]>(SqlQueryable`1[SqlEntity]),
    '(.Lambda #Lambda1<System.Func`2[SqlEntity,Int32]>))
.Lambda #Lambda1<System.Func`2[SqlEntity,System.Int32]>(SqlEntity $emp) {
    (.Call $emp.GetColumn("BirthDate")).DayOfYear + 3
}
```

What happens now, is that in [SqlRuntime.QueryExpression.fs](https://github.com/fsprojects/SQLProvider/blob/master/src/SQLProvider/SqlRuntime.QueryExpression.fs) we parse the whole LINQ-expression-tree, and find the parts that we do know to belong to SQL:
the SqlEntity's `emp.GetColumn("BirthDate")`, and create a lambda-expression where this is replaced with a parameter:

```fsharp
fun empBirthDate -> empBirthDate.DayOfYear + 3
```

Now when we get the empBirthDate from the SQL result, we can execute this lambda for the parameter, in .NET-side, not SQL, and then we get the correct result. This is done with `for e in results -> projector.DynamicInvoke(e)` in [SqlRuntime.Linq.fs](https://github.com/fsprojects/SQLProvider/blob/master/src/SQLProvider/SqlRuntime.Linq.fs).

### How to debug SQLProvider in your own solution, not the test-project

The runtime debugging can be done just like any other:

1. Build your own project
2. Then just build SQLProvider.sln and then go to bin-folder, select your framework, and copy FSharp.Data.SqlProvider.dll and FSharp.Data.SqlProvider.pdb
2. And then replace your own project's bin-folder copies of these 2 files, run your software without rebuild.

Debugging the design-time VS2022 is doable, but a bit more complex.
This will mess your SQLProvider Nuget cache, so after done, delete the SQLProvider cache-folder and restore the package again.

1. Open SQLProvider.sln (with Visual Studio 2022) and build it (in debug mode). Keep this open for now.
2. Open explorer, it has made under bin-folder some folders, e.g. \net48 \net6.0 \netstandard2.0 \netstandard2.1 and \typeproviders
3. Open another explorer, go to your location of Nuget cache, the version you are using e.g. `C:\Users\me\.nuget\packages\sqlprovider\1.3.30` 
4. Replace the nuget cache \typeproviders folder with your fresh bin typeproviders folder.
5. Replace the nuget cache \lib folder with other folders from your fresh bin folder.
6. Open another instance of VS2022 to the start-screen but don't open any project yet.
7. Go back to your first instance of VS2022 with SQLProvider.sln. Add some breakpoints. Select from the top menu: Debug - Attach to Process...
8. Select devenv.exe which is the another VS2022 instance.
9. Switch to this new instance and load your own project that uses SQLProvider and it'll stop to the breakpoints.

### Other things to know

 - SQLProvider also runs ExpressionOptimizer functions to simplify the LINQ-expression-trees
 - If you do IN-query (LINQ-Contains) to IEnumerable, it's as normal IN-query, but if the source collection is SqlQueryable<SqlEntity>, then the query is serialized as a nested query, where we have to check that the parameter names won't collide (i.e. param1 can be used only once).

 This documentation was written on 2017-04-11.

*)
