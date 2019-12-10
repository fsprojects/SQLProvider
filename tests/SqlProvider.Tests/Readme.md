
## FSharp Query Expressions

Query expressions enable you to query a data source and put the data in a desired form. Query expressions provide support for LINQ in F#.

SQLProvider generally support most of the [F# Query Expressions](https://docs.microsoft.com/en-us/dotnet/fsharp/language-reference/query-expressions),
but it also can optimize the queries heavily.

Here are some sample queries (taken from [QueryTests.fs](https://github.com/fsprojects/SQLProvider/blob/master/tests/SqlProvider.Tests/QueryTests.fs) and corresponding SQL (by SQLLite, but the SQL-operations do vary.) More details and best practices are described in [Querying](http://fsprojects.github.io/SQLProvider/core/querying.html) documentation.

### Select query

```fsharp
query {
    for cust in dc.Main.Customers do
    select cust
}
```

SQLProvider avoids doing "SELECT *"
Instead the columns are fetched on compile time and serialized to the query explicitly, to avoid surprises.
For example if someone added a bunch of new sensitive or large columns.

```SQL
SELECT 
   [cust].[Address] as 'Address',
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
```

The returned object is a FSharp.Data.Sql.Common.SqlEntity,
which has columns as strongly typed, and also array of ColumnValues.

If you need to update the entity, you'll need the entity,
but otherwise it's a good idea to avoid this large selects and specify the columns in your select clause.

### Select where query

```fsharp
query {
    for cust in dc.Main.Customers do
    where (cust.CustomerId = "ALFKI")
    select cust
}
```

SQLProvider generates parametrized SQL,
to avoid SQL-injection vulnerabilities and
to take advantage of execution plan caching.

```SQL
SELECT 
   [cust].[Address] as 'Address',
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
WHERE (([cust].[CustomerID] = @param1)) 
-- params @param1 - "ALFKI"; 
```

This would return the same SQL:

```fsharp
query {
    for cust in dc.Main.Customers do
    where ("ALFKI" = cust.CustomerId)
    select cust
}
```

### Canonical operation substing query

```fsharp
query {
    for cust in dc.Main.Customers do
    where (cust.CustomerId.Substring(2,3)+"F"="NATF")
    select cust.CustomerId
}
```

You can do operations in .NET and they are translated to
corresponding SQL language of the used database server.

```SQL
SELECT [cust].[CustomerID] as 'CustomerID' 
FROM main.Customers as [cust] 
WHERE (((SUBSTR([cust].[CustomerID], @param1, @param2)
         || @param3) = @param4)) 
-- params @param1 - 2; @param2 - 3; 
--   @param3 - "F"; @param4 - "NATF"; 
```

### Select with distinct

```fsharp
query {
    for cust in dc.Main.Customers do
    select cust.City
    distinct
}
```

```SQL
SELECT DISTINCT [Customers].[City] as 'City' 
FROM main.Customers as [Customers]
```

### Select with skip and take

```fsharp
query {
    for cust in dc.Main.Customers do
    select cust.City
    skip 5
    take 3
}
```

Skip and take support varies by database.

```SQL
SELECT [Customers].[City] as 'City' 
FROM main.Customers as [Customers] 
LIMIT 3 OFFSET 5; 
```

### Select with take only / top

```fsharp
query {
    for cust in dc.Main.Customers do
    select cust.City
    take 5
}
```

Take syntax varies by database.

SQL take will not throw error if not enough items are found.
In .NET the Seq.take will, and there limit-but-not-throw is Seq.truncate.

```SQL
SELECT [Customers].[City] as 'City' 
FROM main.Customers as [Customers]  
LIMIT 5; 
```

### Select where null query

```fsharp
query {
    for cust in dc.Main.Customers do
    where (cust.CustomerId <> null)
    select cust.CustomerId
}
```
FSharp side `cust.CustomerId <> null` us
translated to a proper SQL null-checking.

```SQL
SELECT [cust].[CustomerID] as 'CustomerID' 
FROM main.Customers as [cust] 
WHERE (([cust].[CustomerID] IS NOT NULL))
```

### Select with where boolean option type

```fsharp
query {
    for cust in dc.Main.Customers do
    where (cust.City.IsSome)
    select cust.CustomerId
}
```

If you don't like null-values in your F#-code,
you can use `UseOptionTypes=true`
parameter and handle nullable columns as FSharp Option-types,
converting .IsNone and .IsSome to the proper SQL null-checking.

```SQL
SELECT [cust].[CustomerID] as 'CustomerID' 
FROM main.Customers as [cust] 
WHERE (([cust].[City] IS NOT NULL))

```

### Select where query between column properties

```fsharp
query {
    for order in dc.Main.Orders do
    where (order.ShippedDate > order.RequiredDate)
    select (order.ShippedDate, order.RequiredDate)
}
```

```SQL
SELECT
   [order].[ShippedDate] as 'ShippedDate',
   [order].[RequiredDate] as 'RequiredDate' 
FROM main.Orders as [order] 
WHERE (([order].[ShippedDate] > [order].[RequiredDate]))

```

### Select with boolean variables outside the query

```fsharp
let myCond1 = true
let myCond2 = false
let myCond3 = 0.8 > 0.5
let myCond4 = 4
query {
    for cust in dc.Main.Customers do
    // Simple booleans outside queries are supported:
    where (((myCond1 && myCond1=true) 
        && cust.City="Helsinki" 
        || myCond1) || cust.City="London")
    // Boolean in select fetches just 
    // either country or address, not both:
    select (
       if not(myCond3) then cust.Country 
       else cust.Address)
}
```

SQLProvider will short-circuit and optimize the query before sending it to the database.
So if variables can be evaluated to simplify the query even in runtime, it will be done.

```SQL
SELECT 
   [cust].[Country] as 'Country',
   [cust].[Address] as 'Address' 
FROM main.Customers as [cust] 
WHERE ( (1=1) )
```

### Select one item, with head

```fsharp
query {
    for cust in dc.Main.Customers do
    where (cust.CustomerId = "ALFKI")
    select cust.CustomerId
    head
}
```

There are some database-related differences.
"LIMIT 1" in SQLite is generated as "SELECT TOP 1" in Ms SQL Server.
There is basically no difference between "head" and "headOrDefault".

```SQL
SELECT [cust].[CustomerID] as 'CustomerID' 
FROM main.Customers as [cust] 
WHERE (([cust].[CustomerID] = @param1)) 
LIMIT 1; -- params @param1 - "ALFKI"; 
```

The SQL select would be same even if you have listed the same column multiple times in F#:
"select (cust.CustomerId, cust.CustomerId)" would do still only one "SELECT [cust].[CustomerID]"

### Select with contains query

```fsharp
query {
    for cust in dc.Main.Customers do
    select cust.CustomerId
    contains "ALFKI"
}
```

Contains returns a boolean.
So we don't actually need to transfer the items over the wire.

```SQL
SELECT COUNT(1) 
FROM main.Customers as [Customers] 
WHERE (([Customers].[CustomerID] = @param1)) 
LIMIT 1; 
-- params @param1 - "ALFKI";
```

### Select with contains query with where

```fsharp
query {
    for cust in dc.Main.Customers do
    where (cust.City <> "")
    select cust.CustomerId
    contains "ALFKI"
}
```

```SQL
SELECT COUNT(1) 
FROM main.Customers as [cust] 
WHERE (([cust].[City] <> @param1) 
   AND ([cust].[CustomerID] = @param2)) 
LIMIT 1; 
-- params @param1 - ""; @param2 - "ALFKI"

```

### Select with count

```fsharp
query {
    for cust in dc.Main.Customers do
    select cust.CustomerId
    count
}
```

Counting results a number, so count(1) is enough.

```SQL
SELECT COUNT(1) 
FROM main.Customers as [Customers]
```

### Select with distinct count

```fsharp
query {
    for cust in dc.Main.Customers do
    select cust.CustomerId
    distinct
    count
}
```

Distinct count will count how many different items there are.

```SQL
SELECT COUNT(DISTINCT [Customers].[CustomerID]) 
FROM main.Customers as [Customers]
```

### Exists when exists

```fsharp
query {
    for cust in dc.Main.Customers do
    exists (cust.CustomerId = "WOLZA")
}
```

```SQL
SELECT 
   [Customers].[Address] as 'Address',
   [Customers].[City] as 'City',
   [Customers].[CompanyName] as 'CompanyName',
   [Customers].[ContactName] as 'ContactName',
   [Customers].[ContactTitle] as 'ContactTitle',
   [Customers].[Country] as 'Country',
   [Customers].[CustomerID] as 'CustomerID',
   [Customers].[Fax] as 'Fax',
   [Customers].[Phone] as 'Phone',
   [Customers].[PostalCode] as 'PostalCode',
   [Customers].[Region] as 'Region' 
FROM main.Customers as [Customers] 
WHERE (([Customers].[CustomerID] = @param1))
 LIMIT 1; 
 -- params @param1 - "WOLZA"; 
```

### Select with exactly one

```fsharp
query {
    for cust in dc.Main.Customers do
    where (cust.CustomerId = "ALFKI")
    select cust.CustomerId
    exactlyOne
}
```

The "exactly one" condition is checked at client side.
We don't care to LIMIT or TOP the query results here as 
we expect only a single result, fail-condition is not happy-path anyway.
And limiting could hurt index hitting (e.g. in Oracle).

```SQL
 SELECT [cust].[CustomerID] as 'CustomerID' 
 FROM main.Customers as [cust] 
 WHERE (([cust].[CustomerID] = @param1)) 
 -- params @param1 - "ALFKI";
```


### Select query with join

```fsharp
query {
    for cust in dc.Main.Customers do
    join order in dc.Main.Orders
        on (cust.CustomerId = order.CustomerId)
    select (cust.CustomerId, order.OrderDate)
}
```

Basic join is inner join, and can be used with any columns.

```SQL
SELECT 
   [cust].[CustomerID] as '[cust].[CustomerID]',
   [order].[OrderDate] as '[order].[OrderDate]' 
FROM main.Customers as [cust] 
INNER JOIN  [main].[Orders] as [order] 
   on [cust].[CustomerID] = [order].[CustomerID]
```

### Select query with join using relationships

```fsharp
query {
    for cust in dc.Main.Customers do
    for order in 
       cust.``main.Orders by CustomerID`` do
    select (cust.CustomerId, order.OrderDate)
}
```

Navigation properties are automatically generated from the relationships.

```SQL
SELECT 
   [arg1].[CustomerID] as '[arg1].[CustomerID]',
   [arg2].[OrderDate] as '[arg2].[OrderDate]' 
FROM main.Customers as [arg1] 
INNER JOIN  [main].[Orders] as [arg2] 
   on [arg2].[CustomerID] = [arg1].[CustomerID]
```

### Select query with join multi columns on any columns

```fsharp
query {
    for cust in dc.Main.Customers do
    join order in dc.Main.Orders 
       on ((cust.CustomerId, cust.CustomerId) =
           (order.CustomerId, order.CustomerId))
    select (cust.CustomerId, order.OrderDate)
}
```

```SQL
SELECT 
   [cust].[CustomerID] as '[cust].[CustomerID]',
   [order].[OrderDate] as '[order].[OrderDate]' 
FROM main.Customers as [cust] 
INNER JOIN  [main].[Orders] as [order] 
   on [cust].[CustomerID] = [order].[CustomerID]  
      AND [cust].[CustomerID] = [order].[CustomerID]
```


### Select query with multiple joins on relationships

```fsharp
query {
    for cust in dc.Main.Customers do
    for order in cust.``main.Orders by CustomerID`` do
    for orderDetail in order.``main.OrderDetails by OrderID`` do
    select (cust.CustomerId, 
            orderDetail.ProductId, 
            orderDetail.Quantity)
}
```

```SQL
SELECT 
   [arg1].[CustomerID] as '[arg1].[CustomerID]',
   [arg3].[ProductID] as '[arg3].[ProductID]',
   [arg3].[Quantity] as '[arg3].[Quantity]' 
FROM main.Customers as [arg1] 
INNER JOIN  [main].[Orders] as [arg2] 
   on [arg2].[CustomerID] = [arg1].[CustomerID] 
INNER JOIN  [main].[OrderDetails] as [arg3] 
   on [arg3].[OrderID] = [arg2].[OrderID]
```

### Left join, aka. outer join, aka. left outer join

```fsharp
query {
    for cust in dc.Main.Customers do
    join order in (!!) dc.Main.Orders 
       on (cust.CustomerId = order.OrderId.ToString())
    select (cust.CustomerId, order.OrderDate)
}
```

SQLProvider is not using the standard LeftOuterJoin keyword.
Instead you can transfer any join to a left join (/left outer join)
by using a custom (!!) -operation before the join.

Join condition can have supported operators like ToString() here.

```SQL
SELECT 
   [cust].[CustomerID] as '[cust].[CustomerID]',
   [order].[OrderDate] as '[order].[OrderDate]' 
FROM main.Customers as [cust] 
LEFT OUTER JOIN [main].[Orders] as [order] 
   on [cust].[CustomerID] = CAST([order].[OrderID] AS TEXT)
```

### Left outer join with inner join 

```fsharp
query {
    for cust in dc.Main.Customers do
    join ord1 in dc.Main.Orders 
       on (cust.CustomerId = ord1.CustomerId)
    join ord2 in (!!) dc.Main.Orders 
       on (cust.CustomerId = ord2.CustomerId)
    for ord3 in ord2.``main.OrderDetails by OrderID`` do
    join ord4 in (!!) dc.Main.Orders 
       on (ord2.CustomerId = ord4.CustomerId)
    where (cust.CustomerId = "ALFKI"
        && ord1.OrderId = 10643L 
        && ord2.OrderId = 10643L 
        && ord3.OrderId = 10643L
        && ord4.OrderId = 10643L
        )
    select (Some (cust, ord1))
    head
}
```

```SQL
SELECT 
    [cust].[Address] as '[cust].[Address]',
    [cust].[City] as '[cust].[City]',
    [cust].[CompanyName] as '[cust].[CompanyName]',
    [cust].[ContactName] as '[cust].[ContactName]',
    [cust].[ContactTitle] as '[cust].[ContactTitle]',
    [cust].[Country] as '[cust].[Country]',
    [cust].[CustomerID] as '[cust].[CustomerID]',
    [cust].[Fax] as '[cust].[Fax]',
    [cust].[Phone] as '[cust].[Phone]',
    [cust].[PostalCode] as '[cust].[PostalCode]',
    [cust].[Region] as '[cust].[Region]',
    [ord1].[CustomerID] as '[ord1].[CustomerID]',
    [ord1].[EmployeeID] as '[ord1].[EmployeeID]',
    [ord1].[Freight] as '[ord1].[Freight]',
    [ord1].[OrderDate] as '[ord1].[OrderDate]',
    [ord1].[OrderID] as '[ord1].[OrderID]',
    [ord1].[RequiredDate] as '[ord1].[RequiredDate]',
    [ord1].[ShipAddress] as '[ord1].[ShipAddress]',
    [ord1].[ShipCity] as '[ord1].[ShipCity]',
    [ord1].[ShipCountry] as '[ord1].[ShipCountry]',
    [ord1].[ShipName] as '[ord1].[ShipName]',
    [ord1].[ShipPostalCode] as '[ord1].[ShipPostalCode]',
    [ord1].[ShipRegion] as '[ord1].[ShipRegion]',
    [ord1].[ShippedDate] as '[ord1].[ShippedDate]' 
FROM main.Customers as [cust] 
INNER JOIN  [main].[Orders] as [ord1] 
   on [cust].[CustomerID] = [ord1].[CustomerID] 
LEFT OUTER JOIN  [main].[Orders] as [ord2] 
   on [cust].[CustomerID] = [ord2].[CustomerID] 
INNER JOIN  [main].[OrderDetails] as [ord3] 
   on [ord3].[OrderID] = [ord2].[OrderID] 
LEFT OUTER JOIN  [main].[Orders] as [ord4] 
   on [ord2].[CustomerID] = [ord4].[CustomerID] 
WHERE (([ord4].[OrderID] = @param1 
  AND ([ord3].[OrderID] = @param2 
  AND ([ord2].[OrderID] = @param3 
  AND ([cust].[CustomerID] = @param4 
  AND [ord1].[OrderID] = @param5))))) 
LIMIT 1; 
-- params @param1 - 10643L; @param2 - 10643L; 
--    @param3 - 10643L; @param4 - "ALFKI"; 
--    @param5 - 10643L; 
```

### Select with exactly one or default

```fsharp
query {
    for cust in dc.Main.Customers do
    where (cust.CustomerId = "ZZZZ")
    select cust.CustomerId
    exactlyOneOrDefault
}
```

The operator exactlyOneOrDefault returns either None (null) or Some item,
but if multiple items found, it will throw an InvalidOperationException.

```SQL
SELECT [cust].[CustomerID] as 'CustomerID' 
FROM main.Customers as [cust] 
WHERE (([cust].[CustomerID] = @param1)) 
-- params @param1 - "ZZZZ"; 
```

### Select query into temp

```fsharp
query {
    for cust in dc.Main.Customers do
    select (cust.City, cust.Country) into y
    select (snd y, fst y)
}
```

SQLProvider tries to generate one simple query, and avoid nesting of selects.
Tools like Entity Framework is famous of very complex nested queries hammering non-correctly indexed databases down. SQLProvider tries to avoid that.

```SQL
SELECT
   [Customers].[City] as 'City',
   [Customers].[Country] as 'Country' 
FROM main.Customers as [Customers]
```

### Select query with operations in select

```fsharp
query {
    for cust in dc.Main.Customers do
    select (cust.Country + " " + 
            cust.Address + (1).ToString())
}
```

Used string concatenation operation ("||") varies per database.

Also the generated SELECT-clause varies depending on do you have used
parameter SelectOperations.DatabaseSide or SelectOperations.DotNetSide.
DatabaseSide creates this:

```SQL
SELECT 
   (([cust].[Country] || @param1) || [cust].[Address]) 
      as [op1235565709] 
FROM main.Customers as [cust]
```

DotNetSide creates this:

```SQL
SELECT 
   [Customers].[Country] as 'Country',
   [Customers].[Address] as 'Address' 
FROM main.Customers as [Customers]
```

The returned end-result for the FSharp-developer will be the same.
When considering which one to use, think the network traffic vs other resources.
Parsing in database can make sense if you have a huge string and you do e.g. Substring(1).
Meanwhile database server computing power may not be so well scalable.

### Select and sort query

```fsharp
query {
    for cust in dc.Main.Customers do
    sortBy cust.City
    select cust.City
}
```

```SQL
SELECT [Customers].[City] as 'City' 
FROM main.Customers as [Customers]  
ORDER BY [City]  -- params 
```

### Select and sort desc query

```fsharp
query {
    for cust in dc.Main.Customers do
    sortByDescending cust.City
    select cust.City
}
```

```SQL
SELECT [Customers].[City] as 'City' 
FROM main.Customers as [Customers]
ORDER BY [City] DESC
```

### Select and sort query with then by query

```fsharp
query {
    for cust in dc.Main.Customers do
    sortBy cust.Country
    thenBy cust.City
    select cust.City
}
```

You can do multi-column sorting with thenBy.

```SQL
SELECT [Customers].[City] as 'City' 
FROM main.Customers as [Customers]  
ORDER BY [Country] , [City]

```

### Select and sort query with then by desc query

```fsharp
query {
    for cust in dc.Main.Customers do
    sortBy cust.Country
    thenByDescending cust.City
    select cust.City
}
```

```SQL
SELECT [Customers].[City] as 'City' 
FROM main.Customers as [Customers]  
ORDER BY [Country] , [City] DESC
```


### Select into where

```fsharp
query {
    for cust in dc.Main.Customers do
    select (cust.City + "te") into y
    select (y+"st") into y
    where (y <> "Helsinktest")
}
```

Into hides the previous entities, so the returned result is the result of the last select.

Where-operation has to be parsed in the database, but the generated SELECT-clause varies depending on do you have used parameter SelectOperations.DatabaseSide or SelectOperations.DotNetSide.
DotNetSide creates this:

```SQL
SELECT [Customers].[City] as 'City'
FROM main.Customers as [cust] 
WHERE (((
        ([cust].[City] || @param2) 
           || @param3) <> @param4)) 
-- params @param1 - "te"; @param2 - "te"; 
--    @param3 - "st"; @param4 - "Helsinktest"; 
```

...meanwhile DatabaseSide would starts to do the string-parsing in the database
like: `SELECT ([cust].[City] || @param1) as [result]`...

### Select query let temp variables

```fsharp
query {
    for cust in dc.Main.Customers do
    let y = cust.City
    select cust.Address
}
```

If you use simple temp-variable, it will just be removed.

```SQL
SELECT [Customers].[Address] as 'Address' 
FROM main.Customers as [Customers] 
```

However, there difference between "let" and "into" in LINQ is that:
Let is not hiding the previous query variables.
Into will hide them. 

So if you use do this:

```fsharp
query {
    for cust in dc.Main.Customers do
    let y1 = cust.Address
    let y2 = cust.City
    select (y1, y2, cust)
}
```

...the .NET LINQ is actually doing a tuple of (cust, cust.Address) and (cust, City),
and SQLProvider will select the whole cust-entity columns.

### Select where not query

```fsharp
query {
    for cust in dc.Main.Customers do
    where (not(cust.CustomerId = "ALFKI"))
    select cust.CustomerId
}
```

```SQL
SELECT [cust].[CustomerID] as 'CustomerID' 
FROM main.Customers as [cust] 
WHERE (([cust].[CustomerID] <> @param1)) 
-- params @param1 - "ALFKI"; 
```

### Select where in query

```fsharp
let arr = ["ALFKI"; "ANATR"; "AROUT"]
query {
    for cust in dc.Main.Customers do
    where (arr.Contains(cust.CustomerId))
    select cust.CustomerId
} 
```

Could also be used as NOT-IN just by using `where (not(arr.Contains(cust.CustomerId)))`

```SQL
SELECT [cust].[CustomerID] as 'CustomerID' 
FROM main.Customers as [cust] 
WHERE (([cust].[CustomerID] 
   IN (@param1,@param2,@param3))) 
-- params @param1 - "ALFKI"; @param2 - "ANATR"; 
--    @param3 - "AROUT"; 
```

### Select where in queryable, nested query

```fsharp
let query1 = 
    query {
        for cust in dc.Main.Customers do
        where (cust.City="London")
        select cust.CustomerId
    }

query {
    for cust in dc.Main.Customers do
    where (query1.Contains(cust.CustomerId))
    select cust.CustomerId
}
```

Nested SELECT IN-queries are supported.
Contains is opened from System.Linq but if you don't want to use that,
there is also a custom operator of `|=|` doing the same.

```SQL
SELECT [cust].[CustomerID] as 'CustomerID' 
FROM main.Customers as [cust] 
WHERE (([cust].[CustomerID] IN (
   SELECT [cust].[CustomerID] as 'CustomerID' 
   FROM main.Customers as [cust] 
   WHERE (([cust].[City] = @param9017178840nested1))))) 
-- params @param9017178840nested1 - "London"; 
```

### Select query with case

```fsharp
query {
    for cust in dc.Main.Customers do
    select (if cust.Country = "UK" then (cust.City)
        else ("Outside UK"))
}
```

To generate CASE SQL you need to use the option SelectOperations.DatabaseSide
If you are doing a nested query (like IN-query) 
then it will be generated automatically even without SelectOperations.DatabaseSide.

```SQL
SELECT 
   CASE WHEN ([cust].[Country] = @param1) 
      THEN [cust].[City] 
   ELSE @param2 END as [result] 
   FROM main.Customers as [cust]  
-- params @param1 - "UK"; @param2 - "Outside UK"; 
```

### Select where left join (box-check) and not in queryable query

```fsharp
let query1 = 
    query {
        for cust in dc.Main.Customers do
        where (cust.City="London")
        select cust.CustomerId
    }

query {
    for cust in dc.Main.Customers do
    for ord in (!!) cust.``main.Orders by CustomerID`` do
    where (box(ord.OrderDate) = null &&
        not(query1.Contains(cust.CustomerId)))
    select cust.CustomerId
} |> Seq.toArray
```

Box-check is used here because OrderDate is not-nullable field 
but left join may cause it to be null anyway.

```SQL
SELECT [arg1].[CustomerID] as '[arg1].[CustomerID]' 
FROM main.Customers as [arg1] 
LEFT OUTER JOIN [main].[Orders] as [arg2] 
   on [arg2].[CustomerID] = [arg1].[CustomerID] 
WHERE (([arg2].[OrderDate] IS NULL 
   AND [arg1].[CustomerID] NOT IN (
      SELECT [cust].[CustomerID] as 'CustomerID' 
      FROM main.Customers as [cust] 
      WHERE (([cust].[City] = @param9017178840nested1))))) 
-- params @param9017178840nested1 - "London"; 
```

### Select where query with operations in where

```fsharp
query {
    for cust in dc.Main.Customers do
    where (cust.CustomerId = "ALFKI" 
       && (cust.City.StartsWith("B")))
    select cust.CustomerId
}
```

```SQL
SELECT [cust].[CustomerID] as 'CustomerID' 
FROM main.Customers as [cust] 
WHERE (([cust].[CustomerID] = @param1 
   AND [cust].[City] LIKE @param2)) 
-- params @param1 - "ALFKI"; @param2 - "B%"; 

```


### Select query async

```fsharp
async {
    let! asyncquery =
        query {
            for cust in dc.Main.Customers do
            select cust.CustomerId
        } |> Seq.executeQueryAsync 
    return asyncquery |> Seq.toList
} |> Async.StartAsTask
```

```SQL
SELECT [Customers].[CustomerID] as 'CustomerID' 
FROM main.Customers as [Customers]
```


### Select query with minBy

```fsharp
query {
    for ord in dc.Main.OrderDetails do
    minBy ord.Discount
}
```

Aggregations like sumBy (Sum), minBy (Min), maxBy (Max), Count, averageBy (AVG), Sum are supported.
Min and Max can be also used with types like DateTime and String.

```SQL
 SELECT MIN([OrderDetails].[Discount]) 
    as '[OrderDetails].[MIN_Discount]' 
 FROM main.OrderDetails as [OrderDetails]  
```

Using sumByNullable, minByNullable, maxByNullable and averageByNullable are identical.

If you want to do joins over multiple tables, use
 `query { ... select od.UnitPrice } |> Seq.sumAsync`

### Select query with minBy with custom calculations

```fsharp
query {
    for od in dc.Main.OrderDetails do
    sumBy (od.UnitPrice*od.UnitPrice-2m)
}
```
Aggregation operations can have operators.
Return type can be casted inside query if needed.

```SQL
SELECT 
   SUM((([OrderDetails].[UnitPrice] * [OrderDetails].[UnitPrice]) - 2)) 
      as '[OrderDetails].[SUM_ccUnitPrice]' 
FROM main.OrderDetails as [OrderDetails]

```

### Select query with averageBy length

```fsharp
query {
    for c in dc.Main.Customers do
    averageBy (decimal(c.ContactName.Length))
}
```

The good thing is that these kind of operations work across the databases
and you don't have to know your database specific SQL-syntax. Just use .NET.

```SQL
SELECT 
   AVG(LENGTH([Customers].[ContactName])) 
   as '[Customers].[AVG_cContactName]' 
FROM main.Customers as [Customers]
```

### Where before join

```fsharp
query {
    for od in dc.Main.OrderDetails do
    where(od.OrderId > 0L)
    join o in dc.Main.Orders 
       on (od.OrderId=o.OrderId)
    sortBy od.OrderId
    select (od.OrderId, o.OrderId)
}
```

```SQL
SELECT 
   [OrderDetails].[OrderID] as '[OrderDetails].[OrderID]',
   [o].[OrderID] as '[o].[OrderID]' 
FROM main.OrderDetails as [OrderDetails] 
INNER JOIN  [main].[Orders] as [o] 
   on [OrderDetails].[OrderID] = [o].[OrderID] 
WHERE (([OrderDetails].[OrderID] > @param1)) 
ORDER BY [OrderDetails].[OrderID]  
-- params @param1 - 0L;
```

### Select into a custom generic type

```fsharp
type Dummy<'t> = D of 't
query {
    for emp in dc.Main.Customers do
    select (D {First=emp.ContactName})
}
```

You can select/map into a custom record and SQLProvider will just
fetch the columns needed in the select clause.

```SQL
SELECT [Customers].[ContactName] as 'ContactName' 
FROM main.Customers as [Customers]
```

### Nth query

```fsharp
query {
    for cust in dc.Main.Customers do
    select cust.CustomerId
    nth 4
}
```

SQLProvider tries to limit the fetched item count as much as possible.
The generated query varies a bit per database.

```SQL
SELECT [Customers].[CustomerID] as 'CustomerID' 
FROM main.Customers as [Customers]
LIMIT 1 OFFSET 3; 
```

### Select query with find

```fsharp
query {
    for ord in dc.Main.OrderDetails do
    find (ord.UnitPrice > 10m)
}
```

Find entity that hold the condition given.

```SQL
SELECT 
   [OrderDetails].[Discount] as 'Discount',
   [OrderDetails].[OrderID] as 'OrderID',
   [OrderDetails].[ProductID] as 'ProductID',
   [OrderDetails].[Quantity] as 'Quantity',
   [OrderDetails].[UnitPrice] as 'UnitPrice' 
FROM main.OrderDetails as [OrderDetails] 
WHERE (([OrderDetails].[UnitPrice] > @param1)) 
LIMIT 1; 
-- params @param1 - 10M; 

-- params @param1 - 0M; 
```

### Select query with all

```fsharp
query {
    for ord in dc.Main.OrderDetails do
    all (ord.UnitPrice > 0m)
}
```

Will return a boolean.
Very much like negative-find:
SQLProvider tries to find a single entity that is not holding the condition.

```SQL
SELECT 
   [OrderDetails].[Discount] as 'Discount',
   [OrderDetails].[OrderID] as 'OrderID',
   [OrderDetails].[ProductID] as 'ProductID',
   [OrderDetails].[Quantity] as 'Quantity',
   [OrderDetails].[UnitPrice] as 'UnitPrice' 
FROM main.OrderDetails as [OrderDetails] 
WHERE (([OrderDetails].[UnitPrice] <= @param1)) 
LIMIT 1; 
-- params @param1 - 0M; 
```

### Select query with groupBy

```fsharp
query {
    for cust in dc.Main.Customers do
    groupBy cust.City
}
```

```SQL
SELECT [City] FROM main.Customers as [Customers]  
GROUP BY [City]
```

### Select query with groupBy aggregate

```fsharp
query {
    for o in dc.Main.Orders do
    groupBy o.OrderDate.Date into g
    select (g.Key, g.Sum(fun p ->
        if p.Freight > 1.0 then p.Freight else 0.0))
}
```

Select case is supported when doing GroupBy.

```SQL
SELECT 
   DATE([OrderDate]), 
   SUM(
      CASE WHEN ([Freight] > @param1) 
         THEN [Freight] 
      ELSE @param2 END
      ) as 'SUM_cFreight' 
FROM main.Orders as [Orders]  
GROUP BY DATE([OrderDate])
-- params @param1 - 1.0; @param2 - 0.0; 

```

### Select query with groupBy aggregate temp total

```fsharp
query {
    for o in dc.Main.Orders do
    groupBy o.OrderDate.Date into g
    let total = query {
        for s in g do
        sumBy s.Freight
    }
    select (g.Key, total)
}
```

```SQL
SELECT 
   DATE([OrderDate]), 
   SUM([Freight]) as 'SUM_Freight' 
FROM main.Orders as [Orders]
GROUP BY DATE([OrderDate])
```

### Select query with groupBy constant

```fsharp
query {
    for p in dc.Main.Products do
    groupBy 1 into g
    select (g.Max(fun p -> p.CategoryId), 
            g.Average(fun p -> p.CategoryId))
}
```

If you want to do multiple aggregate operations in a single query
without group-by you have to do it by groupBy over a constant (like 1) in FSharp side.
You can also do custom calculations inside aggregate operations.

```SQL
SELECT 
   AVG([CategoryID]) as 'AVG_CategoryID', 
   MAX([CategoryID]) as 'MAX_CategoryID' 
FROM main.Products as [Products]
```

### Select query with groupBy count

```fsharp
query {
    for cust in dc.Main.Customers do
    groupBy cust.City into c
    select (c.Key, c.Count())
}
```

You could also group by a date value.

```SQL
SELECT 
   [City], 
   COUNT(1) as 'COUNT_City' 
FROM main.Customers as [Customers]
GROUP BY [City] -- params 
```

### Select query with groupBy having and then sort

```fsharp
query {
    for order in dc.Main.Orders do
    groupBy (order.ShipCity) into ts
    where (ts.Count() > 1)
    sortBy (ts.Key)
    thenBy (ts.Key)
    select (ts.Key, 
            ts.Average(fun o -> o.Freight))
}
```

Where after group-by is translated to having.

```SQL
SELECT 
   [ShipCity], 
   AVG([Freight]) as 'AVG_Freight' 
FROM main.Orders as [Orders]
GROUP BY [ShipCity] 
HAVING ((COUNT(1) > @param1)) 
ORDER BY [Orders].[ShipCity], [Orders].[ShipCity]  
-- params @param1 - 1; 
```

### Select query with groupBy multiple columns

```fsharp
query {
    for p in dc.Main.Products do
    groupBy (p.ReorderLevel, p.CategoryId) into c
    select (c.Key, c.Sum(fun i -> i.UnitPrice))
}
```

```SQL
SELECT 
   [ReorderLevel], 
   [CategoryID], 
   SUM([UnitPrice]) as 'SUM_UnitPrice' 
FROM main.Products as [Products]
GROUP BY [ReorderLevel], [CategoryID]
```

### Select query with groupBy sum

```fsharp
query {
    for od in dc.Main.OrderDetails do
    groupBy od.ProductId into p
    select (p.Key,
            p.Sum(fun f -> f.UnitPrice),
            p.Sum(fun f -> f.Discount),
            p.Sum(fun f -> f.UnitPrice+1m))
}
```

```SQL
SELECT 
   [ProductID], 
   SUM(([UnitPrice] + 1)) as 'SUM_cUnitPrice', 
   SUM([Discount]) as 'SUM_Discount', 
   SUM([UnitPrice]) as 'SUM_UnitPrice' 
FROM main.OrderDetails as [OrderDetails]
GROUP BY [ProductID]
```

### Select query with groupBy having count

```fsharp
query {
    for cust in dc.Main.Customers do
    groupBy cust.City into c
    where (c.Count() > 1)
    where (c.Count() < 6)
    select (c.Key, c.Count())
}
```

```SQL
SELECT 
   [City], 
   COUNT(1) as 'COUNT_City' 
FROM main.Customers as [Customers]
GROUP BY [City]
HAVING ((COUNT(1) > @param1) 
    AND (COUNT(1) < @param2)) 
-- params @param1 - 1; @param2 - 6; 
```

### Select query with groupBy where and having

```fsharp
query {
    for emp in dc.Main.Employees do
    where (emp.Country = "USA")
    groupBy emp.City into grp
    where (grp.Count() > 0)
    select (grp.Key, 
            grp.Sum(fun e -> e.EmployeeId))
}
```

```SQL
SELECT 
   [emp].[City], 
   SUM([emp].[EmployeeID]) as '[emp].[SUM_EmployeeID]' 
FROM main.Employees as [emp] 
WHERE (([emp].[Country] = @param1)) 
GROUP BY [emp].[City] 
HAVING ((COUNT(1) > @param2)) 
-- params @param1 - "USA"; @param2 - 0; 
```

### Select query with groupBy having key

```fsharp
query {
    for cust in dc.Main.Customers do
    groupBy cust.City into c
    where (c.Key = "London") 
    select (c.Key, c.Count()) 
}
```

```SQL
SELECT 
   [City], 
   COUNT(1) as 'COUNT_City' 
FROM main.Customers as [Customers]
GROUP BY [City] 
HAVING (([City] = @param1)) 
-- params @param1 - "London"; 
```

### If query

```fsharp
query {
    for cust in dc.Main.Customers do
    if cust.Country = "UK" then select cust.City
}
```

```SQL
SELECT [arg1].[City] as 'City' 
FROM main.Customers as [arg1] 
WHERE (([arg1].[Country] = @param1)) 
-- params @param1 - "UK"; 
```


### Sort query with lambda cast to IComparable

```fsharp
query {
    for cust in dc.Main.Customers do
    sortBy ((fun (c : sql.dataContext.``main.CustomersEntity``) -> 
                      c.CustomerId :> IComparable) cust)
    select cust.City
    take 1
}
```

```SQL
SELECT [Customers].[City] as 'City' 
FROM main.Customers as [Customers]  
ORDER BY [CustomerID]  
LIMIT 1;

```

### Select query with join and then groupBy

```fsharp
query {
    for cust in dc.Main.Customers do
    join order in dc.Main.Orders 
       on (cust.CustomerId = order.CustomerId)
    groupBy (cust.City, order.ShipCity) into g
    select (g.Key, g.Max(fun (c,o) -> c.PostalCode))
}
```

```SQL
SELECT 
   [cust].[City], 
   [order].[ShipCity], 
   MAX([cust].[PostalCode]) as '[cust].[MAX_PostalCode]' 
FROM main.Customers as [cust] 
INNER JOIN  [main].[Orders] as [order] 
   on [cust].[CustomerID] = [order].[CustomerID]  
GROUP BY [cust].[City], [order].[ShipCity]
```

### Select query with joins and then groupBy

```fsharp
query {
    for cust in dc.Main.Customers do
    join order in dc.Main.Orders 
       on (cust.CustomerId = order.CustomerId)
    join order2 in dc.Main.Orders 
       on (cust.CustomerId = order2.CustomerId)
    groupBy (cust.City, order.ShipCity) into g
    select (g.Key, g.Max(fun (c,o,o2) -> c.PostalCode))
}
```

Instead of creating too much query-logic in FSharp, 
it is prefereable to create a view or stored procedure
and use that from SQLProvider.

```SQL
SELECT 
   [cust].[City], 
   [order].[ShipCity], 
   MAX([cust].[PostalCode]) as '[cust].[MAX_PostalCode]' 
FROM main.Customers as [cust] 
INNER JOIN  [main].[Orders] as [order] 
   on [cust].[CustomerID] = [order].[CustomerID] 
INNER JOIN  [main].[Orders] as [order2] 
   on [cust].[CustomerID] = [order2].[CustomerID]
GROUP BY [cust].[City], [order].[ShipCity]
```

### Simple math query

```fsharp
query {
    for p in dc.Main.Products do
    where (p.ProductId - 21L = 23L)
    select p.ProductId
}
```

Here the 23 is parametrized but the 21 is not parametrized.
This is because 21 is a number and we expect that calculation
not to change between different queries.

```SQL
SELECT [p].[ProductID] as 'ProductID' 
FROM main.Products as [p] 
WHERE ((([p].[ProductID] - 21) = @param1)) 
-- params @param1 - 23L; 
```

### Async sum with operations

```fsharp
query {
    for emp in dc.Main.Employees do
    select (decimal(emp.HireDate.Year)*
            2m*Math.Min(
                2m, if emp.HireDate.Subtract(emp.BirthDate.AddYears(1)).Days>0 then
                        Math.Abs(
                        decimal(emp.HireDate.Subtract(emp.BirthDate).Days)
                        /decimal(emp.HireDate.Subtract(emp.BirthDate.AddYears(1)).Days))
                else 1m
        ))
} |> Seq.sumAsync |> Async.RunSynchronously
```

Operations (like DateTime operations) do vary per database.

```SQL
SELECT 
   SUM(((CAST(STRFTIME('%Y', [Employees].[HireDate]) as INTEGER) * 2) * 
       MIN(
         CASE WHEN (
            CAST(JULIANDAY([Employees].[HireDate]) - 
            JULIANDAY(DATETIME([Employees].[BirthDate], '+1 year')) 
              as INTEGER) > @param1) 
         THEN ABS((CAST(JULIANDAY([Employees].[HireDate]) - 
            JULIANDAY([Employees].[BirthDate]) as INTEGER) / 
               CAST(JULIANDAY([Employees].[HireDate]) - 
                  JULIANDAY(
                    DATETIME([Employees].[BirthDate], '+1 year')) as INTEGER))) 
         ELSE @param2 END , @param3))) 
            as '[Employees].[SUM_cccHireDate]' 
FROM main.Employees as [Employees]
-- params @param1 - 0; @param2 - 1M; @param3 - 2M; 

```

### Canonical join query

```fsharp
query {
    for cust in dc.Main.Customers do
    join emp in dc.Main.Employees 
       on (cust.City = 
           if emp.City = "" then "" else emp.City)
    sortBy (cust.ContactName)
    select (cust.ContactName)
}
```

```SQL
SELECT [cust].[ContactName] as '[cust].[ContactName]' 
FROM main.Customers as [cust] 
INNER JOIN  [main].[Employees] as [emp] 
   on [cust].[City] = 
      CASE WHEN ([emp].[City] = @param1) 
      THEN @param2 
      ELSE [emp].[City] END
ORDER BY [cust].[ContactName]  
-- params @param1 - ""; @param2 - ""; 

```

### Select cross-join

```fsharp
query {
    for cust in dc.Main.Customers do
    for emp in dc.Main.Employees do
    select (cust.ContactName, emp.LastName)
    take 3
}
```

Cross join is an all-to-all relation, 
for x in xs, 
for y in ys
...

```SQL
SELECT 
   [Customers].[ContactName] as '[Customers].[ContactName]',
   [Employees].[LastName] as '[Employees].[LastName]' 
FROM main.Customers as [Customers], 
     main.Employees as [Employees]
LIMIT 3; -- params 

```

### Select cross-join 3 tables

```fsharp
query {
    for cust in dc.Main.Customers do
    for emp in dc.Main.Employees do
    for ter in dc.Main.Territories do
    where (cust.City=emp.City && ter.RegionId > 3L)
    select (cust.ContactName, emp.LastName, ter.RegionId)
    take 3
}
```

```SQL
SELECT 
   [Customers].[ContactName] as '[Customers].[ContactName]',
   [Employees].[LastName] as '[Employees].[LastName]',
   [Territories].[RegionID] as '[Territories].[RegionID]' 
FROM main.Customers as [Customers], 
     main.Employees as [Employees], 
     main.Territories as [Territories] 
WHERE (
   ([Customers].[City] = [Employees].[City] 
      AND [Territories].[RegionID] > @param2)) 
LIMIT 3; 
-- params @param2 - 3L; 
```

### Select join and cross-join

```fsharp
query {
    for cust in dc.Main.Customers do
    for x in cust.``main.Orders by CustomerID`` do
    for emp in dc.Main.Employees do
    select (x.Freight, cust.ContactName, emp.LastName)
    head
}
```

```SQL
SELECT 
   [arg2].[Freight] as '[arg2].[Freight]',
   [arg1].[ContactName] as '[arg1].[ContactName]',
   [Employees].[LastName] as '[Employees].[LastName]' 
FROM main.Customers as [arg1], 
     main.Employees as [Employees] 
INNER JOIN  [main].[Orders] as [arg2] 
   on [arg2].[CustomerID] = [arg1].[CustomerID]
```

### Select nested query

```fsharp
query {
    for c3 in (query {
        for b2 in (query {
            for a1 in (query {
                for emp in dc.Main.Employees do
                select emp
            }) do
            select a1
        }) do
        select b2.LastName
    }) do
    select c3
}
```

SQLProvider rather selects the whole entity than 
tries to generate nested selects to database query.

Instead of creating too much query-logic in FSharp, 
it is prefereable to create a view or stored procedure
and use that from SQLProvider.


```SQL
SELECT 
   [emp].[Address] as 'Address',
   [emp].[BirthDate] as 'BirthDate',
   [emp].[City] as 'City',
   [emp].[Country] as 'Country',
   [emp].[EmployeeID] as 'EmployeeID',
   [emp].[Extension] as 'Extension',
   [emp].[FirstName] as 'FirstName',
   [emp].[HireDate] as 'HireDate',
   [emp].[HomePhone] as 'HomePhone',
   [emp].[LastName] as 'LastName',
   [emp].[Notes] as 'Notes',
   [emp].[Photo] as 'Photo',
   [emp].[PhotoPath] as 'PhotoPath',
   [emp].[PostalCode] as 'PostalCode',
   [emp].[Region] as 'Region',
   [emp].[Title] as 'Title',
   [emp].[TitleOfCourtesy] as 'TitleOfCourtesy' 
FROM main.Employees as [emp]
```


### Select nested query with crossjoin

```fsharp
query {
    for cust in dc.Main.Customers do
    for a1 in (query {
        for emp in dc.Main.Employees do
        select (emp)
    }) do
    where(a1.FirstName = cust.ContactName)
    select (a1.FirstName)
}
```

```SQL
 SELECT 
    [Customers].[Address] as '[Customers].[Address]',
    [Customers].[City] as '[Customers].[City]',
    [Customers].[CompanyName] as '[Customers].[CompanyName]',
    [Customers].[ContactName] as '[Customers].[ContactName]',
    [Customers].[ContactTitle] as '[Customers].[ContactTitle]',
    [Customers].[Country] as '[Customers].[Country]',
    [Customers].[CustomerID] as '[Customers].[CustomerID]',
    [Customers].[Fax] as '[Customers].[Fax]',
    [Customers].[Phone] as '[Customers].[Phone]',
    [Customers].[PostalCode] as '[Customers].[PostalCode]',
    [Customers].[Region] as '[Customers].[Region]',
    [Employees].[FirstName] as '[Employees].[FirstName]' 
FROM main.Customers as [Customers] , 
     main.Employees as [Employees] 
WHERE (([Employees].[FirstName] = [Customers].[ContactName]))
```


### Select join twice to same table

```fsharp
query {
    for emp in dc.Main.Employees do
    join cust1 in dc.Main.Customers 
       on (emp.Country = cust1.Country)
    join cust2 in dc.Main.Customers 
       on (emp.Country = cust2.Country)
    where (cust1.City = "Butte" && cust2.City = "Kirkland")
    select (emp.EmployeeId, cust1.City, cust2.City)
}
```

```SQL
SELECT 
   [emp].[EmployeeID] as '[emp].[EmployeeID]',
   [cust1].[City] as '[cust1].[City]',
   [cust2].[City] as '[cust2].[City]' 
FROM main.Employees as [emp] 
INNER JOIN  [main].[Customers] as [cust1] 
   on [emp].[Country] = [cust1].[Country] 
INNER JOIN  [main].[Customers] as [cust2] 
   on [emp].[Country] = [cust2].[Country]
WHERE (([cust1].[City] = @param1 
   AND [cust2].[City] = @param2)) 
-- params @param1 - "Butte"; @param2 - "Kirkland"; 
```



### Inverted operations query`

```fsharp
query {
    for o in dc.Main.Orders do
    where (500m < o.Freight)
    select o.OrderId
}
```

```SQL
SELECT [o].[OrderID] as 'OrderID' 
FROM main.Orders as [o] 
WHERE (([o].[Freight] > @param1)) 
-- params @param1 - 500M; 
```

### Canonical operations case-when-elses

```fsharp
query {
    for cust in dc.Main.Customers do
    join emp in dc.Main.Employees 
       on (cust.City.Trim() + "_" + cust.Country = 
          emp.City.Trim() + "_" + emp.Country)
    where ((if box(emp.BirthDate)=null then 200 
            else 100) = 100) 
    where ((if emp.EmployeeId > 1L then 200 
            else 100) = 100) 
    where ((if emp.BirthDate > emp.BirthDate then 200 
            else 100) = 100)
    select (cust.CustomerId, cust.City, emp.BirthDate)
    distinct
}
```

```SQL
SELECT DISTINCT 
   [cust].[CustomerID] as '[cust].[CustomerID]',
   [cust].[City] as '[cust].[City]',
   [emp].[BirthDate] as '[emp].[BirthDate]' 
FROM main.Customers as [cust] 
INNER JOIN  [main].[Employees] 
   as [emp] on ((TRIM([cust].[City]) || @param1) || [cust].[Country]) = 
      ((TRIM([emp].[City]) || @param2) || [emp].[Country]) 
WHERE ((CASE WHEN ([emp].[BirthDate] IS NULL) THEN 
          @param4 ELSE @param5 END  = @param6) 
    AND (CASE WHEN ([emp].[EmployeeID] > @param7) THEN 
          @param8 ELSE @param9 END  = @param10) 
    AND (CASE WHEN ([emp].[BirthDate] > [emp].[BirthDate]) THEN 
          @param12 ELSE @param13 END  = @param14)) 
-- params @param1 - "_"; @param2 - "_"; @param4 - 200; 
--    @param5 - 100; @param6 - 100; @param7 - 1L; 
--    @param8 - 200; @param9 - 100; @param10 - 100; 
--    @param12 - 200; @param13 - 100; @param14 - 100; 
```

### Query with yield keyword

```fsharp
query {
    for cus in dc.Main.Customers do
    where (cus.City = "London")
    yield (cus.City + "1")
}
```

```SQL
SELECT ([_arg2].[City] || @param1) as [result] 
FROM main.Customers as [_arg2] 
WHERE (([_arg2].[City] = @param2)) 
-- params @param1 - "1"; @param2 - "London"; 
```

### Union query

```fsharp
let query1 = 
    query {
        for cus in dc.Main.Customers do
        where (cus.City <> "Atlantis1")
        select (cus.City)
    }
let query2 = 
    query {
        for emp in dc.Main.Employees do
        where (emp.City <> "Atlantis2")
        select (emp.City)
    } 
query1.Union(query2)
```

Union, Intersect, Except and Concat (Union all) are from System.Linq-namespace.

```SQL
SELECT [cus].[City] as 'City' 
FROM main.Customers as [cus] 
WHERE (([cus].[City] <> @param1)) 

UNION 

SELECT [emp].[City] as 'City' 
FROM main.Employees as [emp] 
WHERE (([emp].[City] <> @param1975168501nested1))

-- params @param1 - "Atlantis1";
--   @param1975168501nested1 - "Atlantis2"; 
```

### Intersect query

```fsharp
let query1 = 
    query {
        for cus in dc.Main.Customers do
        where (cus.City <> "Atlantis1")
        select (cus.City)
    }
let query2 = 
    query {
        for emp in dc.Main.Employees do
        where (emp.City <> "Atlantis2")
        select (emp.City)
    } 
query1.Intersect(query2)
```

```SQL
SELECT [cus].[City] as 'City' 
FROM main.Customers as [cus] 
WHERE (([cus].[City] <> @param1)) 

INTERSECT 

SELECT [emp].[City] as 'City' 
FROM main.Employees as [emp] 
WHERE (([emp].[City] <> @param1975168501nested1))  

-- params @param1 - "Atlantis1"; 
--   @param1975168501nested1 - "Atlantis2"; 

```

### Except query

```fsharp
let query1 = 
    query {
        for cus in dc.Main.Customers do
        where (cus.City <> "Atlantis1")
        select (cus.City)
    }
let query2 = 
    query {
        for emp in dc.Main.Employees do
        where (emp.City <> "Atlantis2")
        select (emp.City)
    } 
query2.Except(query1)
```

```SQL
SELECT [emp].[City] as 'City' 
FROM main.Employees as [emp] 
WHERE (([emp].[City] <> @param1)) 

EXCEPT 

SELECT [cus].[City] as 'City' 
FROM main.Customers as [cus] 
WHERE (([cus].[City] <> @param347080386nested1))  

-- params @param1 - "Atlantis2"; 
--   @param347080386nested1 - "Atlantis1"; 
```

### Union all query

```fsharp
let query1 = 
    query {
        for cus in dc.Main.Customers do
        select (cus.City)
    }
let query2 = 
    query {
        for emp in dc.Main.Employees do
        select (emp.City)
    } 
query1.Concat(query2)
```

```SQL
SELECT [Customers].[City] as 'City' 
FROM main.Customers as [Customers]  

UNION ALL 

SELECT [Employees].[City] as 'City' 
FROM main.Employees as [Employees]
```


### Delete where query

```fsharp
query {
    for cust in dc.Main.Customers do
    where (
       cust.City = "Atlantis" || 
       cust.CompanyName = "Home")
} |> Seq.``delete all items from single table`` 
|> Async.RunSynchronously
```

```SQL
DELETE FROM main.Customers 
WHERE (([Customers].[City] = @param1 
   OR [Customers].[CompanyName] = @param2)) 
-- params @param1 - "Atlantis"; @param2 - "Home"; 
```

### Select with subquery exists subquery

```fsharp
query {
    for cust in dc.Main.Customers do
    where(query {
            for cust2 in dc.Main.Customers do
            exists(cust2.CustomerId = "ALFKI")
        })
    select cust.CustomerId
}
```

```SQL
SELECT [cust].[CustomerID] as 'CustomerID' 
FROM main.Customers as [cust] 
WHERE ((EXISTS (
   SELECT 
      [cust2].[Address] as 'Address',
      [cust2].[City] as 'City',
      [cust2].[CompanyName] as 'CompanyName',
      [cust2].[ContactName] as 'ContactName',
      [cust2].[ContactTitle] as 'ContactTitle',
      [cust2].[Country] as 'Country',
      [cust2].[CustomerID] as 'CustomerID',
      [cust2].[Fax] as 'Fax',
      [cust2].[Phone] as 'Phone',
      [cust2].[PostalCode] as 'PostalCode',
      [cust2].[Region] as 'Region' 
   FROM main.Customers as [cust2] 
   WHERE (([cust2].[CustomerID] = @param1176508420nested1))
))) 
-- params @param1176508420nested1 - "ALFKI"; 
```

### Select with subquery exists parameter from main query

```fsharp
query {
    for o in dc.Main.Orders do
    where(query {
            for od in dc.Main.OrderDetails do
            where (od.Quantity > (int16 10))
            exists(od.OrderId = o.OrderId)
        })
    select o.OrderId
}
```

```SQL
SELECT [o].[OrderID] as 'OrderID' 
FROM main.Orders as [o] 
WHERE ((EXISTS (
   SELECT 
      [od].[Discount] as 'Discount',
      [od].[OrderID] as 'OrderID',
      [od].[ProductID] as 'ProductID',
      [od].[Quantity] as 'Quantity',
      [od].[UnitPrice] as 'UnitPrice' 
   FROM main.OrderDetails as [od] 
   WHERE (([od].[Quantity] > @param10243793270nested1) 
     AND ([od].[OrderID] = [o].[OrderID]))
)))
-- params @param10243793270nested1 - 10; 
```

### Select with subquery not exists parameter from main query

```fsharp
query {
    for o in dc.Main.Orders do
    where(not(query {
            for od in dc.Main.OrderDetails do
            where (od.Quantity < (int16 10))
            exists(od.OrderId = o.OrderId)
        }))
    select o.OrderId
}
```

```SQL
SELECT [o].[OrderID] as 'OrderID' 
FROM main.Orders as [o] 
WHERE ((NOT EXISTS (
   SELECT [od].[Discount] as 'Discount',
   [od].[OrderID] as 'OrderID',
   [od].[ProductID] as 'ProductID',
   [od].[Quantity] as 'Quantity',
   [od].[UnitPrice] as 'UnitPrice' 
   FROM main.OrderDetails as [od] 
   WHERE (([od].[Quantity] < @param12721273480nested1) 
      AND ([od].[OrderID] = [o].[OrderID]))
))) 
-- params @param12721273480nested1 - 10; 
```

## Some not-so-practical queries to display the query translation capabilities

### Canonical operations query

```fsharp
let L = "L"
query {
    for cust in dc.Main.Customers do
    join emp in dc.Main.Employees 
       on (cust.City.Trim() + "x" + cust.Country = 
           emp.City.Trim() + "x" + emp.Country)
    join secondCust in dc.Main.Customers 
       on (cust.City + emp.City + "A" = 
           secondCust.City + secondCust.City + "A")
    where (
        cust.City + emp.City + cust.City + emp.City + 
           cust.City = cust.City + emp.City + cust.City + 
           emp.City + cust.City
        && abs(emp.EmployeeId)+1L > 4L 
        && cust.City.Length + secondCust.City.Length + 
             emp.City.Length = 3 * cust.City.Length
        && (cust.City.Replace("on","xx") + L).Replace("xx","on") 
            + ("O" + L) = "London" + "LOL" 
        && cust.City.IndexOf("n")>0 
        && cust.City.IndexOf(
              cust.City.Substring(1,cust.City.Length-1))>0
        && Math.Max(emp.BirthDate.Date.AddYears(3).Month +
               1, 0) > 3
        && emp.BirthDate.AddDays(1.).Subtract(emp.BirthDate).Days=1
    )
    sortBy (abs(abs(emp.BirthDate.Day * emp.BirthDate.Day)))
    select (cust.CustomerId, cust.City, emp.BirthDate)
    distinct
}
```

Silly query not hitting indexes, but a show-case that SQLProvider can translate.

```SQL
 SELECT DISTINCT 
     [cust].[CustomerID] as '[cust].[CustomerID]',
     [cust].[City] as '[cust].[City]',
     [emp].[BirthDate] as '[emp].[BirthDate]' 
FROM main.Customers as [cust] 
INNER JOIN  [main].[Employees] as [emp] 
   on ((TRIM([cust].[City]) || @param1) || [cust].[Country]) = 
      ((TRIM([emp].[City]) || @param2) || [emp].[Country]) 
INNER JOIN  [main].[Customers] as [secondCust] 
   on (([cust].[City] || [emp].[City]) || @param3) = 
      (([secondCust].[City] || [secondCust].[City]) || @param4) 
WHERE (((((((([cust].[City] || [emp].[City]) || [cust].[City]) 
       || [emp].[City]) || [cust].[City]) = 
          (((([cust].[City] || [emp].[City]) 
       || [cust].[City]) || [emp].[City]) || [cust].[City]) 
      AND (ABS([emp].[EmployeeID]) + 1) > @param6) 
   AND (((LENGTH([cust].[City]) + LENGTH([secondCust].[City])) + 
     LENGTH([emp].[City])) = (3 * LENGTH([cust].[City])) 
   AND (REPLACE((REPLACE([cust].[City],@param8,@param9) 
     || @param10),@param11,@param12) || @param13) = @param14)) 
   AND ((INSTR([cust].[City],@param15) > @param16 
   AND INSTR([cust].[City],
      SUBSTR([cust].[City], @param17, 
         (LENGTH([cust].[City]) - 1))) > @param18) 
   AND (MAX((CAST(STRFTIME('%m', 
      DATETIME(DATE([emp].[BirthDate]), '+3 year')) as 
        INTEGER) + 1), @param19) > @param20
    AND CAST(JULIANDAY(DATETIME([emp].[BirthDate], '+1 day')) - 
        JULIANDAY([emp].[BirthDate]) as INTEGER) = @param21)))) 
ORDER BY 
   ABS(ABS((CAST(STRFTIME('%d', [emp].[BirthDate]) as INTEGER) * 
   CAST(STRFTIME('%d', [emp].[BirthDate]) as INTEGER))))  
-- params @param1 - "x"; @param2 - "x"; @param3 - "A"; 
--   @param4 - "A"; @param6 - 4L; @param8 - "on"; 
--   @param9 - "xx"; @param10 - "L"; @param11 - "xx"; 
--   @param12 - "on"; @param13 - "OL"; @param14 - "LondonLOL"; 
--   @param15 - "n"; @param16 - 0; @param17 - 1; @param18 - 0; 
--   @param19 - 0; @param20 - 3; @param21 - 1; 
```

### Select with over 8 joins
```fsharp
query {
    for cust in dc.Main.Customers do
    join ord1 in dc.Main.Orders 
       on (cust.CustomerId = ord1.CustomerId)
    join ord2 in dc.Main.Orders 
       on (cust.CustomerId = ord2.CustomerId)
    join ord3 in dc.Main.Orders 
       on (cust.CustomerId = ord3.CustomerId)
    join ord4 in dc.Main.Orders 
       on (cust.CustomerId = ord4.CustomerId)
    join ord5 in dc.Main.Orders 
       on (cust.CustomerId = ord5.CustomerId)
    join ord6 in dc.Main.Orders 
       on (cust.CustomerId = ord6.CustomerId)
    join ord7 in dc.Main.Orders 
       on (cust.CustomerId = ord7.CustomerId)
    join ord8 in dc.Main.Orders 
       on (cust.CustomerId = ord8.CustomerId)
    join ord9 in dc.Main.Orders 
       on (cust.CustomerId = ord9.CustomerId)
    where (cust.CustomerId = "ALFKI"
        && ord1.OrderId = 10643L 
        && ord2.OrderId = 10643L 
        && ord3.OrderId = 10643L
        && ord4.OrderId = 10643L 
        && ord5.OrderId = 10643L 
        && ord6.OrderId = 10643L
        && ord7.OrderId = 10643L 
        && ord8.OrderId = 10643L 
        && ord9.OrderId = 10643L
        )
    select (Some (cust, ord9))
    exactlyOneOrDefault
}
```

```SQL
SELECT 
   [cust].[Address] as '[cust].[Address]',
   [cust].[City] as '[cust].[City]',
   [cust].[CompanyName] as '[cust].[CompanyName]',
   [cust].[ContactName] as '[cust].[ContactName]',
   [cust].[ContactTitle] as '[cust].[ContactTitle]',
   [cust].[Country] as '[cust].[Country]',
   [cust].[CustomerID] as '[cust].[CustomerID]',
   [cust].[Fax] as '[cust].[Fax]',
   [cust].[Phone] as '[cust].[Phone]',
   [cust].[PostalCode] as '[cust].[PostalCode]',
   [cust].[Region] as '[cust].[Region]',
   [ord9].[CustomerID] as '[ord9].[CustomerID]',
   [ord9].[EmployeeID] as '[ord9].[EmployeeID]',
   [ord9].[Freight] as '[ord9].[Freight]',
   [ord9].[OrderDate] as '[ord9].[OrderDate]',
   [ord9].[OrderID] as '[ord9].[OrderID]',
   [ord9].[RequiredDate] as '[ord9].[RequiredDate]',
   [ord9].[ShipAddress] as '[ord9].[ShipAddress]',
   [ord9].[ShipCity] as '[ord9].[ShipCity]',
   [ord9].[ShipCountry] as '[ord9].[ShipCountry]',
   [ord9].[ShipName] as '[ord9].[ShipName]',
   [ord9].[ShipPostalCode] as '[ord9].[ShipPostalCode]',
   [ord9].[ShipRegion] as '[ord9].[ShipRegion]',
   [ord9].[ShippedDate] as '[ord9].[ShippedDate]' 
FROM main.Customers as [cust] 
INNER JOIN  [main].[Orders] as [ord1] 
   on [cust].[CustomerID] = [ord1].[CustomerID] 
INNER JOIN  [main].[Orders] as [ord2] 
   on [cust].[CustomerID] = [ord2].[CustomerID] 
INNER JOIN  [main].[Orders] as [ord3] 
   on [cust].[CustomerID] = [ord3].[CustomerID] 
INNER JOIN  [main].[Orders] as [ord4] 
   on [cust].[CustomerID] = [ord4].[CustomerID] 
INNER JOIN  [main].[Orders] as [ord5] 
   on [cust].[CustomerID] = [ord5].[CustomerID] 
INNER JOIN  [main].[Orders] as [ord6] 
   on [cust].[CustomerID] = [ord6].[CustomerID] 
INNER JOIN  [main].[Orders] as [ord7] 
   on [cust].[CustomerID] = [ord7].[CustomerID] 
INNER JOIN  [main].[Orders] as [ord8] 
   on [cust].[CustomerID] = [ord8].[CustomerID] 
INNER JOIN  [main].[Orders] as [ord9] 
   on [cust].[CustomerID] = [ord9].[CustomerID] 
WHERE (([ord9].[OrderID] = @param1 
   AND ([ord8].[OrderID] = @param2 
      AND ((([cust].[CustomerID] = @param3 
      AND [ord1].[OrderID] = @param4) 
      AND ([ord2].[OrderID] = @param5 
         AND [ord3].[OrderID] = @param6)) 
      AND (([ord4].[OrderID] = @param7 
         AND [ord5].[OrderID] = @param8) 
         AND ([ord6].[OrderID] = @param9 
            AND [ord7].[OrderID] = @param10))))
)) 
-- params @param1 - 10643L; @param2 - 10643L; 
--   @param3 - "ALFKI"; @param4 - 10643L; 
--   @param5 - 10643L; @param6 - 10643L; 
--   @param7 - 10643L; @param8 - 10643L; 
--   @param9 - 10643L; @param10 - 10643L; 
```

### Select with subquery of subqueries

```fsharp
let subquery (subQueryIds:IQueryable<string>) = 
    query {
        for cust in dc.Main.Customers do
        where(subQueryIds.Contains(cust.CustomerId))
        select cust.CustomerId
    }
let subquery2 = 
    query {
        for custc in dc.Main.Customers do
        where(custc.City |=| (query {
                    for ord in dc.Main.Orders do
                    where(ord.ShipCity ="Helsinki")
                    distinct
                    select ord.ShipCity
                }))
        select custc.CustomerId //"WILMK"
    }
let initial1 = ["ALFKI"].AsQueryable()
let initial2 = ["ANATR"].AsQueryable()
let initial3 = ["AROUT"].AsQueryable()
let qry = 
    query {
        for cust in dc.Main.Customers do
        where(
            subquery(subquery(subquery(subquery(initial1))))
                .Contains(cust.CustomerId) ||
            subquery(subquery(subquery(subquery(initial2))))
                .Contains(cust.CustomerId) || 
            subquery(initial3).Contains(cust.CustomerId) ||
            subquery2.Contains(cust.CustomerId))
        select cust.CustomerId
    }
```

There can be a lot of IN-sub-queries nested.
Nested query parameters are using nested hashcodes to avoid collision in naming. 

```SQL
SELECT [cust].[CustomerID] as 'CustomerID' 
FROM main.Customers as [cust] 
WHERE (([cust].[CustomerID] IN (
   SELECT [custc].[CustomerID] as 'CustomerID' 
   FROM main.Customers as [custc] 
   WHERE (([custc].[City] IN (
      SELECT DISTINCT [ord].[ShipCity] as 'ShipCity' 
      FROM main.Orders as [ord] 
      WHERE (([ord].[ShipCity] = @param14833652930nested15217231600nested1)))))) 
         OR ([cust].[CustomerID] IN (
            SELECT [cust].[CustomerID] as 'CustomerID' 
            FROM main.Customers as [cust] 
            WHERE (([cust].[CustomerID] IN (@param1751753631nested1)))
         ) OR ([cust].[CustomerID] IN (
            SELECT [cust].[CustomerID] as 'CustomerID' 
            FROM main.Customers as [cust] 
            WHERE (([cust].[CustomerID] IN (
               SELECT [cust].[CustomerID] as 'CustomerID' 
               FROM main.Customers as [cust] 
               WHERE (([cust].[CustomerID] IN (
                  SELECT [cust].[CustomerID] as 'CustomerID' 
                  FROM main.Customers as [cust] 
                  WHERE (([cust].[CustomerID] IN (
                     SELECT [cust].[CustomerID] as 'CustomerID' 
                     FROM main.Customers as [cust] 
                     WHERE (([cust].[CustomerID] IN (@param1893332643nested3449698180nested13871977310nested1751753630nested1)
        )))))))))))) OR [cust].[CustomerID] IN (
           SELECT [cust].[CustomerID] as 'CustomerID' 
           FROM main.Customers as [cust] 
           WHERE (([cust].[CustomerID] IN (
              SELECT [cust].[CustomerID] as 'CustomerID' 
              FROM main.Customers as [cust] 
              WHERE (([cust].[CustomerID] IN (
                 SELECT [cust].[CustomerID] as 'CustomerID' 
                 FROM main.Customers as [cust] 
                 WHERE (([cust].[CustomerID] IN (
                    SELECT [cust].[CustomerID] as 'CustomerID' 
                    FROM main.Customers as [cust] 
                    WHERE (([cust].[CustomerID] IN (@param1893332644nested3449698180nested13871977310nested1751753630nested1)
)))))))))))))))) 
-- params @param14833652930nested15217231600nested1 - "Helsinki"; 
--   @param1751753631nested1 - "AROUT";
--   @param1893332643nested3449698180nested13871977310nested1751753630nested1 - "ALFKI";
--   @param1893332644nested3449698180nested13871977310nested1751753630nested1 - "ANATR"; 
```

