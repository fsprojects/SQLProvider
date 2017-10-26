(*** hide ***)
#I @"../../files/sqlite"

(*** hide ***)
#I "../../../bin/net451"

(*** hide ***)
#if INTERACTIVE
#r "FSharp.Data.SqlProvider.dll"
#r @"System.Transactions.dll"
#endif
(*** hide ***)
open FSharp.Data.Sql
(*** hide ***)
type TypeProviderConnection = 
    SqlDataProvider<
        ConnectionString = "Server=localhost;Database=test;User=test;Password=test",
        DatabaseVendor = Common.DatabaseProviderTypes.MSSQLSERVER,
        IndividualsAmount=1000,
        UseOptionTypes=true, 
        CaseSensitivityChange = Common.CaseSensitivityChange.ORIGINAL>

(**

# Asynchronous Database Operation

You get more performance by concurrency. The idea of async database operations 
is to release the business logics thread while the database is doing its job. 
This can lead a huge performance difference on heavy traffic environment 
(basically, will your business logics server / web-server crash or not). 

![](https://i.imgur.com/DBPLRlP.png)

In the picture, we talk about the red block, which can be released to serve other customers.
As usual with async operations, there will be more thread context switching, 
which may cause minor performance delays, but concurrency benefits should outweigh the
context switching cons.

This is the theory. In practice SQLProvider is calling implementation of async methods from
abstract classes under System.Data.Common. The implementation quality of your database 
connection .NET drivers will define if async is good for you or not. (E.g. The current 
situation is that MS-SQL-server handles async well and MySQL not so.)

Currently SQLProvider supports async operations on runtime, not design-time.

Your execution thread may change. For transactions to support this, 
.NET 4.5.1 has a fix for asynchronous transactions that has to be explicitly used.

### Async queries and updates

Concept for async queries is this:

*)

open System
open System.Threading.Tasks
open FSharp.Data.Sql

type MyWebServer() = 
    member __.``Execute Business Logics`` (id : Guid) : Task<_> = 
        async {
            use transaction = 
                new System.Transactions.TransactionScope(
                // .NET 4.5.1 fix for asynchronous transactions:
                    System.Transactions.TransactionScopeAsyncFlowOption.Enabled
                )
            let context = TypeProviderConnection.GetDataContext cstr
            let! fetched =
                query {
                    for  t2 in context.MyDataBase.MyTable2 do
                    join t1 in context.MyDataBase.MyTable1 on (t2.ForeignId = t1.Id)
                    where (t2.Id = id)
                    select (t1)
                } |> Seq.executeQueryAsync

            fetched |> Seq.iter (fun entity ->
                entity.SetColumn("Updated", DateTime.UtcNow |> box)
            )
            do! context.SubmitUpdatesAsync()

            transaction.Complete()
            return "done!"
        } |> Async.StartAsTask

(**

The functions to work with asynchrony are:

* Array.executeQueryAsync : IQueryable<'a> -> Async<<'a> []>
* List.executeQueryAsync : IQueryable<'a> -> Async<'a list>
* Seq.executeQueryAsync : IQueryable<'a> -> Async<seq<'a>>
* Seq.lengthAsync : IQueryable<'a> -> Async<int>
* Seq.headAsync : IQueryable<'a> -> Async<'a>
* Seq.tryHeadAsync : IQueryable<'a> -> Async<'a option>
* and for your data context: SubmitUpdatesAsync : unit -> Async<Unit>
* Seq.sumAsync : IQueryable<'a when 'a : comparison> -> Async<'a>
* Seq.minAsync : IQueryable<'a when 'a : comparison> -> Async<'a>
* Seq.maxAsync : IQueryable<'a when 'a : comparison> -> Async<'a>
* Seq.averageAsync : IQueryable<'a when 'a : comparison> -> Async<'a>
 
Seq is .NET IEnumerable, which is lazy. So be careful if using Seq.executeQueryAsync 
to not execute your queries several times.

Also stored procedures do support InvokeAsync.

#### Database asynchrony can't be used as a way to do parallelism inside one context. 

Usually database operations can't be executed as parallel inside one context/transaction. 
That is an anti-pattern in general: the network lag between database and your logics server 
is probably the bottleneck of your system. So, in this order: 

1. Try to execute your business logics as database queries, as one big query.
2. Or sometimes, not often, load eagerly data with single query and process it in the logics server.
3. Avoid case that you create as many queries as your collection has items.

So if you are still in the worst case, 3, and have to deal with a List<Async<T>>, you cannot 
say Async.Parallel as that may corrupt your data. To avoid custom imperative while-loops, 
we have provided a little helper function for you, that is List.evaluateOneByOne.

Avoid network traffic between business logics (BL) and database (DB). 
When you exit the query-computation, you cause the traffic.

### Why Not to Use Async

As with all the technical choices, there are drawbacks to consider.

* Your codebase will be more complex. This will slow down your development speed if your developers are not F#-professionals.
* You have to use other technologies that support async or .NET tasks, like WCF or SignalR. There is no point of doing async and then still using `RunSynchronously` at the end.
* You may consider async as premature optimization. Starting without async and converting all later is an option, although your APIs will have to change.
* Async and transactions is a problem with Mono environment.
* Async will make your error stacktraces are harder to read: You may be used to search your functions from the stacktrace to spot any problems. With async, you don't have your own code in the error-stack. At the time of e.g. SQL-exception, there is no thread waiting, your code is not actively running, there is no stack.

*)
