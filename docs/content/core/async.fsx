(*** hide ***)
#I @"../../files/sqlite"

(*** hide ***)
#I "../../../bin"

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
is release the business logics thread while the database is doing its job. 
This can lead a huge performance difference on heavy traffic environment 
(basically, will your business logics server / web-server crash or not). 

![](http://i.imgur.com/DBPLRlP.png)

In the picture, we talk about the red block, which can be released to serve other customers.
As usual with async operations, there will happen some more thread context switching, 
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

Consept for async queries is this:

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

* Seq.executeQueryAsync : IQueryable<'a> -> Async<seq<'a>>
* Seq.lengthAsync : IQueryable<'a> -> Async<int>
* Seq.headAsync : IQueryable<'a> -> Async<'a>
* Seq.tryHeadAsync : IQueryable<'a> -> Async<'a option>
* and for your data context: SubmitUpdatesAsync : unit -> Async<Unit>


#### Database asynchrony can't be used as a way to do parallelism inside one context. 

Usually database operations can't be executed as parallel inside one context/transaction. 
That is an anti-pattern in general: the network lag between database and your logics server 
is probably the bottle-neck of your system. So, in this order: 

1. Try to execute your business logics as database queries, as one big query.
2. Or sometimes, not often, load eagerly data with single query and process it in the logics server.
3. Avoid case that you create as many queries as your collection has items.

So if you are still in the worst case, 3, and have to deal with a List<Async<T>>, you cannot 
say Async.Parallel as that may corrupt your data. To avoid custom imperative while-loops, 
we have provided a little helper function for you, that is List.evaluateOneByOne.

Avoid network traffic between business logics (BL) and database (DB). 
When you exit the query-computation, you cause the traffic.

*)