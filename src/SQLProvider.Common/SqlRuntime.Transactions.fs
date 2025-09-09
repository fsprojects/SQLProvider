namespace FSharp.Data.Sql.Transactions

open System

/// <summary>
/// Transaction isolation level for database operations.
/// Corresponds to the System.Transactions.IsolationLevel but provides SQL provider specific functionality.
/// </summary>
type IsolationLevel =
    /// Highest isolation level - transactions are completely isolated from each other
    | Serializable = 0
    /// Prevents dirty reads and non-repeatable reads, but phantom reads can occur
    | RepeatableRead = 1
    /// Prevents dirty reads but allows non-repeatable reads and phantom reads (default for most databases)
    | ReadCommitted = 2
    /// Allows dirty reads, non-repeatable reads, and phantom reads (fastest but least safe)
    | ReadUncommitted = 3
    /// SQL Server specific - provides statement-level read consistency using versioning
    | Snapshot = 4
    /// Allows any level of isolation, including overlapping changes (SQL Server specific)
    | Chaos = 5
    /// Use the default isolation level of the database
    | Unspecified = 6
    /// Special value to indicate that no transaction should be created
    | DontCreateTransaction = 99

/// <summary>
/// Configuration options for database transactions.
/// Corresponds to the System.Transactions.TransactionOptions.
/// </summary>
[<Struct>]
type TransactionOptions = {
    /// Maximum time the transaction can remain active before timing out
    Timeout : TimeSpan
    /// The isolation level for the transaction
    IsolationLevel : IsolationLevel
}

/// <summary>
/// Utility functions for converting between different transaction isolation level representations.
/// </summary>
module TransactionUtils =
    let internal toSystemTransactionsIsolationLevel isolationLevel =
        match isolationLevel with
        | IsolationLevel.Serializable -> System.Transactions.IsolationLevel.Serializable
        | IsolationLevel.RepeatableRead -> System.Transactions.IsolationLevel.RepeatableRead
        | IsolationLevel.ReadCommitted -> System.Transactions.IsolationLevel.ReadCommitted
        | IsolationLevel.ReadUncommitted -> System.Transactions.IsolationLevel.ReadUncommitted
        | IsolationLevel.Snapshot -> System.Transactions.IsolationLevel.Snapshot
        | IsolationLevel.Chaos -> System.Transactions.IsolationLevel.Chaos
        | IsolationLevel.Unspecified -> System.Transactions.IsolationLevel.Unspecified
        | _ -> failwithf "Unhandled IsolationLevel value: %A." isolationLevel

    /// <summary>
    /// Converts SQL provider isolation level to System.Data.IsolationLevel.
    /// Use this when you need to work directly with ADO.NET connection transactions.
    /// </summary>
    /// <param name="isolationLevel">The SQL provider isolation level</param>
    /// <returns>The corresponding System.Data.IsolationLevel</returns>
    let toSystemDataIsolationLevel isolationLevel =
        match isolationLevel with
        | IsolationLevel.Serializable -> System.Data.IsolationLevel.Serializable
        | IsolationLevel.RepeatableRead -> System.Data.IsolationLevel.RepeatableRead
        | IsolationLevel.ReadCommitted -> System.Data.IsolationLevel.ReadCommitted
        | IsolationLevel.ReadUncommitted -> System.Data.IsolationLevel.ReadUncommitted
        | IsolationLevel.Snapshot -> System.Data.IsolationLevel.Snapshot
        | IsolationLevel.Chaos -> System.Data.IsolationLevel.Chaos
        | IsolationLevel.Unspecified -> System.Data.IsolationLevel.Unspecified
        | _ -> failwithf "Unhandled IsolationLevel value: %A." isolationLevel

    let internal fromSystemTransactionsIsolationLevel isolationLevel =
        match isolationLevel with
        | System.Transactions.IsolationLevel.Serializable -> IsolationLevel.Serializable
        | System.Transactions.IsolationLevel.RepeatableRead -> IsolationLevel.RepeatableRead
        | System.Transactions.IsolationLevel.ReadCommitted -> IsolationLevel.ReadCommitted
        | System.Transactions.IsolationLevel.ReadUncommitted -> IsolationLevel.ReadUncommitted
        | System.Transactions.IsolationLevel.Snapshot -> IsolationLevel.Snapshot
        | System.Transactions.IsolationLevel.Chaos -> IsolationLevel.Chaos
        | System.Transactions.IsolationLevel.Unspecified -> IsolationLevel.Unspecified
        | _ -> failwithf "Unhandled System.Transactions.IsolationLevel value: %A." isolationLevel

    let ensureTransaction (transactionOptions : TransactionOptions) =
        if transactionOptions.IsolationLevel = IsolationLevel.DontCreateTransaction then
            Unchecked.defaultof<Transactions.TransactionScope>
        else
        let transactionOptions =
            Transactions.TransactionOptions(
                Timeout = transactionOptions.Timeout,
                IsolationLevel = toSystemTransactionsIsolationLevel transactionOptions.IsolationLevel)
        
        let transactionScopeOption = Transactions.TransactionScopeOption.Required // Default

        // Todo: Take TransactionScopeAsyncFlowOption into use when implemented in Mono.
        // Without it, transactions are not thread-safe over threads e.g. using async can be dangerous)
        // However, default option for TransactionScopeOption is Required, so you can create top level transaction
        // and this Mono-transaction will have its properties.
        let isMono = not (isNull(Type.GetType "Mono.Runtime"))
        match isMono with
        | true -> new Transactions.TransactionScope(transactionScopeOption, transactionOptions)
        | false ->
            // Note1: On Mono, 4.6.1 or newer is requred for compiling TransactionScopeAsyncFlowOption.
            new Transactions.TransactionScope(transactionScopeOption, transactionOptions, System.Transactions.TransactionScopeAsyncFlowOption.Enabled)

type TransactionOptions with
    static member Default =
        let sysTranOpt = System.Transactions.TransactionOptions()
        {
            Timeout = sysTranOpt.Timeout
            IsolationLevel = TransactionUtils.fromSystemTransactionsIsolationLevel sysTranOpt.IsolationLevel
        }
