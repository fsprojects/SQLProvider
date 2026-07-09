namespace FSharp.Data.Sql.Transactions

open System

// SQLProvider's transaction model, in one place so it isn't misread:
//  * A TransactionScope is created ONLY on the write path - each provider's
//    ProcessUpdates/ProcessUpdatesAsync (i.e. SubmitUpdates/SubmitUpdatesAsync).
//    Query/read execution is intentionally NOT wrapped in a transaction (no overhead).
//  * The scope uses TransactionScopeOption.Required (see ensureTransaction), so if the
//    caller has opened their own ambient System.Transactions scope, SubmitUpdates JOINS it
//    and the caller's isolation level wins - TransactionOptions here are then irrelevant.
//  * The isolation level below only takes effect for a STANDALONE write batch (when there is
//    no ambient transaction). Choosing isolation for a read-then-write unit of work is the
//    caller's job: wrap the reads and the SubmitUpdates in one ambient transaction yourself.
//    (SQLProvider even exposes separate read/write data-context types to help structure this.)
// So TransactionOptions.Default = Serializable is NOT a global "everything runs Serializable",
// it is the fallback isolation for an unscoped write batch only.
//
// Recommended usage:
//  * Pure reads: use a read-only data context (GetReadOnlyDataContext()) - no transaction, no locking overhead.
//  * Writes / read-then-write: open your OWN ambient scope with the isolation you want, e.g.
//      use scope = new System.Transactions.TransactionScope(
//                      System.Transactions.TransactionScopeOption.Required,
//                      System.Transactions.TransactionOptions(IsolationLevel = <your level>),
//                      System.Transactions.TransactionScopeAsyncFlowOption.Enabled)
//    do your reads + SubmitUpdates() inside it (SubmitUpdates enlists via Required), then scope.Complete().
//    A bare `new TransactionScope(AsyncFlowOption.Enabled)` roots a SERIALIZABLE transaction (the .NET default) -
//    pass explicit TransactionOptions if you want ReadCommitted etc.
//  * A read that must run inside a write transaction: call .AsReadOnly() on the writable context to reuse its connection.

/// Transaction isolation level for database operations.
/// Corresponds to the System.Transactions.IsolationLevel but provides SQL provider specific functionality.
/// Only applied when SubmitUpdates creates its own (standalone) write transaction; if you wrap your
/// own ambient System.Transactions scope, that scope's isolation level is used instead.
type IsolationLevel =
    /// Highest isolation level - transactions are completely isolated from each other
    | Serializable = 0
    /// Prevents dirty reads and non-repeatable reads, but phantom reads can occur
    | RepeatableRead = 1
    /// Prevents dirty reads but allows non-repeatable reads and phantom reads.
    /// (This describes the default isolation of most DB *engines* - it is NOT SQLProvider's default;
    /// TransactionOptions.Default is Serializable, inherited from System.Transactions.)
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

/// Configuration options for database transactions.
/// Corresponds to the System.Transactions.TransactionOptions.
[<Struct>]
type TransactionOptions = {
    /// Maximum time the transaction can remain active before timing out
    Timeout : TimeSpan
    /// The isolation level for the transaction
    IsolationLevel : IsolationLevel
}

/// Utility functions for converting between different transaction isolation level representations.
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

    /// Converts SQL provider isolation level to System.Data.IsolationLevel.
    /// Use this when you need to work directly with ADO.NET connection transactions.
    /// param isolationLevel: The SQL provider isolation level
    /// returns: The corresponding System.Data.IsolationLevel
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

    /// Creates the TransactionScope that wraps a SubmitUpdates write batch. Called ONLY from the
    /// providers' ProcessUpdates/ProcessUpdatesAsync - reads are never wrapped. Uses
    /// TransactionScopeOption.Required: if the caller already opened an ambient transaction this
    /// enlists in it (and the caller's isolation level applies, not the one passed here); otherwise
    /// it starts a fresh standalone transaction using transactionOptions. Pass
    /// IsolationLevel.DontCreateTransaction to skip creating a scope entirely.
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
    /// Inherits System.Transactions' defaults: Timeout = 1 minute and IsolationLevel = Serializable.
    /// The Serializable level only applies to a standalone SubmitUpdates write batch (no ambient
    /// transaction); wrap your read-then-write work in your own scope to control isolation. See the
    /// note on the IsolationLevel type above.
    static member Default =
        let sysTranOpt = System.Transactions.TransactionOptions()
        {
            Timeout = sysTranOpt.Timeout
            IsolationLevel = TransactionUtils.fromSystemTransactionsIsolationLevel sysTranOpt.IsolationLevel
        }
