namespace FSharp.Data.Sql.Transactions

open System

/// Corresponds to the System.Transactions.IsolationLevel.
type IsolationLevel =
    | Serializable = 0
    | RepeatableRead = 1
    | ReadCommitted = 2
    | ReadUncommitted = 3
    | Snapshot = 4
    | Chaos = 5
    | Unspecified = 6
    | DontCreateTransaction = 99

/// Corresponds to the System.Transactions.TransactionOptions.
type TransactionOptions = {
    Timeout : TimeSpan
    IsolationLevel : IsolationLevel
}

module internal TransactionUtils =
    let toSystemTransactionsIsolationLevel isolationLevel =
        match isolationLevel with
        | IsolationLevel.Serializable -> System.Transactions.IsolationLevel.Serializable
        | IsolationLevel.RepeatableRead -> System.Transactions.IsolationLevel.RepeatableRead
        | IsolationLevel.ReadCommitted -> System.Transactions.IsolationLevel.ReadCommitted
        | IsolationLevel.ReadUncommitted -> System.Transactions.IsolationLevel.ReadUncommitted
        | IsolationLevel.Snapshot -> System.Transactions.IsolationLevel.Snapshot
        | IsolationLevel.Chaos -> System.Transactions.IsolationLevel.Chaos
        | IsolationLevel.Unspecified -> System.Transactions.IsolationLevel.Unspecified
        | _ -> failwithf "Unhandled IsolationLevel value: %A." isolationLevel

    let fromSystemTransactionsIsolationLevel isolationLevel =
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
            new Transactions.TransactionOptions(
                Timeout = transactionOptions.Timeout,
                IsolationLevel = toSystemTransactionsIsolationLevel transactionOptions.IsolationLevel)
        
        let transactionScopeOption = Transactions.TransactionScopeOption.Required // Default

        // Todo: Take TransactionScopeAsyncFlowOption into use when implemented in Mono.
        // Without it, transactions are not thread-safe over threads e.g. using async can be dangerous)
        // However, default option for TransactionScopeOption is Required, so you can create top level transaction
        // and this Mono-transaction will have its properties.
        let isMono = Type.GetType ("Mono.Runtime") <> null
        match isMono with
        | true -> new Transactions.TransactionScope(transactionScopeOption, transactionOptions)
        | false ->
            // Note1: On Mono, 4.6.1 or newer is requred for compiling TransactionScopeAsyncFlowOption.
            new Transactions.TransactionScope(transactionScopeOption, transactionOptions, System.Transactions.TransactionScopeAsyncFlowOption.Enabled)

type TransactionOptions with
    static member Default =
        let sysTranOpt = new System.Transactions.TransactionOptions()
        {
            Timeout = sysTranOpt.Timeout
            IsolationLevel = TransactionUtils.fromSystemTransactionsIsolationLevel sysTranOpt.IsolationLevel
        }
