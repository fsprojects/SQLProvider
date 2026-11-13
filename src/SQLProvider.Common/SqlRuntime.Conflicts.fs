namespace FSharp.Data.Sql

open System
open System.Runtime.CompilerServices
open FSharp.Quotations
open FSharp.Quotations.Patterns
open FSharp.Linq.RuntimeHelpers
open FSharp.Data.Sql.Common

/// Reads the typed conflict lambdas: a provided property reads and writes its column through an erased GetColumn or
/// SetColumn call with the column's name, so the quotation of a lambda over an entity names the columns it touches.
module internal ConflictQuotations =

    let private isRow (row: Var) (expr: Expr) =
        match expr with
        | Var v
        | Coerce(Var v, _) -> v = row
        | _ -> false

    /// The column a property read of the row stands for.
    let rec private (|ColumnRead|_|) (row: Var) (expr: Expr) =
        match expr with
        | Coerce(inner, _) -> (|ColumnRead|_|) row inner
        | Call(Some target, meth, [ Value(:? string as name, _) ]) when isRow row target && meth.Name.StartsWith "GetColumn" ->
            Some name
        | _ -> None

    /// The value of an expression that doesn't read the rows, Some and ValueSome unwrapped and None as null.
    let private constantOf (expr: Expr) =
        match LeafExpressionConverter.EvaluateQuotation expr with
        | null -> null
        | value ->
            let t = value.GetType()

            if t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<option<_>> then
                t.GetProperty("Value").GetValue value
            elif t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<voption<_>> then
                if t.GetProperty("IsSome").GetValue value :?> bool then t.GetProperty("Value").GetValue value else null
            else
                value

    /// The columns of `fun row -> row.A` or `fun row -> (row.A, row.B)`.
    let columnsOf (columns: Expr) =
        let refused () =
            failwith "The conflict's columns are a property of the row, or a tuple of them: fun row -> row.Code or fun row -> (row.Code, row.Region)."

        match columns with
        | Lambda(row, body) ->
            match body with
            | ColumnRead row name -> [ name ]
            | NewTuple items -> [ for item in items -> match item with ColumnRead row name -> name | _ -> refused () ]
            | Coerce(NewTuple items, _) -> [ for item in items -> match item with ColumnRead row name -> name | _ -> refused () ]
            | _ -> refused ()
        | _ -> refused ()

    /// What `fun existing inserted -> existing.A <- inserted.A; existing.B <- 42` sets: each column to the inserted row's
    /// value of the same column, or to a value that doesn't read the rows.
    let updatesOf (update: Expr) =
        match update with
        | Lambda(existing, Lambda(inserted, body)) ->
            let assignment (column: string) (value: Expr) =
                let reads = value.GetFreeVars() |> Seq.exists (fun v -> v = existing || v = inserted)

                match value with
                | ColumnRead inserted source when source = column -> Inserted column
                | ColumnRead inserted source ->
                    failwithf "%s gets the inserted %s: a column can take the inserted row's value of itself only." column source
                | _ when reads ->
                    failwithf "%s gets a value computed from the rows: a column can take the inserted row's value of itself, or a value known before the insert." column
                | _ -> Constant(column, constantOf value)

            let isSetter (target: Expr) (meth: Reflection.MethodInfo) =
                isRow existing target && meth.Name.StartsWith "SetColumn"

            let rec statements expr =
                match expr with
                | Sequential(first, rest) -> statements first @ statements rest
                | Value(_, t) when t = typeof<unit> -> []
                // A property set expands to the setter's call, its value bound first
                | Let(bound, value, Call(Some target, meth, [ Value(:? string as column, _); Var used ])) when
                    used = bound && isSetter target meth
                    ->
                    [ assignment column value ]
                | Call(Some target, meth, [ Value(:? string as column, _); value ]) when isSetter target meth ->
                    [ assignment column value ]
                | _ ->
                    failwith "The update sets properties of the existing row: fun existing inserted -> existing.Name <- inserted.Name; existing.Note <- \"merged\"."

            let updates = statements body

            if updates.IsEmpty then
                failwith "The update sets at least one property of the existing row: fun existing inserted -> existing.Name <- inserted.Name. To leave the row as it is, use OnConflictDoNothing."

            let columns = [ for update in updates -> match update with Inserted column | Constant(column, _) -> column ]

            match columns |> List.countBy id |> List.tryFind (fun (_, times) -> times > 1) with
            | Some(column, _) -> failwithf "The update sets %s more than once." column
            | None -> updates
        | _ -> failwith "The update is a lambda of the existing and the inserted row: fun existing inserted -> existing.Name <- inserted.Name."

/// Typed conflict handling for an inserted row (OnConflict.DoNothingOn and UpdateOn, with the columns checked by the
/// compiler).
[<Extension>]
type SqlEntityConflictExtensions =

    /// <summary>When the inserted row breaks the unique constraint or index on exactly the given columns, the existing row
    /// stays as it is: <c>ON CONFLICT (columns) DO NOTHING</c>.</summary>
    /// <param name="row">The row to insert.</param>
    /// <param name="conflictColumns">The constraint's or index's columns: a property of the row, or a tuple of them, e.g.
    /// <c>fun r -> r.Code</c> or <c>fun r -> (r.Code, r.Region)</c>.</param>
    /// <remarks>Sets <c>row.OnConflict</c> to <c>OnConflict.DoNothingOn</c>. PostgreSQL, SQLite 3.24+ and DuckDB; MySQL
    /// and the databases without an upsert refuse it when the row is submitted.</remarks>
    [<Extension>]
    static member OnConflictDoNothing<'T, 'Key when 'T :> SqlEntity>
        (row: 'T, [<ReflectedDefinition>] conflictColumns: Expr<'T -> 'Key>)
        =
        row.OnConflict <- DoNothingOn(ConflictQuotations.columnsOf conflictColumns)

    /// <summary>When the inserted row breaks the unique constraint or index on exactly the given columns, the existing row
    /// changes as the update says: <c>ON CONFLICT (columns) DO UPDATE SET ...</c>.</summary>
    /// <param name="row">The row to insert.</param>
    /// <param name="conflictColumns">The constraint's or index's columns: a property of the row, or a tuple of them, e.g.
    /// <c>fun r -> r.Code</c> or <c>fun r -> (r.Code, r.Region)</c>.</param>
    /// <param name="update">Sets properties of the existing row, each to the inserted row's value of the same property or
    /// to a value known before the insert (a constant or a captured value), e.g.
    /// <c>fun existing inserted -> existing.Name &lt;- inserted.Name; existing.Note &lt;- "merged"</c>. Properties not
    /// set keep their old values.</param>
    /// <remarks>Sets <c>row.OnConflict</c> to <c>OnConflict.UpdateOn</c>. A value computed from the rows, such as
    /// <c>existing.Count + 1</c>, is refused, and so is an update that sets nothing or one property twice. PostgreSQL,
    /// SQLite 3.24+ and DuckDB; MySQL and the databases without an upsert refuse it when the row is submitted.</remarks>
    [<Extension>]
    static member OnConflictUpdate<'T, 'Key when 'T :> SqlEntity>
        (
            row: 'T,
            [<ReflectedDefinition>] conflictColumns: Expr<'T -> 'Key>,
            [<ReflectedDefinition>] update: Expr<'T -> 'T -> unit>
        ) =
        row.OnConflict <- UpdateOn(ConflictQuotations.columnsOf conflictColumns, ConflictQuotations.updatesOf update)
