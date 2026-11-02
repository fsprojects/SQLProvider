/// Checks the SQL each test sends. A test must send at least one SQL command, so a query that by accident runs in
/// memory instead of in the database fails; [<NoSql>] marks a test that sends none on purpose. [<ExpectSql>] lists what
/// the SQL must contain: each argument is one expectation, met when any command the test sent contains one of its
/// |-separated alternatives, ignoring case and spacing, e.g. [<ExpectSql("WHERE", "LIMIT 10|TOP 10")>].
module SqlCheck

open System
open System.Collections.Concurrent
open System.Text.RegularExpressions
open FSharp.Data.Sql
open NUnit.Framework
open NUnit.Framework.Interfaces

/// What the test's SQL must contain; see the module comment.
[<AttributeUsage(AttributeTargets.Method, AllowMultiple = false)>]
type ExpectSqlAttribute([<ParamArray>] expectations: string array) =
    inherit Attribute()
    member _.Expectations = expectations

/// The test sends no SQL on purpose (a mock context, a failure before execution, or no query at all).
[<AttributeUsage(AttributeTargets.Method, AllowMultiple = false)>]
type NoSqlAttribute() =
    inherit Attribute()

/// The commands sent by each running test, by test id.
let private sent = ConcurrentDictionary<string, ConcurrentQueue<string>>()

let private testId () =
    try
        TestContext.CurrentContext.Test.ID
    with _ ->
        null

/// A command belongs to NUnit's current test where the flow that sent it knows the test. A continuation on the thread
/// pool (an Async awaited in a task) may not; it belongs to the running test when only one runs.
let private subscribed =
    lazy
        (Common.QueryEvents.SqlQueryEvent
         |> Event.add (fun e ->
             match testId () with
             | id when not (isNull id) && sent.ContainsKey id -> sent[id].Enqueue e.Command
             | _ ->
                 match List.ofSeq sent.Values with
                 | [ only ] -> only.Enqueue e.Command
                 | _ -> ()))

/// Upper case, single spaces, no spaces around parentheses or commas.
let normalise (sql: string) =
    let spaced = Regex.Replace(sql.ToUpperInvariant(), @"\s+", " ")
    Regex.Replace(spaced, @"\s*([(),])\s*", "$1")

/// The expectations no command meets.
let unmet (expectations: string seq) (commands: string list) =
    let normalised = commands |> List.map normalise

    expectations
    |> Seq.filter (fun expectation ->
        let alternatives = expectation.Split('|') |> Array.map normalise
        not (normalised |> List.exists (fun command -> alternatives |> Array.exists command.Contains)))
    |> List.ofSeq

[<AttributeUsage(AttributeTargets.Assembly)>]
type SqlCheckAttribute() =
    inherit Attribute()

    interface ITestAction with
        member _.Targets = ActionTargets.Test

        member _.BeforeTest(test: ITest) =
            subscribed.Force()
            sent[test.Id] <- ConcurrentQueue()

        member _.AfterTest(test: ITest) =
            match sent.TryRemove test.Id with
            | true, commands when TestContext.CurrentContext.Result.Outcome.Status = TestStatus.Passed ->
                let commands = List.ofSeq commands
                let noSql, expected =
                    match test.Method with
                    | null -> false, [||]
                    | m ->
                        not (Array.isEmpty (m.GetCustomAttributes<NoSqlAttribute>(false))),
                        m.GetCustomAttributes<ExpectSqlAttribute>(false)

                let shown () =
                    if commands.IsEmpty then "(none)" else String.Join("\n", commands)

                if not noSql then
                    if commands.IsEmpty then
                        Assert.Fail "The test sent no SQL: its query ran in memory. Mark it [<NoSql>] if that is meant."

                    for expect in expected do
                        match unmet expect.Expectations commands with
                        | [] -> ()
                        | missing ->
                            let wanted = String.Join(", ", missing)
                            Assert.Fail($"No SQL sent contains {wanted}. Sent:\n{shown ()}")
            | _ -> ()

[<assembly: SqlCheck>]
do ()
