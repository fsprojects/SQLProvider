// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.

open System
open FSharp.Data.Sql
open FSharpComposableQuery

FSharp.Data.Sql.Common.QueryEvents.SqlQueryEvent |> Event.add (printfn "Executing SQL: %s")

type HR = SqlDataProvider<ConnectionStringName = "mopdc", DatabaseVendor = Common.DatabaseProviderTypes.ORACLE>
let ctx = HR.GetDataContext()


type AuditRecord = {
    Id : decimal
    Message : string
    User : string
}

type Predicate = 
  | After of DateTime
  | Before of DateTime
  | And of Predicate * Predicate
  | Or of Predicate * Predicate
  | Not of Predicate

let audits  = 
    <@ fun p -> query { 
       for u in ctx.``[MOPDC].[AUDIT_TRAIL]`` do
       if p u.RECORDED_ON 
       then yield u
      } @>
    
let rec eval(t) =
    match t with
    | After n -> <@ fun x -> x >= n @>
    | Before n -> <@ fun x -> x < n @>
    | And (t1,t2) -> <@ fun x -> (%eval t1) x && (%eval t2) x @>
    | Or (t1,t2) -> <@ fun x -> (%eval t1) x || (%eval t2) x @>
    | Not (t0) -> <@ fun x -> not((%eval t0) x) @>

[<EntryPoint>]
let main argv =
    
    let search = And(After(DateTime(2015, 01, 01)), Before(DateTime.Today))

    let result = 
        query { for aud in ((%audits) (%eval search)) do
                yield { Id = aud.AUDIT_TRAIL_ID; Message = aud.AUDIT_MESSAGE; User = aud.RECORDED_BY }
        } |> Seq.toList

    result |> List.iter (printfn "%A")

    printfn "Finished"
    Console.ReadLine() |> ignore
    0 // return an integer exit code
