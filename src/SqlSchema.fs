namespace FSharp.Data.Sql.Schema

open System
open System.Data
open System.Data.SqlClient
open System.Text.RegularExpressions

module internal Patterns =
   let (|Match|_|) (pat:string) (inp:string) =
      let m = Regex.Match(inp, pat) in
      if m.Success
      then Some (List.tail [ for g in m.Groups -> g.Value ])
      else None

type Column = { Name:string; ClrType: Type; SqlDbType: SqlDbType; IsPrimarKey:bool; }
type Relationship = { Name:string; PrimaryTable:string; PrimaryKey:string; ForeignTable:string; ForeignKey:string }
type Direction = In | Out
type SprocParam = { Name:string; ClrType:Type; SqlDbType:SqlDbType; Direction:Direction; MaxLength:int option; Ordinal:int }
type Sproc = {FullName:string; Params:SprocParam list; ReturnColumns:Column list }

type Table = { Schema: string; Name:string; Type:string }
    with 
        member x.FullName = sprintf "[%s].[%s]" x.Schema x.Name
        static member FromFullName(fullName:string) = 
            match fullName with
            | Patterns.Match @"\[(.*)\].\[(.*)\]" [schema;name] -> { Schema = schema; Name = name; Type="" } 
            | _ -> failwith ""
