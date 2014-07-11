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

type TypeMapping = {
    ProviderTypeName : string
    ClrType : string
    ProviderType : int
    DbType : DbType
    UseReaderResults : bool
}  

type Column = { Name:string; TypeMapping : TypeMapping; IsPrimarKey:bool; IsNullable:bool }
type Relationship = { Name:string; PrimaryTable:string; PrimaryKey:string; ForeignTable:string; ForeignKey:string }

type SprocReturnColumns = { Name:string; TypeMapping : TypeMapping; IsNullable:bool; Direction:ParameterDirection; }
type SprocParam = { Name:string; TypeMapping : TypeMapping; Direction:ParameterDirection; MaxLength:int option; Ordinal:int }
type SprocDefinition = { Name:string; FullName:string; DbName:string; Params:SprocParam list; ReturnColumns: SprocReturnColumns list }

type Sproc =
    | Root of pathElement:string * Sproc
    | SprocPath of pathElement:string * Sproc
    | Sproc of SprocDefinition

type PrimaryKey = { Name : string; Table : string; Column : string; IndexName : string }
type Table = { Schema: string; Name:string; Type:string }
    with 
        // Note here the [].[] format is ONLY used internally.  Do not use this in queries; Different vendors have 
        // different ways to qualify whitespace.
        member x.FullName = sprintf "[%s].[%s]" x.Schema x.Name
        static member FromFullName(fullName:string) = 
            match fullName with
            | Patterns.Match @"\[(.*)\].\[(.*)\]" [schema;name] -> { Schema = schema; Name = name; Type="" } 
            | _ -> { Schema = ""; Name = fullName; Type="" }//failwith ""
        static member CreateFullName(schema, name) = 
            sprintf "[%s].[%s]" schema name
