namespace FSharp.Data.Sql.Schema

open System
open System.Data
open System.Data.SqlClient
open System.Text.RegularExpressions
open FSharp.Data.Sql.Common.Utilities

module internal Patterns =
   let (|Match|_|) (pat:string) (inp:string) =
      let m = Regex.Match(inp, pat) in
      if m.Success
      then Some (List.tail [ for g in m.Groups -> g.Value ])
      else None

type TypeMapping = {
    ProviderTypeName : string option
    ClrType : string
    ProviderType : int option
    DbType : DbType
}
with 
    static member Create(?clrType, ?dbType, ?providerTypeName, ?providerType) =
        {
            ClrType = defaultArg clrType ((typeof<string>).ToString())
            DbType = defaultArg dbType (DbType.String)
            ProviderTypeName = providerTypeName
            ProviderType = providerType
        }

type QueryParameter = { Name:string; TypeMapping : TypeMapping; Direction:ParameterDirection; Length:int option; Ordinal:int }
with
    static member Create(name, ordinal, ?typeMapping, ?direction, ?length) = 
        {
            Name = name; Ordinal = ordinal;
            TypeMapping = defaultArg typeMapping (TypeMapping.Create()); 
            Direction = defaultArg direction ParameterDirection.Input; 
            Length = length 
        }

type Column = { Name:string; TypeMapping : TypeMapping; IsPrimarKey:bool; IsNullable:bool }
type Relationship = { Name:string; PrimaryTable:string; PrimaryKey:string; ForeignTable:string; ForeignKey:string }

type SprocName = {
    ProcName : string
    Owner : string
    PackageName : string
}
with
    member x.ToList() =  
           if String.IsNullOrEmpty(x.PackageName)
           then [x.ProcName] 
           else [x.PackageName; x.ProcName]
    member x.DbName with get() = String.Join(".", x.ToList())
    member x.FriendlyName with get() = String.Join(" ", x.ToList())
    member x.FullName with get() = String.Join("_", x.ToList())



type SprocDefinition = { Name:SprocName; Params:QueryParameter list }

type Sproc =
    | Root of string * Sproc
    | SprocPath of string * Sproc
    | Sproc of SprocDefinition
    | Empty

type PrimaryKey = { Name : string; Table : string; Column : string; IndexName : string }
type Table = { Schema: string; Name:string; Type:string }
    with 
        // Note here the [].[] format is ONLY used internally.  Do not use this in queries; Different vendors have 
        // different ways to qualify whitespace.
        member x.FullName = 
            let quoteWhiteSpace (str:String) = 
                (if str.Contains(" ") then sprintf "\"%s\"" str else str)
            if (String.IsNullOrWhiteSpace(x.Schema))
            then (quoteWhiteSpace x.Name)
            else x.Schema + "." + (quoteWhiteSpace x.Name)
        //sprintf "%s" x.Name //x.Schema x.Name
        static member FromFullName(fullName:string) = 
            match fullName with
            | Patterns.Match @"(.*)\.(.*)" [schema;name] -> { Schema = schema; Name = name; Type="" } 
            | _ -> { Schema = ""; Name = fullName; Type="" }//failwith ""
        static member CreateFullName(schema, name) = 
            if (String.IsNullOrWhiteSpace(schema))
            then (quoteWhiteSpace name)
            else schema + "." + (quoteWhiteSpace name)