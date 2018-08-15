namespace FSharp.Data.Sql.Schema

open System
open System.Data
open System.Data.SqlClient
open System.Text.RegularExpressions
open FSharp.Data.Sql.Common.Utilities
open System.Reflection

module internal Patterns =
    let (|Match|_|) (pat:string) (inp:string) =
        let m = Regex.Match(inp, pat) in
        if m.Success then Some (List.tail [ for g in m.Groups -> g.Value ]) else None

type TypeMapping =
    { ProviderTypeName: string option
      ClrType: string
      ProviderType: int option
      DbType: DbType }
    with
        static member Create(?clrType, ?dbType, ?providerTypeName, ?providerType) =
            { ClrType = defaultArg clrType ((typeof<string>).ToString())
              DbType = defaultArg dbType (DbType.String)
              ProviderTypeName = providerTypeName
              ProviderType = providerType }

type QueryParameter =
    { Name: string
      TypeMapping: TypeMapping
      Direction: ParameterDirection
      Length: int option
      Ordinal: int }
    with
        static member Create(name, ordinal, ?typeMapping, ?direction, ?length) =
            { Name = name
              Ordinal = ordinal
              TypeMapping = defaultArg typeMapping (TypeMapping.Create())
              Direction = defaultArg direction ParameterDirection.Input
              Length = length }

type Column =
    { Name: string
      TypeMapping: TypeMapping
      IsPrimaryKey: bool
      IsNullable: bool
      IsAutonumber: bool
      HasDefault: bool
      TypeInfo: string option }
    with
        static member FromQueryParameter(q: QueryParameter) =
            { Name = q.Name
              TypeMapping = q.TypeMapping
              IsPrimaryKey = false
              IsNullable = true
              IsAutonumber = false
              HasDefault = false
              TypeInfo = None }

type ColumnLookup = Map<string,Column>

type Relationship =
    { Name: string
      PrimaryTable: string
      PrimaryKey: string
      ForeignTable: string
      ForeignKey: string }

type SprocName =
    { ProcName: string
      Owner: string
      PackageName: string }
    with
        member x.ToList() =
           if String.IsNullOrEmpty(x.PackageName) then [x.ProcName]
           else [x.PackageName; x.ProcName]
        member x.DbName with get() = String.Join(".", x.ToList())
        member x.FriendlyName with get() = String.Join(" ", x.ToList())
        member x.FullName with get() = String.Join("_", x.ToList())
        override x.ToString() = x.FullName.ToString()

type CompileTimeSprocDefinition =
    { Name: SprocName
      [<NonSerialized>] 
      Params: (IDbConnection -> QueryParameter list)
      [<NonSerialized>] 
      ReturnColumns: (IDbConnection -> QueryParameter list -> QueryParameter list) }
    override x.ToString() = x.Name.ToString()

type RunTimeSprocDefinition =
    { Name: SprocName
      Params: QueryParameter list }

[<System.Runtime.Serialization.KnownType("GetKnownTypes")>]
type Sproc =
    | Root of string * Sproc
    | Package of string * CompileTimePackageDefinition
    | Sproc of CompileTimeSprocDefinition
    | Empty
    static member GetKnownTypes() =
        typedefof<Sproc>.GetNestedTypes(BindingFlags.Public ||| BindingFlags.NonPublic) 
        |> Array.filter Microsoft.FSharp.Reflection.FSharpType.IsUnion

and CompileTimePackageDefinition =
    { Name : string
      [<NonSerialized>] // Todo: Serialize for ContextSchemaPath...
      Sprocs : (IDbConnection -> CompileTimeSprocDefinition list)
    }

type Table =
    { Schema: string
      Name: string
      Type: string }
    with
        // Note here the [].[] format is ONLY used internally.  Do not use this in queries; Different vendors have
        // different ways to qualify whitespace.
        member x.FullName =
            if (String.IsNullOrWhiteSpace(x.Schema)) then (quoteWhiteSpace x.Name)
            else x.Schema + "." + (quoteWhiteSpace x.Name)
        static member FromFullName(fullName:string) =
            match fullName with
            | Patterns.Match @"(.*)\.(.*)" [schema;name] -> { Schema = schema; Name = name; Type="" }
            | _ -> { Schema = ""; Name = fullName; Type="" }
        static member CreateFullName(schema, name) =
            if (String.IsNullOrWhiteSpace(schema)) then (quoteWhiteSpace name)
            else schema + "." + (quoteWhiteSpace name)
