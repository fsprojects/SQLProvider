namespace FSharp.Data.Sql.Schema

open System
open System.Data
open System.Text.RegularExpressions
open FSharp.Data.Sql.Common.Utilities
open System.Reflection

module internal Patterns =
    let tablePattern = System.Text.RegularExpressions.Regex(@"^(.+)\.(.+)$", RegexOptions.Compiled)
    [<return: Struct>]
    let (|MatchTable|_|) (inp:string) =
        let m = tablePattern.Match inp in
        if m.Success then
            match (List.tail [ for g in m.Groups -> g.Value ]) with
            | [schema;name] -> ValueSome struct(schema,name)
            | _ -> ValueNone
        else ValueNone

[<Struct>]
type TypeMapping =
    { ProviderTypeName: string voption
      ClrType: string
      ProviderType: int voption
      DbType: DbType }
    with
        static member Create(?clrType, ?dbType, ?providerTypeName, ?providerType) =
            { ClrType = defaultArg clrType ((typeof<string>).ToString())
              DbType = defaultArg dbType (DbType.String)
              ProviderTypeName = match providerTypeName with Some x -> ValueSome x | None -> ValueNone
              ProviderType = match providerType with Some x -> ValueSome x | None -> ValueNone }

[<Struct>]
type QueryParameter =
    { Name: string
      TypeMapping: TypeMapping
      Direction: ParameterDirection
      Length: int voption
      Ordinal: int }
    with
        static member Create(name, ordinal, ?typeMapping, ?direction, ?length) =
            { Name = name
              Ordinal = ordinal
              TypeMapping = defaultArg typeMapping (TypeMapping.Create())
              Direction = defaultArg direction ParameterDirection.Input
              Length = match length with Some x -> ValueSome x | None -> ValueNone }

[<Struct>]
type Column =
    { Name: string
      TypeMapping: TypeMapping
      IsPrimaryKey: bool
      IsNullable: bool
      IsAutonumber: bool
      HasDefault: bool
      IsComputed: bool
      TypeInfo: string voption }
    with
        static member FromQueryParameter(q: QueryParameter) =
            { Name = q.Name
              TypeMapping = q.TypeMapping
              IsPrimaryKey = false
              IsNullable = true
              IsAutonumber = false
              HasDefault = false
              IsComputed = false
              TypeInfo = ValueNone }

type ColumnLookup = Map<string,Column>

[<Struct>]
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
        member x.DbName with get() = String.concat "." (x.ToList())
        member x.FriendlyName with get() = String.concat " " (x.ToList())
        member x.FullName with get() = String.concat "_" (x.ToList())
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

type CompileTimePackageDefinition =
    { Name : string
      [<NonSerialized>] // Todo: Serialize for ContextSchemaPath...
      Sprocs : (IDbConnection -> CompileTimeSprocDefinition list)
    }

[<System.Runtime.Serialization.KnownType("GetKnownTypes")>]
type Sproc =
    | Root of string * Sproc
    | Package of string * CompileTimePackageDefinition
    | Sproc of CompileTimeSprocDefinition
    | Empty
    static member GetKnownTypes() =
        typedefof<Sproc>.GetNestedTypes(BindingFlags.Public ||| BindingFlags.NonPublic)
        |> Array.filter Microsoft.FSharp.Reflection.FSharpType.IsUnion

type Table =
    { Schema: string
      Name: string
      Type: string }
    with
        static member CreateQuotedFullName(schema, name, startQuote, endQuote) =
            let quote s = startQuote + s + endQuote
            let tableName = name |> quoteWhiteSpace |> quote
            if (String.IsNullOrWhiteSpace(schema)) then tableName
            else (quote schema) + "." + tableName
        
        member x.QuotedFullName (startQuote, endQuote) =
            Table.CreateQuotedFullName(x.Schema, x.Name, startQuote, endQuote)
            
        // Note here the [].[] format is ONLY used internally.  Do not use this in queries; Different vendors have
        // different ways to qualify whitespace.
        member x.FullName = x.QuotedFullName ("", "")
        static member FromFullName(fullName:string) =
            match fullName with
            | Patterns.MatchTable (schema, name) -> { Schema = schema; Name = name; Type="" }
            | _ -> { Schema = ""; Name = fullName; Type="" }
        static member CreateFullName(schema, name) =
            Table.CreateQuotedFullName(schema, name, "", "")
