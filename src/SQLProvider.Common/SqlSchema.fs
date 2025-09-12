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

/// Represents the mapping between database types and .NET CLR types.
/// Used to translate between database-specific types and .NET types during query execution.
[<Struct>]
type TypeMapping =
    /// The database provider's specific type name (e.g., "varchar", "int")
    { ProviderTypeName: string voption
      /// The corresponding .NET CLR type name
      ClrType: string
      /// The database provider's numeric type identifier
      ProviderType: int voption
      /// The standard .NET DbType enumeration value
      DbType: DbType }
    with
        static member Create(?clrType, ?dbType, ?providerTypeName, ?providerType) =
            { ClrType = defaultArg clrType ((typeof<string>).ToString())
              DbType = defaultArg dbType (DbType.String)
              ProviderTypeName = match providerTypeName with Some x -> ValueSome x | None -> ValueNone
              ProviderType = match providerType with Some x -> ValueSome x | None -> ValueNone }

/// Represents a parameter used in SQL queries, stored procedures, or functions.
/// Contains all metadata needed to properly bind .NET values to database parameters.
[<Struct>]
type QueryParameter =
    /// The parameter name as used in the SQL query
    { Name: string
      /// Type mapping information for converting between .NET and database types
      TypeMapping: TypeMapping
      /// Whether the parameter is input, output, or bidirectional
      Direction: ParameterDirection
      /// Maximum length for string/binary parameters
      Length: int voption
      /// Position of the parameter in the parameter list (zero-based)
      Ordinal: int }
    with
        static member Create(name, ordinal, ?typeMapping, ?direction, ?length) =
            { Name = name
              Ordinal = ordinal
              TypeMapping = defaultArg typeMapping (TypeMapping.Create())
              Direction = defaultArg direction ParameterDirection.Input
              Length = match length with Some x -> ValueSome x | None -> ValueNone }

/// Represents a database table column with its metadata and characteristics.
/// Contains all information needed to generate typed properties and handle data conversion.
[<Struct>]
type Column =
    /// The column name as it appears in the database
    { Name: string
      /// Type mapping information for converting between .NET and database types
      TypeMapping: TypeMapping
      /// True if this column is part of the table's primary key
      IsPrimaryKey: bool
      /// True if this column can contain NULL values
      IsNullable: bool
      /// True if this column has an auto-increment/identity specification
      IsAutonumber: bool
      /// True if this column has a default value defined
      HasDefault: bool
      /// True if this column is computed/calculated by the database
      IsComputed: bool
      /// Additional type information specific to the database provider
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

/// Lookup table for quickly finding column information by name.
type ColumnLookup = Map<string,Column>

/// Represents a foreign key relationship between two database tables.
/// Used for automatic navigation property generation and join optimization.
[<Struct>]
type Relationship =
    /// A descriptive name for this relationship
    { Name: string
      /// The name of the table containing the primary key
      PrimaryTable: string
      /// The column name of the primary key
      PrimaryKey: string
      /// The name of the table containing the foreign key
      ForeignTable: string
      /// The column name of the foreign key
      ForeignKey: string }

/// Represents the name of a stored procedure, including schema and package information.
/// Handles different database naming conventions (e.g., Oracle packages, SQL Server schemas).
type SprocName =
    /// The procedure name
    { ProcName: string
      /// The schema or owner name
      Owner: string
      /// The package name (primarily for Oracle)
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

/// Represents a database table with its schema information.
/// Provides methods for generating properly quoted table names for different database providers.
type Table =
    /// The schema name (may be empty for databases that don't use schemas)
    { Schema: string
      /// The table name
      Name: string
      /// The table type (e.g., "TABLE", "VIEW", "SYSTEM TABLE")
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
