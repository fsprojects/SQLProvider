namespace FSharp.Data.Sql.Common

open System
open System.Collections.Generic
open System.ComponentModel
open System.Data
open System.Data.Common
open System.Linq.Expressions
open System.Reflection
open System.Runtime.Serialization
open FSharp.Data.Sql
open FSharp.Data.Sql.Schema
open Microsoft.FSharp.Reflection
open System.Collections.Concurrent

type DatabaseProviderTypes =
    | MSSQLSERVER = 0
    | SQLITE = 1
    | POSTGRESQL = 2
    | MYSQL = 3
    | ORACLE = 4
    | MSACCESS = 5
    | ODBC = 6
type RelationshipDirection = Children = 0 | Parents = 1

type CaseSensitivityChange =
    | ORIGINAL = 0
    | TOUPPER = 1
    | TOLOWER = 2

module public QueryEvents =
   let private expressionEvent = new Event<System.Linq.Expressions.Expression>()
   let private sqlEvent = new Event<string>()

   [<CLIEvent>]
   let LinqExpressionEvent = expressionEvent.Publish

   [<CLIEvent>]
   let SqlQueryEvent = sqlEvent.Publish

   let PublishExpression(e) = expressionEvent.Trigger(e)
   let PublishSqlQuery(s) = sqlEvent.Trigger(s)

type EntityState =
    | Unchanged
    | Created
    | Modified of string list
    | Delete
    | Deleted

[<DataContract(Name = "SqlEntity", Namespace = "http://schemas.microsoft.com/sql/2011/Contracts"); DefaultMember("Item")>]
type SqlEntity(dc: ISqlDataContext, tableName, columns: ColumnLookup) =
    let table = Table.FromFullName tableName
    let propertyChanged = Event<_,_>()

    let data = Dictionary<string,obj>()
    let aliasCache = new ConcurrentDictionary<string,SqlEntity>(HashIdentity.Structural)

    let replaceFirst (text:string) (oldValue:string) (newValue) =
        let position = text.IndexOf oldValue
        if position < 0 then
            text
        else
            text.Substring(0, position) + newValue + text.Substring(position + oldValue.Length)

    member val _State = Unchanged with get, set

    member e.Delete() =
        e._State <- Delete
        dc.SubmitChangedEntity e

    member internal e.TriggerPropertyChange(name) = propertyChanged.Trigger(e, PropertyChangedEventArgs(name))
    member internal __.ColumnValuesWithDefinition = seq { for kvp in data -> kvp.Key, kvp.Value, columns.TryFind(kvp.Key) }

    member __.ColumnValues = seq { for kvp in data -> kvp.Key, kvp.Value }
    member __.Table= table
    member __.DataContext with get() = dc

    member __.GetColumn<'T>(key) : 'T =
        let defaultValue() =
            if typeof<'T> = typeof<string> then (box String.Empty) :?> 'T
            else Unchecked.defaultof<'T>
        if data.ContainsKey key then
           match data.[key] with
           | null -> defaultValue()
           | :? System.DBNull -> defaultValue()
            //This deals with an oracle specific case where the type mappings says it returns a System.Decimal but actually returns a float!?!?!  WTF...
           | data when data.GetType() <> typeof<'T> && typeof<'T> <> typeof<obj> -> unbox <| Convert.ChangeType(data, typeof<'T>)
           | data -> unbox data
        else defaultValue()

    member __.GetColumnOption<'T>(key) : Option<'T> =
       if data.ContainsKey key then
           match data.[key] with
           | null -> None
           | :? System.DBNull -> None
           | data when data.GetType() <> typeof<'T> && typeof<'T> <> typeof<obj> -> Some(unbox<'T> <| Convert.ChangeType(data, typeof<'T>))
           | data -> Some(unbox data)
       else None

    member internal this.GetColumnOptionWithDefinition(key) =
        this.GetColumnOption(key) |> Option.bind (fun v -> Some(box v, columns.TryFind(key)))

    member private e.UpdateField key =
        match e._State with
        | Modified fields ->
            e._State <- Modified (key::fields)
            e.DataContext.SubmitChangedEntity e
        | Unchanged ->
            e._State <- Modified [key]
            e.DataContext.SubmitChangedEntity e
        | Deleted | Delete -> failwith ("You cannot modify an entity that is pending deletion: " + key)
        | Created -> ()

    member __.SetColumnSilent(key,value) =
        data.[key] <- value

    member e.SetColumn<'t>(key,value : 't) =
        data.[key] <- value
        e.UpdateField key
        e.TriggerPropertyChange key

    member e.SetData(data) = data |> Seq.iter e.SetColumnSilent

    member __.SetColumnOptionSilent(key,value) =
      match value with
      | Some value ->
          if not (data.ContainsKey key) then data.Add(key,value)
          else data.[key] <- value
      | None -> data.Remove key |> ignore

    member e.SetColumnOption(key,value) =
      match value with
      | Some value ->
          if not (data.ContainsKey key) then data.Add(key,value)
          else data.[key] <- value
          e.TriggerPropertyChange key
      | None -> if data.Remove key then e.TriggerPropertyChange key
      e.UpdateField key

    member __.HasValue(key) = data.ContainsKey key

    /// creates a new SQL entity from alias data in this entity
    member internal e.GetSubTable(alias:string,tableName) =
        aliasCache.GetOrAdd(alias, fun alias ->
            let tableName = if tableName <> "" then tableName else e.Table.FullName
            let newEntity = SqlEntity(dc, tableName, columns)
            // attributes names cannot have a period in them unless they are an alias
            let pred =
                let prefix = "[" + alias + "]."
                let prefix2 = alias + "."
                let prefix3 = "`" + alias + "`."
                let prefix4 = alias + "_"
                (fun (k:string,v) ->
                    if k.StartsWith prefix then
                        let temp = replaceFirst k prefix ""
                        let temp = temp.Substring(1,temp.Length-2)
                        Some(temp,v)
                    // this case is for PostgreSQL and other vendors that use " as whitespace qualifiers
                    elif  k.StartsWith prefix2 then
                        let temp = replaceFirst k prefix2 ""
                        Some(temp,v)
                    // this case is for MySQL and other vendors that use ` as whitespace qualifiers
                    elif  k.StartsWith prefix3 then
                        let temp = replaceFirst k prefix3 ""
                        let temp = temp.Substring(1,temp.Length-2)
                        Some(temp,v)
                    //this case for MSAccess, uses _ as whitespace qualifier
                    elif  k.StartsWith prefix4 then
                        let temp = replaceFirst k prefix4 ""
                        Some(temp,v)
                    else None)

            e.ColumnValues
            |> Seq.choose pred
            |> Seq.iter( fun (k,v) -> newEntity.SetColumnSilent(k,v))

            newEntity)

    member x.MapTo<'a>(?propertyTypeMapping : (string * obj) -> obj) =
        let typ = typeof<'a>
        let propertyTypeMapping = defaultArg propertyTypeMapping snd
        let cleanName (n:string) = n.Replace("_","").Replace(" ","").ToLower()
        let dataMap = x.ColumnValues |> Seq.map (fun (n,v) -> cleanName n, v) |> dict
        if FSharpType.IsRecord typ
        then
            let ctor = FSharpValue.PreComputeRecordConstructor(typ)
            let fields = FSharpType.GetRecordFields(typ)
            let values =
                [|
                    for prop in fields do
                        match dataMap.TryGetValue(cleanName prop.Name) with
                        | true, data -> yield propertyTypeMapping (prop.Name,data)
                        | false, _ -> ()
                |]
            unbox<'a> (ctor(values))
        else
            let instance = Activator.CreateInstance<'a>()
            for prop in typ.GetProperties() do
                match dataMap.TryGetValue(cleanName prop.Name) with
                | true, data -> prop.GetSetMethod().Invoke(instance, [|propertyTypeMapping (prop.Name,data)|]) |> ignore
                | false, _ -> ()
            instance

    interface System.ComponentModel.INotifyPropertyChanged with
        [<CLIEvent>] member __.PropertyChanged = propertyChanged.Publish

    interface System.ComponentModel.ICustomTypeDescriptor with
        member e.GetComponentName() = TypeDescriptor.GetComponentName(e,true)
        member e.GetDefaultEvent() = TypeDescriptor.GetDefaultEvent(e,true)
        member e.GetClassName() = e.Table.FullName
        member e.GetEvents(_) = TypeDescriptor.GetEvents(e,true)
        member e.GetEvents() = TypeDescriptor.GetEvents(e,null,true)
        member e.GetConverter() = TypeDescriptor.GetConverter(e,true)
        member __.GetPropertyOwner(_) = upcast data
        member e.GetAttributes() = TypeDescriptor.GetAttributes(e,true)
        member e.GetEditor(typeBase) = TypeDescriptor.GetEditor(e,typeBase,true)
        member __.GetDefaultProperty() = null
        member e.GetProperties()  = (e :> ICustomTypeDescriptor).GetProperties(null)
        member __.GetProperties(_) =
            PropertyDescriptorCollection(
               data
               |> Seq.map(
                  function KeyValue(k,v) ->
                              { new PropertyDescriptor(k,[||])  with
                                 override __.PropertyType with get() = v.GetType()
                                 override __.SetValue(e,value) = (e :?> SqlEntity).SetColumn(k,value)
                                 override __.GetValue(e) = (e:?>SqlEntity).GetColumn k
                                 override __.IsReadOnly with get() = false
                                 override __.ComponentType with get () = null
                                 override __.CanResetValue(_) = false
                                 override __.ResetValue(_) = ()
                                 override __.ShouldSerializeValue(_) = false })
               |> Seq.cast<PropertyDescriptor> |> Seq.toArray )

and ResultSet = seq<(string * obj)[]>
and ReturnSetType =
    | ScalarResultSet of string * obj
    | ResultSet of string * ResultSet
and ReturnValueType =
    | Unit
    | Scalar of string * obj
    | SingleResultSet of string * ResultSet
    | Set of seq<ReturnSetType>

and ISqlDataContext =
    abstract ConnectionString           : string
    abstract CreateRelated              : SqlEntity * string * string * string * string * string * RelationshipDirection -> System.Linq.IQueryable<SqlEntity>
    abstract CreateEntities             : string -> System.Linq.IQueryable<SqlEntity>
    abstract CallSproc                  : RunTimeSprocDefinition * QueryParameter[] * obj[] -> obj
    abstract GetIndividual              : string * obj -> SqlEntity
    abstract SubmitChangedEntity        : SqlEntity -> unit
    abstract SubmitPendingChanges       : unit -> unit
    abstract SubmitPendingChangesAsync  : unit -> Async<unit>
    abstract SubmitPendingChanges       : bool * bool -> string
    abstract SubmitPendingChangesAsync  : bool * bool -> Async<string>
    abstract ClearPendingChanges        : unit -> unit
    abstract GetPendingEntities         : unit -> SqlEntity list
    abstract GetPrimaryKeyDefinition    : string -> string
    abstract CreateConnection           : unit -> IDbConnection
    abstract CreateEntity               : string -> SqlEntity
    abstract ReadEntities               : string * ColumnLookup * IDataReader -> SqlEntity[]
    abstract ReadEntitiesAsync          : string * ColumnLookup * DbDataReader -> Async<SqlEntity[]>

and LinkData =
    { PrimaryTable       : Table
      PrimaryKey         : string
      ForeignTable       : Table
      ForeignKey         : string
      OuterJoin          : bool
      RelDirection       : RelationshipDirection      }
    with
        member x.Rev() =
            { x with PrimaryTable = x.ForeignTable; PrimaryKey = x.ForeignKey; ForeignTable = x.PrimaryTable; ForeignKey = x.PrimaryKey }

and alias = string
and table = string

and Condition =
    // this is  (table alias * column name * operator * right hand value ) list  * (the same again list)
    // basically any AND or OR expression can have N terms and can have N nested condition children
    // this is largely from my CRM type provider. I don't think in practice for the SQL provider
    // you will ever have more than what is representable in a traditional binary expression tree, but
    // changing it would be a lot of effort ;)
    | And of (alias * string * ConditionOperator * obj option) list * (Condition list) option
    | Or of (alias * string * ConditionOperator * obj option) list * (Condition list) option

and internal SqlExp =
    | BaseTable    of alias * Table                      // name of the initiating IQueryable table - this isn't always the ultimate table that is selected
    | SelectMany   of alias * alias * LinkData * SqlExp  // from alias, to alias and join data including to and from table names. Note both the select many and join syntax end up here
    | FilterClause of Condition * SqlExp                 // filters from the where clause(es)
    | Projection   of Expression * SqlExp                // entire LINQ projection expression tree
    | Distinct     of SqlExp                             // distinct indicator
    | OrderBy      of alias * string * bool * SqlExp     // alias and column name, bool indicates ascending sort
    | Skip         of int * SqlExp
    | Take         of int * SqlExp
    | Count        of SqlExp
    | AggregateOp  of Utilities.AggregateOperation * alias * string * SqlExp
    with member this.HasAutoTupled() =
            let rec aux = function
                | BaseTable(_) -> false
                | SelectMany(_) -> true
                | FilterClause(_,rest)
                | Projection(_,rest)
                | Distinct rest
                | OrderBy(_,_,_,rest)
                | Skip(_,rest)
                | Take(_,rest)
                | Count(rest) 
                | AggregateOp(_,_,_,rest) -> aux rest
            aux this

and internal SqlQuery =
    { Filters       : Condition list
      Links         : (alias * LinkData * alias) list
      Aliases       : Map<string, Table>
      Ordering      : (alias * string * bool) list
      Projection    : Expression option
      Distinct      : bool
      UltimateChild : (string * Table) option
      Skip          : int option
      Take          : int option
      Count         : bool 
      AggregateOp   : (Utilities.AggregateOperation * alias * string) list }
    with
        static member Empty = { Filters = []; Links = []; Aliases = Map.empty; Ordering = []; Count = false; AggregateOp = []
                                Projection = None; Distinct = false; UltimateChild = None; Skip = None; Take = None }

        static member ofSqlExp(exp,entityIndex: string ResizeArray) =
            let legaliseName (alias:alias) =
                if alias.StartsWith("_") then alias.TrimStart([|'_'|]) else alias

            let rec convert (q:SqlQuery) = function
                | BaseTable(a,e) -> match q.UltimateChild with
                                        | Some(_,_) -> q
                                        | None when q.Links.Length > 0 && q.Links |> List.exists(fun (a',_,_) -> a' = a) = false ->
                                                // the check here relates to the special case as described in the FilterClause below.
                                                // need to make sure the pre-tuple alias (if applicable) is not used in the projection,
                                                // but rather the later alias of the same object after it has been tupled.
                                                  { q with UltimateChild = Some(legaliseName entityIndex.[0], e) }
                                        | None -> { q with UltimateChild = Some(legaliseName a,e) }
                | SelectMany(a,b,link,rest) ->
                   match link.RelDirection with
                   | RelationshipDirection.Children ->
                         convert { q with Aliases = q.Aliases.Add(legaliseName b,link.ForeignTable).Add(legaliseName a,link.PrimaryTable);
                                          Links = (legaliseName a, link, legaliseName b)  :: q.Links } rest
                   | _ ->
                         convert { q with Aliases = q.Aliases.Add(legaliseName a,link.ForeignTable).Add(legaliseName b,link.PrimaryTable);
                                         Links = (legaliseName a, link, legaliseName b) :: q.Links  } rest
                | FilterClause(c,rest) ->  convert { q with Filters = (c)::q.Filters } rest
                | Projection(exp,rest) ->
                    if q.Projection.IsSome then failwith "the type provider only supports a single projection"
                    else convert { q with Projection = Some exp } rest
                | Distinct(rest) ->
                    if q.Distinct then failwith "distinct is applied to the entire query and can only be supplied once"
                    else convert { q with Distinct = true } rest
                | OrderBy(alias,key,desc,rest) ->
                    convert { q with Ordering = (legaliseName alias,key,desc)::q.Ordering } rest
                | Skip(amount, rest) ->
                    if q.Skip.IsSome then failwith "skip may only be specified once"
                    else convert { q with Skip = Some(amount) } rest
                | Take(amount, rest) ->
                    match q.Take with
                    | Some x when x = 1 && amount = 1 -> convert { q with Take = Some(amount) } rest
                    | Some x -> failwith "take may only be specified once"
                    | None -> convert { q with Take = Some(amount) } rest
                | Count(rest) ->
                    if q.Count then failwith "count may only be specified once"
                    else convert { q with Count = true } rest
                | AggregateOp(operation, alias, key, rest) ->
                    convert { q with AggregateOp = (operation, alias, key)::q.AggregateOp } rest
            let sq = convert (SqlQuery.Empty) exp
            sq

and internal ISqlProvider =
    /// return a new, unopened connection using the provided connection string
    abstract CreateConnection : string -> IDbConnection
    /// return a new command associated with the provided connection and command text
    abstract CreateCommand : IDbConnection * string -> IDbCommand
    /// return a new command parameter with the provided name, value and optionally type, direction and length
    abstract CreateCommandParameter : QueryParameter * obj -> IDbDataParameter
    /// This function will be called when the provider is first created and should be used
    /// to generate a cache of type mappings, and to set the three mapping function properties
    abstract CreateTypeMappings : IDbConnection -> Unit
    /// Queries the information schema and returns a list of table information
    abstract GetTables  : IDbConnection * CaseSensitivityChange -> Table list
    /// Queries the given table and returns a list of its columns
    /// this function should also populate a primary key cache for tables that
    /// have a single non-composite primary key
    abstract GetColumns : IDbConnection * Table -> ColumnLookup
    /// Returns the primary key column for a given table from the pre-populated cache
    /// as generated by calls to GetColumns
    abstract GetPrimaryKey : Table -> string option
    /// Returns constraint information for a given table, returning two lists of relationships, where
    /// the first are relationships where the table is the primary entity, and the second where
    /// it is the foreign entity
    abstract GetRelationships : IDbConnection * Table -> (Relationship list * Relationship list)
    /// Returns a list of stored procedure metadata
    abstract GetSprocs  : IDbConnection -> Sproc list
    /// Returns the db vendor specific SQL  query to select the top x amount of rows from
    /// the given table
    abstract GetIndividualsQueryText : Table * int -> string
    /// Returns the db vendor specific SQL query to select a single row based on the table and column name specified
    abstract GetIndividualQueryText : Table * string -> string
    /// Writes all pending database changes to database
    abstract ProcessUpdates : IDbConnection * System.Collections.Concurrent.ConcurrentDictionary<SqlEntity,DateTime> -> unit
    /// Asynchronously writes all pending database changes to database
    abstract ProcessUpdatesAsync : System.Data.Common.DbConnection * System.Collections.Concurrent.ConcurrentDictionary<SqlEntity,DateTime> -> Async<unit>
    /// Accepts a SqlQuery object and produces the SQL to execute on the server.
    /// the other parameters are the base table alias, the base table, and a dictionary containing
    /// the columns from the various table aliases that are in the SELECT projection
    abstract GenerateQueryText : SqlQuery * string * Table * Dictionary<string,ResizeArray<string>> -> string * ResizeArray<IDbDataParameter>
    ///Builds a command representing a call to a stored procedure
    abstract ExecuteSprocCommand : IDbCommand * QueryParameter[] * QueryParameter[] *  obj[] -> ReturnValueType
