namespace FSharp.Data.Sql.Common

open System
open System.Collections.Generic
open System.ComponentModel
open System.Data
open System.Data.Common
open System.IO
open System.Linq.Expressions
open System.Reflection
open System.Text
open FSharp.Data.Sql
open FSharp.Data.Sql.Transactions
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
    | FIREBIRD = 7
    | MSSQLSERVER_DYNAMIC = 8
    | MSSQLSERVER_SSDT = 9
    | DUCKDB = 10
    | EXTERNAL = 11

type RelationshipDirection = Children = 0 | Parents = 1

type CaseSensitivityChange =
    | ORIGINAL = 0
    | TOUPPER = 1
    | TOLOWER = 2

type NullableColumnType =
    /// Nullable types are just Unchecked default. (Old false.)
    | NO_OPTION = 0
    /// Option types are Option<_>. (Old true.)
    | OPTION = 1
    /// ValueOption is more performant.
    | VALUE_OPTION = 2

type OdbcQuoteCharacter =
    | DEFAULT_QUOTE = 0
    /// MySQL/Postgre style: `alias` 
    | GRAVE_ACCENT = 1
    /// Microsoft SQL style: [alias]
    | SQUARE_BRACKETS = 2
    /// Plain, no special names: alias
    | NO_QUOTES = 3 // alias
    /// Amazon Redshift style: "alias" & Firebird style too
    | DOUBLE_QUOTES = 4
    /// Single quote: 'alias'
    | APHOSTROPHE = 5 

type SQLiteLibrary =
    /// .NET Framework default
    | SystemDataSQLite = 0
    /// Mono version
    | MonoDataSQLite = 1
    /// Auto-select by environment
    | AutoSelect = 2
    /// Microsoft.Data.Sqlite. May support .NET Standard 2.0 contract in the future.
    | MicrosoftDataSqlite = 3

module public QueryEvents =
      
   type SqlEventData = {
       /// The text of the SQL command being executed.
       Command: string

       /// The parameters (if any) passed to the SQL command being executed.
       Parameters: (string*obj) seq

       /// The SHA256 hash of the UTF8-encoded connection string used to perform this command.
       /// Use this to determine on which database connection the command is going to be executed.
       ConnectionStringHash: byte[]      
   }
   with 
      override x.ToString() =
        let arr = x.Parameters |> Seq.toArray
        if arr.Length = 0 then x.Command
        else
            let paramsString = arr |> Seq.fold (fun (sb:StringBuilder) (pName, pValue) -> sb.Append(sprintf "%s - %A; " pName pValue)) (StringBuilder())
            sprintf "%s -- params %s" x.Command (paramsString.ToString())
      
      /// Use this to execute similar queries to test the result of the executed query.
      member x.ToRawSql() =
        x.Parameters
        |> Seq.sortByDescending (fun (n,_) -> n.Length)
        |> Seq.fold (fun (acc:StringBuilder) (pName, pValue) -> 
            match pValue with
            | :? String as pv -> acc.Replace(pName, (sprintf "'%s'" (pv.Replace("'", "''"))))
            | :? Guid as pv -> acc.Replace(pName, (sprintf "'%s'" (pv.ToString())))
            | :? DateTime as pv -> acc.Replace(pName, (sprintf "'%s'" (pv.ToString("yyyy-MM-dd HH:mm:ss"))))
            | :? DateTimeOffset as pv -> acc.Replace(pName, (sprintf "'%s'" (pv.ToString("yyyy-MM-dd HH:mm:ss zzz"))))
            | _ -> acc.Replace(pName, (sprintf "%O" pValue))) (StringBuilder x.Command)
        |> string<StringBuilder>

      member x.ToRawSqlWithParamInfo() =
        let arr = x.Parameters |> Seq.toArray
        if arr.Length = 0 then x.Command
        else
            let paramsString = arr |> Seq.fold (fun (sb:StringBuilder) (pName, pValue) -> sb.Append(sprintf "%s - %A; " pName pValue)) (StringBuilder())
            sprintf "%s -- params opened: %s" (x.ToRawSql()) (paramsString.ToString())

   let private sqlEvent = Event<SqlEventData>()
   
   /// This event fires immediately before the execution of every generated query. 
   /// Listen to this event to display or debug the content of your queries.
   [<CLIEvent>]
   let SqlQueryEvent = sqlEvent.Publish

   let private publishSqlQuery = 
      
      let connStrHashCache = ConcurrentDictionary<string, byte[]>()

      fun connStr qry parameters ->
        
        let hashValue = connStrHashCache.GetOrAdd(connStr, fun str -> Text.Encoding.UTF8.GetBytes(str : string) |> Bytes.sha256)

        sqlEvent.Trigger { Command = qry
                           ConnectionStringHash = hashValue
                           Parameters = parameters
                         }

   let internal PublishSqlQuery connStr qry (spc:IDbDataParameter seq) = 
      publishSqlQuery connStr qry (spc |> Seq.map(fun p -> p.ParameterName, p.Value))

   let PublishSqlQueryCol connStr qry (spc:DbParameterCollection) = 
      publishSqlQuery connStr qry [ for p in spc -> (p.ParameterName, p.Value) ]

   let internal PublishSqlQueryICol connStr qry (spc:IDataParameterCollection) = 
      publishSqlQuery connStr qry [ for op in spc do
                                      let p = op :?> IDataParameter
                                      yield (p.ParameterName, p.Value)]


   let private expressionEvent = Event<System.Linq.Expressions.Expression>()
   
   [<CLIEvent>]
   let LinqExpressionEvent = expressionEvent.Publish

   let internal PublishExpression(e) = expressionEvent.Trigger(e)

[<Struct>]
type EntityState =
    | Unchanged
    | Created
    | Modified of string list
    | Delete
    | Deleted

[<Struct>]
type OnConflict = 
    /// Throws an exception if the primary key already exists (default behaviour).
    | Throw
    /// If the primary key already exists, updates the existing row's columns to match the new entity.
    /// Currently supported only on PostgreSQL 9.5+
    | Update
    /// If the primary key already exists, leaves the existing row unchanged.
    /// Currently supported only on PostgreSQL 9.5+
    | DoNothing

type MappedColumnAttribute(name: string) = 
    inherit Attribute()
    member x.Name with get() = name

type ResultSet = seq<(string * obj)[]>
type ReturnSetType =
    | ScalarResultSet of string * obj
    | ResultSet of string * ResultSet
type ReturnValueType =
    | Unit
    | Scalar of string * obj
    | SingleResultSet of string * ResultSet
    | Set of seq<ReturnSetType>

[<System.Runtime.Serialization.DataContract(Name = "SqlEntity", Namespace = "http://schemas.microsoft.com/sql/2011/Contracts"); DefaultMember("Item")>]
type SqlEntity(dc: ISqlDataContext, tableName, columns: ColumnLookup, activeColumnCount:int) =

    let propertyChanged = Event<_,_>()

    let data = Dictionary<string,obj>(activeColumnCount)
    let mutable aliasCache = None

    member val _State = Unchanged with get, set

    member e.Delete() =
        if dc.IsReadOnly then failwith "Context is readonly" else
        e._State <- Delete
        dc.SubmitChangedEntity e

    member internal e.TriggerPropertyChange(name) = propertyChanged.Trigger(e, PropertyChangedEventArgs(name))
    member __.ColumnValuesWithDefinition = seq { for kvp in data -> kvp.Key, kvp.Value, columns.TryFind(kvp.Key) }

    member __.ColumnValues = seq { for kvp in data -> kvp.Key, kvp.Value }
    member __.HasColumn(key, ?comparison)= 
        let comparisonOption = defaultArg comparison StringComparison.InvariantCulture
        columns |> Seq.exists(fun kp -> (kp.Key |> SchemaProjections.buildFieldName).Equals(key, comparisonOption))
    member __.Table= Table.FromFullName tableName
    member __.DataContext with get() = dc

    member __.GetColumn<'T>(key) : 'T =
        let defaultValue() =
            if Type.(=)(typeof<'T>, typeof<string>) then (box String.Empty) :?> 'T
            else Unchecked.defaultof<'T>
        match data.TryGetValue key with
        | false, _ -> defaultValue()
        | true, dataitem ->
           match dataitem with
           | null -> defaultValue()
           | :? System.DBNull -> defaultValue()
           // Postgres array types
           | :? Array as arr -> 
                unbox arr
           // This deals with an oracle specific case where the type mappings says it returns a System.Decimal but actually returns a float!?!?!  WTF...           
           | data when Type.(<>)(typeof<'T>, data.GetType()) && 
                       Type.(<>)(typeof<'T>, typeof<obj>) &&
                       (data :? IConvertible)
                -> unbox <| Convert.ChangeType(data, typeof<'T>)
           | data -> unbox data

    member __.GetColumnOption<'T>(key) : Option<'T> =
        match data.TryGetValue key with
        | false, _ -> None
        | true, dataitem ->
           match dataitem with
           | null -> None
           | :? System.DBNull -> None
           | data when Type.(<>)(data.GetType(), typeof<'T>) && Type.(<>)(typeof<'T>, typeof<obj>) -> Some(unbox<'T> <| Convert.ChangeType(data, typeof<'T>))
           | data -> Some(unbox data)

    member __.GetColumnValueOption<'T>(key) : ValueOption<'T> =
       match data.TryGetValue key with
       | false, _ -> ValueNone
       | true, dataitem ->
           match dataitem with
           | null -> ValueNone
           | :? System.DBNull -> ValueNone
           | data when Type.(<>)(data.GetType(), typeof<'T>) && Type.(<>)(typeof<'T>, typeof<obj>) -> ValueSome(unbox<'T> <| Convert.ChangeType(data, typeof<'T>))
           | data -> ValueSome(unbox data)

    member __.GetPkColumnOption<'T>(keys: string list) : 'T list =
        keys |> List.choose(fun key -> 
            __.GetColumnOption<'T>(key)) 

    member this.GetColumnOptionWithDefinition(key) =
        this.GetColumnOption(key) |> Option.bind (fun v -> Some(box v, columns.TryFind(key)))

    member private e.UpdateField key =
        if dc.IsReadOnly then failwith "Context is readonly" else
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

    member __.SetPkColumnSilent(keys,value) =
        keys |> List.iter(fun x -> data.[x] <- value)

    member e.SetColumn<'t>(key,value : 't) =
        data.[key] <- value
        e.UpdateField key
        e.TriggerPropertyChange key

    member e.SetData(data) = data |> Seq.iter e.SetColumnSilent

    member __.SetColumnOptionSilent(key,value) =
      match value with
      | Some value ->
          if data.ContainsKey key then
              data.[key] <- value
          else data.Add(key,value)
      | None -> data.Remove key |> ignore

    member __.SetPkColumnOptionSilent(keys,value) =
        match value with
        | Some value ->
            keys |> List.iter(fun x -> 
                if data.ContainsKey x then data.[x] <- value
                else data.Add(x,value))
        | None ->
            keys |> List.iter(fun x -> data.Remove x |> ignore)

    member e.SetColumnOption(key,value) =
      match value with
      | Some value ->
          if data.ContainsKey key then data.[key] <- value
          else data.Add(key,value)
          e.TriggerPropertyChange key
      | None -> if data.Remove key then e.TriggerPropertyChange key
      e.UpdateField key

    member e.SetColumnValueOption(key,value) =
      match value with
      | ValueSome value ->
          if data.ContainsKey key then data.[key] <- value
          else data.Add(key,value)
          e.TriggerPropertyChange key
      | ValueNone -> if data.Remove key then e.TriggerPropertyChange key
      e.UpdateField key

    member __.HasValue(key) = data.ContainsKey key

    /// creates a new SQL entity from alias data in this entity
    member internal e.GetSubTable(alias:string,tableName) =

        match aliasCache with
        | Some x -> ()
        | None ->
            aliasCache <- Some (Dictionary<string,SqlEntity>(HashIdentity.Structural))

        match aliasCache.Value.TryGetValue alias with
        | true, entity -> entity
        | false, _ ->
            let tableName = if tableName <> "" then tableName else e.Table.FullName
            let selectedColumns = 
                e.ColumnValues
                |> Seq.toArray
                |> Array.choose (Utilities.checkPred alias)

            let newEntity = SqlEntity(dc, tableName, columns, selectedColumns.Length)
            // attributes names cannot have a period in them unless they are an alias

            selectedColumns
            |> Array.iter( fun (k,v) -> newEntity.SetColumnSilent(k,v))

            aliasCache.Value.Add(alias,newEntity)
            newEntity

    member x.MapTo<'a>(?propertyTypeMapping : (string * obj) -> obj) =
        let typ = typeof<'a>
        let propertyTypeMapping = defaultArg propertyTypeMapping snd
        let cleanName (n:string) = n.Replace("_","").Replace(" ","").ToLower()
        let clean (pi: PropertyInfo) = 
            match pi.GetCustomAttribute(typeof<MappedColumnAttribute>) with
            | :? MappedColumnAttribute as attr -> attr.Name
            | _ -> pi.Name
            |> cleanName
        let dataMap = x.ColumnValues |> Seq.map (fun (n,v) -> cleanName n, v) |> dict
        if FSharpType.IsRecord typ
        then
            let ctor = FSharpValue.PreComputeRecordConstructor(typ)
            let fields = FSharpType.GetRecordFields(typ)
            let values =
                [|
                    for prop in fields do
                        match dataMap.TryGetValue(clean prop) with
                        | true, data -> yield propertyTypeMapping (prop.Name,data)
                        | false, _ -> ()
                |]
            unbox<'a> (ctor(values))
        else
            let instance = Activator.CreateInstance<'a>()
            for prop in typ.GetProperties() do
                match dataMap.TryGetValue(clean prop) with
                | true, data -> prop.GetSetMethod().Invoke(instance, [|propertyTypeMapping (prop.Name,data)|]) |> ignore
                | false, _ -> ()
            instance
    
    /// Attach/copy entity to a different data-context.
    /// Second parameter: SQL UPDATE or INSERT clause?  
    /// UPDATE: Updates the exising database entity with the values that this entity contains.
    /// INSERT: Makes a copy of entity (database row), which is a new row with the same columns and values (except Id)
    member __.CloneTo(secondContext, itemExistsAlready:bool) = 
        let newItem = SqlEntity(secondContext, tableName, columns, columns.Count)
        if itemExistsAlready then 
            newItem.SetData(data |> Seq.map(fun kvp -> kvp.Key, kvp.Value))
            newItem._State <- Modified (data |> Seq.toList 
                                             |> List.map(fun kvp -> kvp.Key)
                                             |> List.filter(fun k -> k <> "Id"))
        else 
            newItem.SetData(data 
                      |> Seq.filter(fun kvp -> kvp.Key <> "Id" && not (isNull kvp.Value)) 
                      |> Seq.map(fun kvp -> kvp.Key, kvp.Value))
            newItem._State <- Created
        newItem

    /// Makes a copy of entity (database row), which is a new row with the same columns and values (except Id)
    /// If column primaty key is something else and not-auto-generated, then, too bad...
    member __.Clone() = 
        __.CloneTo(dc, false)

    /// Determines what should happen when saving this entity if it is newly-created but another entity with the same primary key already exists
    member val OnConflict = Throw with get, set

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

and ISqlDataContext =
    /// Connection string to database
    abstract ConnectionString           : string
    /// Command timeout (in seconds)
    abstract CommandTimeout             : Option<int>
    /// CreateRelated: instance, _, primary_table, primary_key, foreing_Table, foreign_key, direction. Returns entities
    abstract CreateRelated              : SqlEntity * string * string * string * string * string * RelationshipDirection -> System.Linq.IQueryable<SqlEntity>
    /// Create entities for a table.
    abstract CreateEntities             : string -> System.Linq.IQueryable<SqlEntity>
    /// Call stored procedure: Definition, return columns, values. Returns result.
    abstract CallSproc                  : RunTimeSprocDefinition * QueryParameter[] * obj[] -> obj
    /// Call stored procedure: Definition, return columns, values. Returns result task.
    abstract CallSprocAsync             : RunTimeSprocDefinition * QueryParameter[] * obj[] -> System.Threading.Tasks.Task<SqlEntity>
    /// Get individual row. Takes tablename and id.
    abstract GetIndividual              : string * obj -> SqlEntity
    /// Save entity to database.
    abstract SubmitChangedEntity        : SqlEntity -> unit
    /// Save database-changes in a transaction to database.
    abstract SubmitPendingChanges       : unit -> unit
    /// Save database-changes in a transaction to database.
    abstract SubmitPendingChangesAsync  : unit -> System.Threading.Tasks.Task<unit>
    /// Remove changes that are in context.
    abstract ClearPendingChanges        : unit -> unit
    /// List changes that are in context.
    abstract GetPendingEntities         : unit -> SqlEntity list
    /// Give a tablename and this returns the key name
    abstract GetPrimaryKeyDefinition    : string -> string
    /// return a new, unopened connection using the provided connection string
    abstract CreateConnection           : unit -> IDbConnection
    /// Takes table-name. Returns new entity.
    abstract CreateEntity               : string -> SqlEntity
    /// Read entity. Table name, columns, data-reader. Returns entities.
    abstract ReadEntities               : string * ColumnLookup * IDataReader -> SqlEntity[]
    /// Read entity. Table name, columns, data-reader. Returns entities task.
    abstract ReadEntitiesAsync          : string * ColumnLookup * DbDataReader -> System.Threading.Tasks.Task<SqlEntity[]>
    /// Operations of select in SQL-side or in .NET side?
    abstract SqlOperationsInSelect      : SelectOperations
    /// Save schema offline as Json
    abstract SaveContextSchema          : string -> unit
    /// Context meant to be read operations only
    abstract IsReadOnly                 : bool

/// This is publically exposed and used in the runtime
type IWithDataContext =
    abstract DataContext : ISqlDataContext

/// LinkData is for joins with SelectMany
type LinkData =
    { PrimaryTable       : Table
      PrimaryKey         : SqlColumnType list
      ForeignTable       : Table
      ForeignKey         : SqlColumnType list
      OuterJoin          : bool
      RelDirection       : RelationshipDirection      }
    with
        member x.Rev() =
            { x with PrimaryTable = x.ForeignTable; PrimaryKey = x.ForeignKey; ForeignTable = x.PrimaryTable; ForeignKey = x.PrimaryKey }

/// GroupData is for group-by projections
type GroupData =
    { PrimaryTable       : Table
      KeyColumns         : (alias * SqlColumnType) list
      AggregateColumns   : (alias * SqlColumnType) list
      Projection         : Expression option }

type table = string

type SelectData = LinkQuery of LinkData | GroupQuery of GroupData | CrossJoin of struct (alias * Table)
type [<Struct>] UnionType = NormalUnion | UnionAll | Intersect | Except
type SqlExp =
    | BaseTable    of struct (alias * Table)                // name of the initiating IQueryable table - this isn't always the ultimate table that is selected
    | SelectMany   of alias * alias * SelectData * SqlExp   // from alias, to alias and join data including to and from table names. Note both the select many and join syntax end up here
    | FilterClause of Condition * SqlExp                    // filters from the where clause(es)
    | HavingClause of Condition * SqlExp                    // filters from the where clause(es)
    | Projection   of Expression * SqlExp                   // entire LINQ projection expression tree
    | Distinct     of SqlExp                                // distinct indicator
    | OrderBy      of alias * SqlColumnType * bool * SqlExp // alias and column name, bool indicates ascending sort
    | Union        of UnionType * string * seq<IDbDataParameter> * SqlExp  // union type and subquery
    | Skip         of int * SqlExp
    | Take         of int * SqlExp
    | Count        of SqlExp
    | AggregateOp  of alias * SqlColumnType * SqlExp
    with 
        member this.HasAutoTupled() =
            let rec aux = function
                | BaseTable(_) -> false
                | SelectMany(_) -> true
                | FilterClause(_,rest)
                | HavingClause(_,rest)
                | Projection(_,rest)
                | Distinct rest
                | OrderBy(_,_,_,rest)
                | Skip(_,rest)
                | Take(_,rest)
                | Union(_,_,_,rest)
                | Count(rest) 
                | AggregateOp(_,_,rest) -> aux rest
            aux this
        member this.hasGroupBy() =
            let rec isGroupBy = function
                | SelectMany(_, _,GroupQuery(gdata),_) -> Some (gdata.PrimaryTable, gdata.KeyColumns)
                | BaseTable(_) -> None
                | SelectMany(_) -> None
                | FilterClause(_,rest)
                | HavingClause(_,rest)
                | Projection(_,rest)
                | Distinct rest
                | OrderBy(_,_,_,rest)
                | Skip(_,rest)
                | Take(_,rest)
                | Union(_,_,_,rest)
                | Count(rest) 
                | AggregateOp(_,_,rest) -> isGroupBy rest
            isGroupBy this
        member this.hasSortBy() =
            let rec isSortBy = function
                | OrderBy(_) -> true
                | BaseTable(_) -> false
                | SelectMany(_,_,_,rest)
                | FilterClause(_,rest)
                | HavingClause(_,rest)
                | Projection(_,rest)
                | Distinct rest
                | Skip(_,rest)
                | Take(_,rest)
                | Union(_,_,_,rest)
                | Count(rest) 
                | AggregateOp(_,_,rest) -> isSortBy rest
            isSortBy this

type SqlQuery =
    { Filters       : Condition list
      HavingFilters : Condition list
      Links         : (alias * LinkData * alias) list
      CrossJoins    : (alias * Table) list
      Aliases       : Map<string, Table>
      Ordering      : (alias * SqlColumnType * bool) list
      Projection    : Expression list
      Grouping      : (list<alias * SqlColumnType> * list<alias * SqlColumnType>) list //key columns, aggregate columns
      Distinct      : bool
      UltimateChild : (string * Table) option
      Skip          : int voption
      Take          : int voption
      Union         : (UnionType*string*seq<IDbDataParameter>) option
      Count         : bool 
      AggregateOp   : (alias * SqlColumnType) list }
    with
        static member Empty = { Filters = []; Links = []; Grouping = []; Aliases = Map.empty; Ordering = []; Count = false; AggregateOp = []; CrossJoins = []
                                Projection = []; Distinct = false; UltimateChild = None; Skip = ValueNone; Take = ValueNone; Union = None; HavingFilters = [] }

        static member ofSqlExp(exp,entityIndex: string ResizeArray) =
            let legaliseName (alias:alias) =
                if alias.StartsWith("_") then alias.TrimStart([|'_'|]) else alias

            let rec convert (q:SqlQuery) = function
                | BaseTable(a,e) -> match q.UltimateChild with
                                        | Some(_) when q.CrossJoins.IsEmpty -> q
                                        | None when q.Links.Length > 0 && q.Links |> List.exists(fun (a',_,_) -> a' = a) |> not ->
                                                // the check here relates to the special case as described in the FilterClause below.
                                                // need to make sure the pre-tuple alias (if applicable) is not used in the projection,
                                                // but rather the later alias of the same object after it has been tupled.
                                                  { q with UltimateChild = Some(legaliseName entityIndex.[0], e) }
                                        | _ -> { q with UltimateChild = Some(legaliseName a,e) }
                | SelectMany(a,b,dat,rest) ->
                   match dat with
                   | LinkQuery(link) when link.RelDirection = RelationshipDirection.Children ->
                         convert { q with Aliases = q.Aliases.Add(legaliseName b,link.ForeignTable).Add(legaliseName a,link.PrimaryTable);
                                          Links = (legaliseName a, link, legaliseName b)  :: q.Links } rest
                   | LinkQuery(link) ->
                         convert { q with Aliases = q.Aliases.Add(legaliseName a,link.ForeignTable).Add(legaliseName b,link.PrimaryTable);
                                         Links = (legaliseName a, link, legaliseName b) :: q.Links  } rest
                   | CrossJoin(a,tbl) ->
                         convert { q with Aliases = q.Aliases.Add(legaliseName a,tbl);
                                          CrossJoins = (legaliseName a, tbl) :: q.CrossJoins } rest
                   | GroupQuery(grp) ->
                         convert { q with 
                                    Aliases = q.Aliases.Add(legaliseName a,grp.PrimaryTable).Add(legaliseName b,grp.PrimaryTable);
                                    Links = q.Links  
                                    Grouping = 
                                        let baseAlias:alias = grp.PrimaryTable.Name
                                        let f = grp.KeyColumns |> List.map (fun (al,k) -> legaliseName (match al<>"" with true -> al | false -> baseAlias), k)
                                        let s = grp.AggregateColumns |> List.map (fun (al,opKey) -> legaliseName (match al<>"" with true -> al | false -> baseAlias), opKey)
                                        (f,s)::q.Grouping
                                    Projection = match grp.Projection with Some p -> p::q.Projection | None -> q.Projection } rest
                | FilterClause(c,rest) ->  convert { q with Filters = (c)::q.Filters } rest
                | HavingClause(c,rest) ->  convert { q with HavingFilters = (c)::q.HavingFilters } rest
                | Projection(exp,rest) ->
                    convert { q with Projection = exp::q.Projection } rest
                | Distinct(rest) ->
                    if q.Distinct then failwith "distinct is applied to the entire query and can only be supplied once"
                    else convert { q with Distinct = true } rest
                | OrderBy(alias,key,desc,rest) ->
                    convert { q with Ordering = (legaliseName alias,key,desc)::q.Ordering } rest
                | Skip(amount, rest) ->
                    if q.Skip.IsSome then failwith "skip may only be specified once"
                    elif amount = 0 then convert q rest
                    else convert { q with Skip = ValueSome amount } rest
                | Take(amount, rest) ->
                    if q.Union.IsSome then failwith "Union and take-limit is not yet supported as SQL-syntax varies."
                    match q.Take with
                    | ValueSome x when amount <= x || amount = 1 -> convert { q with Take = ValueSome(amount) } rest
                    | ValueSome x -> failwith "take may only be specified once"
                    | ValueNone -> convert { q with Take = ValueSome(amount) } rest
                | Count(rest) ->
                    if q.Count then failwith "count may only be specified once"
                    else convert { q with Count = true } rest
                | Union(all,subquery, pars, rest) ->
                    if q.Union.IsSome then failwith "Union may only be specified once. However you can try: xs.Union(ys.Union(zs))"
                    elif q.Take.IsSome then failwith "Union and take-limit is not yet supported as SQL-syntax varies."
                    else convert { q with Union = Some(all,subquery,pars) } rest
                | AggregateOp(alias, operationWithKey, rest) ->
                    convert { q with AggregateOp = (alias, operationWithKey)::q.AggregateOp } rest
            let sq = convert (SqlQuery.Empty) exp
            sq

type ISqlProvider =
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
    abstract GetTables  : IDbConnection * CaseSensitivityChange -> Table array
    /// Queries table descriptions / comments for tooltip-info, table name to description
    abstract GetTableDescription  : IDbConnection * string -> string
    /// Queries the given table and returns a list of its columns
    /// this function should also populate a primary key cache for tables that
    /// have a single non-composite primary key
    abstract GetColumns : IDbConnection * Table -> ColumnLookup
    /// Queries column descriptions / comments for tooltip-info, table name, column name to description
    abstract GetColumnDescription  : IDbConnection * string * string -> string
    /// Returns the primary key column for a given table from the pre-populated cache
    /// as generated by calls to GetColumns
    abstract GetPrimaryKey : Table -> string option
    /// Returns constraint information for a given table, returning two lists of relationships, where
    /// the first are relationships where the table is the primary entity, and the second where
    /// it is the foreign entity
    abstract GetRelationships : IDbConnection * Table -> (Relationship array * Relationship array)
    /// Returns a list of stored procedure metadata
    abstract GetSprocs  : IDbConnection -> Sproc list
    /// Returns the db vendor specific SQL  query to select the top x amount of rows from
    /// the given table
    abstract GetIndividualsQueryText : Table * int -> string
    /// Returns the db vendor specific SQL query to select a single row based on the table and column name specified
    abstract GetIndividualQueryText : Table * string -> string
    /// Returns cached schema information, depending on the provider the cached schema may contain the whole database schema or only the schema for entities referenced in the current context
    abstract GetSchemaCache : unit -> SchemaCache
    /// Writes all pending database changes to database
    abstract ProcessUpdates : IDbConnection * System.Collections.Concurrent.ConcurrentDictionary<SqlEntity,DateTime> * TransactionOptions * Option<int> -> unit
    /// Asynchronously writes all pending database changes to database
    abstract ProcessUpdatesAsync : System.Data.Common.DbConnection * System.Collections.Concurrent.ConcurrentDictionary<SqlEntity,DateTime> * TransactionOptions * Option<int> -> System.Threading.Tasks.Task<unit>
    /// Accepts a SqlQuery object and produces the SQL to execute on the server.
    /// the other parameters are the base table alias, the base table, and a dictionary containing
    /// the columns from the various table aliases that are in the SELECT projection
    abstract GenerateQueryText : SqlQuery * string * Table * Dictionary<string,ResizeArray<ProjectionParameter>> * bool * IDbConnection -> string * ResizeArray<IDbDataParameter>
    /// Builds a command representing a call to a stored procedure
    abstract ExecuteSprocCommand : IDbCommand * QueryParameter[] * QueryParameter[] *  obj[] -> ReturnValueType
    /// Builds a command representing a call to a stored procedure, executing async
    abstract ExecuteSprocCommandAsync : System.Data.Common.DbCommand * QueryParameter[] * QueryParameter[] *  obj[] -> System.Threading.Tasks.Task<ReturnValueType>
    /// Provider specific lock to do provider specific locking
    abstract GetLockObject : unit -> obj
    /// MS Access needs to keep connection open
    abstract CloseConnectionAfterQuery : bool
    /// SQLITE doesn't have command type of stored procedures
    abstract StoredProcedures : bool
    /// MSSDT doesn't do design-time connection
    abstract DesignConnection : bool

and SchemaCache =
    { PrimaryKeys   : ConcurrentDictionary<string,string list>
      Tables        : ConcurrentDictionary<string,Table>
      Columns       : ConcurrentDictionary<string,ColumnLookup>
      Relationships : ConcurrentDictionary<string,Relationship array * Relationship array>
      Sprocs        : ResizeArray<Sproc>
      SprocsParams  : ConcurrentDictionary<string,QueryParameter list> //sproc name and params
      Packages      : ResizeArray<CompileTimeSprocDefinition>
      Individuals   : ConcurrentDictionary<string, (obj * IDictionary<string,obj>) array> //table name and entities
      IsOffline     : bool }
    with
        static member Empty = { 
            PrimaryKeys = ConcurrentDictionary<string,string list>()
            Tables = ConcurrentDictionary<string,Table>()
            Columns = ConcurrentDictionary<string,ColumnLookup>()
            Relationships = ConcurrentDictionary<string,Relationship array * Relationship array>()
            Sprocs = ResizeArray()
            SprocsParams = ConcurrentDictionary<string,QueryParameter list>()
            Packages = ResizeArray()
            Individuals = ConcurrentDictionary<string, _>()
            IsOffline = false }
        static member Load(filePath) =
            use ms = new MemoryStream(Encoding.UTF8.GetBytes(File.ReadAllText(filePath)))
            let ser = System.Runtime.Serialization.Json.DataContractJsonSerializer(typeof<SchemaCache>)
            { (ser.ReadObject(ms) :?> SchemaCache) with IsOffline = true }
        static member LoadOrEmpty(filePath) =
            if String.IsNullOrEmpty(filePath) || (not(System.IO.File.Exists filePath)) then 
                SchemaCache.Empty
            else
                SchemaCache.Load(filePath)
        member this.Save(filePath) =
            use ms = new MemoryStream()
            let ser = System.Runtime.Serialization.Json.DataContractJsonSerializer(this.GetType())
            ser.WriteObject(ms, { this with IsOffline = true });  
            let json = ms.ToArray();  
            File.WriteAllText(filePath, Encoding.UTF8.GetString(json, 0, json.Length))

/// GroupResultItems is an item to create key-igrouping-structure.
/// From the select group-by projection, aggregate operations like Enumerable.Count() 
/// is replaced to GroupResultItems.AggregateCount call and this is used to fetch the 
/// SQL result instead of actually counting anything
type GroupResultItems<'key, 'SqlEntity>(keyname:String*String*String*String*String*String*String, keyval, distinctItem:'SqlEntity) as this =
    inherit ResizeArray<'SqlEntity> ([|distinctItem|]) 
    new(keyname, keyval, distinctItem:'SqlEntity) = GroupResultItems((keyname,"","","","","",""), keyval, distinctItem)
    new((k1,k2):String*String, keyval, distinctItem:'SqlEntity) = GroupResultItems((k1, k2,"","","","",""), keyval, distinctItem)
    new((k1,k2,k3):String*String*String, keyval, distinctItem:'SqlEntity) = GroupResultItems((k1, k2,k3,"","","",""), keyval, distinctItem)
    new((k1,k2,k3,k4):String*String*String*String, keyval, distinctItem:'SqlEntity) = GroupResultItems((k1, k2,k3,k4,"","",""), keyval, distinctItem)
    new((k1,k2,k3,k4,k5):String*String*String*String*String, keyval, distinctItem:'SqlEntity) = GroupResultItems((k1, k2,k3,k4,"","",""), keyval, distinctItem)
    new((k1,k2,k3,k4,k5,k6):String*String*String*String*String*String, keyval, distinctItem:'SqlEntity) = GroupResultItems((k1, k2,k3,k4,k5,k6,""), keyval, distinctItem)
    member private __.fetchItem<'ret> itemType (columnName:Option<string>) =
        let fetchCol =
            match columnName with
            | None -> (keyname |> fun (x,_,_,_,_,_,_) -> x).ToUpperInvariant()
            | Some c -> c.ToUpperInvariant()
        let itms =
            match box distinctItem with
            | :? SqlEntity ->
                let ent = unbox<SqlEntity> distinctItem
                ent.ColumnValues 
                    |> Seq.filter(fun (s,k) -> 
                        let sUp = s.ToUpperInvariant()
                        (sUp.Contains("_"+fetchCol) || columnName.IsNone) && 
                            (sUp.Contains(itemType+"_")))
            | :? Tuple<SqlEntity,SqlEntity> ->
                let ent1, ent2 = unbox<SqlEntity*SqlEntity> distinctItem
                Seq.concat [| ent1.ColumnValues; ent2.ColumnValues; |]
                    |> Seq.distinct |> Seq.filter(fun (s,k) -> 
                        let sUp = s.ToUpperInvariant()
                        (sUp.Contains("_"+fetchCol) || columnName.IsNone) && 
                            (sUp.Contains(itemType+"_")))
            | :? Tuple<SqlEntity,SqlEntity,SqlEntity> ->
                let ent1, ent2, ent3 = unbox<SqlEntity*SqlEntity*SqlEntity> distinctItem
                Seq.concat [| ent1.ColumnValues; ent2.ColumnValues; ent3.ColumnValues;|]
                    |> Seq.distinct |> Seq.filter(fun (s,k) -> 
                        let sUp = s.ToUpperInvariant()
                        (sUp.Contains("_"+fetchCol) || columnName.IsNone) && 
                            (sUp.Contains(itemType+"_")))
            | :? Tuple<SqlEntity,SqlEntity,SqlEntity,SqlEntity> ->
                let ent1, ent2, ent3, ent4 = unbox<SqlEntity*SqlEntity*SqlEntity*SqlEntity> distinctItem
                Seq.concat [| ent1.ColumnValues; ent2.ColumnValues; ent3.ColumnValues;ent4.ColumnValues;|]
                    |> Seq.distinct |> Seq.filter(fun (s,k) -> 
                        let sUp = s.ToUpperInvariant()
                        (sUp.Contains("_"+fetchCol) || columnName.IsNone) && 
                            (sUp.Contains(itemType+"_")))
            | :? Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<SqlEntity,SqlEntity> ->
                let ent = unbox<Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<SqlEntity,SqlEntity>> distinctItem
                Seq.concat [| ent.Item1.ColumnValues; ent.Item2.ColumnValues; |]
                    |> Seq.distinct |> Seq.filter(fun (s,k) -> 
                        let sUp = s.ToUpperInvariant()
                        (sUp.Contains("_"+fetchCol) || columnName.IsNone) && 
                            (sUp.Contains(itemType+"_")))
            | :? Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<SqlEntity,SqlEntity,SqlEntity> ->
                let ent = unbox<Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<SqlEntity,SqlEntity,SqlEntity>> distinctItem
                Seq.concat [| ent.Item1.ColumnValues; ent.Item2.ColumnValues; ent.Item3.ColumnValues; |]
                    |> Seq.distinct |> Seq.filter(fun (s,k) -> 
                        let sUp = s.ToUpperInvariant()
                        (sUp.Contains("_"+fetchCol) || columnName.IsNone) && 
                            (sUp.Contains(itemType+"_")))
            | :? Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<SqlEntity,SqlEntity,SqlEntity,SqlEntity> ->
                let ent = unbox<Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject<SqlEntity,SqlEntity,SqlEntity,SqlEntity>> distinctItem
                Seq.concat [| ent.Item1.ColumnValues; ent.Item2.ColumnValues; ent.Item3.ColumnValues; ent.Item4.ColumnValues; |]
                    |> Seq.distinct |> Seq.filter(fun (s,k) -> 
                        let sUp = s.ToUpperInvariant()
                        (sUp.Contains("_"+fetchCol) || columnName.IsNone) && 
                            (sUp.Contains(itemType+"_")))
            | _ -> failwith ("Unknown aggregate item: " + typeof<'SqlEntity>.Name)
        let itm = 
            if Seq.isEmpty itms then
                let cols = (keyname |> fun (x1,x2,x3,x4,x5,x6,x7) -> StringBuilder(x1).Append(" ").Append(x2).Append(" ").Append(x3).Append(" ").Append(x4).Append(" ").Append(x5).Append(" ").Append(x6).Append(" ").Append(x7).ToString()).Trim()
                failwithf "Unsupported aggregate: %s %s" cols (if columnName.IsSome then columnName.Value else "")
            else itms |> Seq.head |> snd
        if itm = box(DBNull.Value) then Unchecked.defaultof<'ret>
        else 
            let returnType = typeof<'ret>
            Utilities.convertTypes itm returnType :?> 'ret
    member __.Values = [|distinctItem|]
    interface System.Linq.IGrouping<'key, 'SqlEntity> with
        member __.Key = keyval
    member __.AggregateCount<'T>(columnName) = this.fetchItem<'T> "COUNT" columnName
    member __.AggregateCountDistinct<'T>(columnName) = this.fetchItem<'T> "COUNTD" columnName
    member __.AggregateSum<'T>(columnName) = 
        let x = this.fetchItem<'T> "SUM" columnName 
        x
    member __.AggregateAverage<'T>(columnName) = this.fetchItem<'T> "AVG" columnName
    member __.AggregateAvg<'T>(columnName) = this.fetchItem<'T> "AVG" columnName
    member __.AggregateMin<'T>(columnName) = this.fetchItem<'T> "MIN" columnName
    member __.AggregateMax<'T>(columnName) = this.fetchItem<'T> "MAX" columnName
    member __.AggregateStandardDeviation<'T>(columnName) = this.fetchItem<'T> "STDDEV" columnName
    member __.AggregateStDev<'T>(columnName) = this.fetchItem<'T> "STDDEV" columnName
    member __.AggregateStdDev<'T>(columnName) = this.fetchItem<'T> "STDDEV" columnName
    member __.AggregateVariance<'T>(columnName) = this.fetchItem<'T> "VAR" columnName
    static member op_Implicit(x:GroupResultItems<'key, 'SqlEntity>) : Microsoft.FSharp.Linq.RuntimeHelpers.Grouping<'key, 'SqlEntity> =
        Microsoft.FSharp.Linq.RuntimeHelpers.Grouping((x :> System.Linq.IGrouping<_,_>).Key, x.Values)
    static member op_Implicit(x:Microsoft.FSharp.Linq.RuntimeHelpers.Grouping<'key, 'SqlEntity>) : GroupResultItems<'key, 'SqlEntity> =
        let key = x.GetType().GetField("key", BindingFlags.NonPublic ||| BindingFlags.Instance)
        let v = key.GetValue(x) |> unbox<'key>
        let i = x |> Seq.head
        GroupResultItems<'key, 'SqlEntity>("", v, i)

module CommonTasks =

    let ``ensure columns have been loaded`` (provider:ISqlProvider) (con:IDbConnection) (entities:ConcurrentDictionary<SqlEntity, DateTime>) =
        entities |> Seq.map(fun e -> e.Key.Table)
                    |> Seq.distinct
                    |> Seq.iter(fun t -> provider.GetColumns(con,t) |> ignore )

    let checkKey (pkLookup:ConcurrentDictionary<string, string list>) id (e:SqlEntity) =
        match pkLookup.TryGetValue e.Table.FullName with
        | true, pkKey ->
            match e.GetPkColumnOption pkKey with
            | [] ->  e.SetPkColumnSilent(pkKey, id)
            | _  -> () // if the primary key exists, do nothing
                            // this is because non-identity columns will have been set
                            // manually and in that case scope_identity would bring back 0 "" or whatever
        | false, _ -> ()

    let parseHaving fieldNotation (keys:(alias*SqlColumnType) list) (conditionList : Condition list) =
        if keys.Length <> 1 then
            failwithf "Unsupported having. Expected 1 key column, found: %i" keys.Length
        else
            let basealias, key = keys.[0]
            let replaceAlias = function "" -> basealias | x -> x
            let replaceEmptyKey = 
                match key with
                | KeyColumn keyName -> function GroupColumn (KeyOp k,c) when k = "" -> GroupColumn (KeyOp keyName,c) | x -> x
                | _ -> id

            let rec parseFilters conditionList = 
                conditionList |> List.map(function 
                    | Condition.And(curr, tail) -> 
                        let converted = curr |> List.map (fun (alias,c,op,i) -> replaceAlias alias, replaceEmptyKey c, op, i)
                        Condition.And(converted, tail |> Option.map parseFilters)
                    | Condition.Or(curr,tail) -> 
                        let converted = curr |> List.map (fun (alias,c,op,i) -> replaceAlias alias, replaceEmptyKey c, op, i)
                        Condition.Or(curr, tail |> Option.map parseFilters)
                    | x -> x)
            parseFilters conditionList

    /// deterministically sort entities before processing in a creation order, so that user can reliably save entities with foreign key relations
    let inline sortEntities (entities: ConcurrentDictionary<SqlEntity, DateTime>) = entities |> Seq.sortBy (fun e -> e.Value) |> Seq.map (fun e -> e.Key)

    /// Check if we know primary column data type from cache.
    /// This helps matching parameter types if there are many different DBTypes mapped to same .NET type, like nvarchar and varchar to string.
    let searchDataTypeFromCache (provider:ISqlProvider) con (sqlQuery:SqlQuery) (baseAlias:string) (baseTable:Table) (alias:string) (col:SqlColumnType) =
        match col with
        | KeyColumn name when not (String.IsNullOrEmpty alias) ->
            if alias = baseAlias then
                match provider.GetColumns(con,baseTable).TryGetValue name with
                | true, col -> ValueSome col.TypeMapping.DbType
                | _ -> ValueNone
            else
                match sqlQuery.Aliases.TryGetValue alias with
                | true, table ->
                    match provider.GetColumns(con,table).TryGetValue name with
                    | true, col -> ValueSome col.TypeMapping.DbType
                    | _ -> ValueNone
                | false, _ -> ValueNone
        | _ -> ValueNone

    let initCallSproc (dc:ISqlDataContext) (def:RunTimeSprocDefinition) (values:obj array) (con:IDbConnection) (com:IDbCommand) hasStoredProcedures =
        
        if hasStoredProcedures then 
            com.CommandType <- CommandType.StoredProcedure

        let columns =
            def.Params
            |> List.map (fun p -> p.Name, Column.FromQueryParameter(p))
            |> Map.ofList

        let entity = SqlEntity(dc, def.Name.DbName, columns, columns.Count)

        let toEntityArray rowSet =
            [|
                for row in rowSet do
                    let entity = SqlEntity(dc, def.Name.DbName, columns, columns.Count)
                    entity.SetData(row)
                    yield entity
            |]

        let param = def.Params |> List.toArray

        QueryEvents.PublishSqlQuery dc.ConnectionString (sprintf "EXEC %s(%s)" com.CommandText (String.concat ", " (values |> Seq.map (sprintf "%A")))) []
        param, entity, toEntityArray

module public OfflineTools =

    /// Merges two ContexSchemaPath offline schema files into one target schema file.
    /// This is a tool method that can be useful in multi-project solution using the same database with different tables.
    let mergeCacheFiles(sourcefile1, sourcefile2, targetfile) =
        if not(System.IO.File.Exists sourcefile1) then "File not found: " + sourcefile1
        elif not(System.IO.File.Exists sourcefile2) then "File not found: " + sourcefile2
        else
        if System.IO.File.Exists targetfile then
            System.IO.File.Delete targetfile
        let s1 = SchemaCache.Load sourcefile1
        let s2 = SchemaCache.Load sourcefile2
        let merged = 
            {   PrimaryKeys = System.Collections.Concurrent.ConcurrentDictionary( 
                                Seq.concat [|s1.PrimaryKeys ; s2.PrimaryKeys |] |> Seq.distinctBy(fun d -> d.Key));
                Tables = System.Collections.Concurrent.ConcurrentDictionary( 
                                Seq.concat [|s1.Tables ; s2.Tables |] |> Seq.distinctBy(fun d -> d.Key));
                Columns = System.Collections.Concurrent.ConcurrentDictionary( 
                                Seq.concat [|s1.Columns ; s2.Columns |] |> Seq.distinctBy(fun d -> d.Key));
                Relationships = System.Collections.Concurrent.ConcurrentDictionary( 
                                    Seq.concat [|s1.Relationships ; s2.Relationships |] |> Seq.distinctBy(fun d -> d.Key));
                Sprocs = ResizeArray(Seq.concat [| s1.Sprocs ; s2.Sprocs |] |> Seq.distinctBy(fun s ->
                                        let rec getName = 
                                            function
                                            | Root(name, sp) -> name + "_" + (getName sp)
                                            | Package(n, ctpd) -> n + "_" + ctpd.ToString()
                                            | Sproc ctpd -> ctpd.ToString()
                                            | Empty -> ""
                                        getName s));
                SprocsParams = System.Collections.Concurrent.ConcurrentDictionary( 
                                Seq.concat [|s1.SprocsParams ; s2.SprocsParams |] |> Seq.distinctBy(fun d -> d.Key));
                Packages = ResizeArray(Seq.concat [| s1.Packages ; s2.Packages |] |> Seq.distinctBy(fun s -> s.ToString()));
                Individuals = System.Collections.Concurrent.ConcurrentDictionary( 
                                Seq.concat [|s1.Individuals ; s2.Individuals |] |> Seq.distinctBy(fun d -> d.Key));
                IsOffline = s1.IsOffline || s2.IsOffline}
        merged.Save targetfile
        "Merge saved " + targetfile + " at " + DateTime.Now.ToString("hh:mm:ss")

    open System.Linq
    open System.Collections

    type MockQueryable<'T>(dc:ISqlDataContext, itms:IQueryable<'T>) = //itms:IQueryable<'T>) =
        interface FSharp.Data.Sql.Patterns.ISqlQueryable
        interface IQueryable<'T>
        interface IQueryable with
            member __.Provider = itms.Provider
            member x.Expression =  itms.Expression
            member __.ElementType = itms.ElementType
        interface seq<'T> with
             member __.GetEnumerator() = itms.GetEnumerator()
        interface IEnumerable with
             member x.GetEnumerator() = itms.GetEnumerator()
        interface IWithDataContext with
             member __.DataContext = dc

    let internal makeColumns (dummydata: obj) =
        let tableItem = (dummydata :?> seq<obj> |> Seq.head).GetType()

        let columnNames =
            tableItem.GetProperties()
            |> Array.map(fun col -> col.Name, col.PropertyType.Name)

        let cols = columnNames |> Array.map(fun (nam,typ) ->
            let tempCol = { Name = nam
                            TypeMapping = TypeMapping.Create(typ); IsPrimaryKey = false
                            IsNullable = String.IsNullOrEmpty typ || typ.StartsWith "FSharpOptio" || typ.StartsWith "FSharpValueOption"
                            IsAutonumber = false;  HasDefault = false; IsComputed = false; TypeInfo = ValueNone }
            nam, tempCol) |> ColumnLookup
        columnNames, cols


    let internal createMockEntitiesDc<'T when 'T :> SqlEntity> dc (tableName:string) (dummydata: obj) =
        let columnNames, cols = makeColumns dummydata
        let rowData = 
            dummydata :?> seq<obj>
            |> Seq.map(fun row ->
                let entity = SqlEntity(dc, tableName, cols, cols.Count)
                for (col, _) in columnNames do
                    let colProp = row.GetType().GetProperty(col)
                    let colData = if isNull colProp then null else colProp.GetValue(row, null)
                    let typ = if isNull colData || isNull (colData.GetType()) then "" else colData.GetType().Name
                    if typ.StartsWith "FSharpOptio" || typ.StartsWith "FSharpValueOption" then
                        let noneProp = colData.GetType().GetProperty("IsNone")
                        let optIsNone =
                            if isNull noneProp then null
                            elif noneProp.GetIndexParameters().Length = 0 then
                                colData.GetType().GetProperty("IsNone").GetValue(colData, null)
                            else
                                isNull (colData.GetType().GetProperty "Value")
                        if (isNull optIsNone) || optIsNone = true then
                            entity.SetColumnOptionSilent(col, None)
                        else
                            let optValProp = colData.GetType().GetProperty("Value")
                            if isNull optValProp then 
                                entity.SetColumnOptionSilent(col, None)
                            else
                                let optVal = optValProp.GetValue(colData, null)
                                entity.SetColumnOptionSilent(col, Some optVal)
                    else
                        entity.SetColumnSilent(col, colData)
                entity :?> 'T)
        MockQueryable(dc, rowData.AsQueryable()) :> IQueryable<'T>

    /// This can be used for testing. Creates de-attached entities..
    /// Example: FSharp.Data.Sql.Common.OfflineTools.CreateMockEntities "MyTable1" [| {| MyColumn1 = "a"; MyColumn2 = 0 |} |]
    let CreateMockEntities<'T when 'T :> SqlEntity> (tableName:string) (dummydata: obj) =
        createMockEntitiesDc<'T> Unchecked.defaultof<ISqlDataContext> tableName dummydata

    /// This can be used for testing. Creates fake DB-context entities..
    /// Example: FSharp.Data.Sql.Common.OfflineTools.CreateMockSqlDataContext ["schema.MyTable1"; [| {| MyColumn1 = "a"; MyColumn2 = 0 |} |] :> obj] |> Map.ofList
    /// See project unit-test for more examples.
    /// NOTE: Case-sensitivity. Tables and columns are DB-names, not Linq-names.
    /// Limitation of mockContext: You cannot Create new entities to the mock context.
    let CreateMockSqlDataContext<'T> (dummydata: Map<string,obj>) =
        let pendingChanges = System.Collections.Concurrent.ConcurrentDictionary<SqlEntity, DateTime>()
        let x = { new ISqlDataContext with
                    member this.CallSproc(arg1: FSharp.Data.Sql.Schema.RunTimeSprocDefinition, arg2: FSharp.Data.Sql.Schema.QueryParameter array, arg3: obj array) =
                        // Note: Calling Sproc result on mock will still fail because SqlEntity "ResultSet" is null and not an array.
                        SqlEntity(this, arg1.Name.FullName, Array.empty |> ColumnLookup, 0)
                    member this.CallSprocAsync(arg1: FSharp.Data.Sql.Schema.RunTimeSprocDefinition, arg2: FSharp.Data.Sql.Schema.QueryParameter array, arg3: obj array) =
                        // Note: Calling Sproc result on mock will still fail because SqlEntity "ResultSet" is null and not an array.
                        task { return SqlEntity(this, arg1.Name.FullName, Array.empty |> ColumnLookup, 0) }
                    member this.ClearPendingChanges(): unit = pendingChanges.Clear()
                    member this.CommandTimeout: Option<int> = None
                    member this.CreateConnection(): Data.IDbConnection = raise (System.NotImplementedException())
                    member this.CreateEntities(arg1: string): IQueryable<SqlEntity> =
                        match dummydata.TryGetValue arg1 with
                        | true, tableData -> createMockEntitiesDc this arg1 tableData
                        | false, _ -> failwith ("Add table to dummydata: " + arg1) 
                    member this.CreateEntity(arg1: string): SqlEntity =
                        match dummydata.TryGetValue arg1 with
                        | true, tableData ->
                            let _, cols = makeColumns tableData
                            new SqlEntity(this, arg1, cols, cols.Count)
                        | false, _ -> new SqlEntity(this, arg1, Seq.empty |> ColumnLookup, 0)
                    member this.CreateRelated(inst: SqlEntity, arg2: string, pe: string, pk: string, fe: string, fk: string, direction: RelationshipDirection): IQueryable<SqlEntity> =
                        if direction = RelationshipDirection.Children then
                            match dummydata.TryGetValue fe with
                            | true, tableData ->
                                let related = createMockEntitiesDc this fe tableData
                                match (inst.ColumnValues |> Map.ofSeq).TryGetValue pk with
                                | true, relevant ->
                                    related.Where(fun e -> e.ColumnValues |> Seq.exists(fun (k, v) -> k = fk && v = relevant))
                                | false, _ -> failwith ("Key not found: " + arg2 + " " + pk)
                            | false, _ -> failwith ("Add table to dummydata: " + fe)
                        else
                            match dummydata.TryGetValue pe with
                            | true, tableData ->
                                    let related = createMockEntitiesDc this pe tableData
                                    match (inst.ColumnValues |> Map.ofSeq).TryGetValue fk with
                                    | true, relevant -> 
                                        related.Where(fun e -> e.ColumnValues |> Seq.exists(fun (k, v) -> k = pk && v = relevant))
                                    | false, _ -> failwith ("Key not found: " + arg2 + " " + fk)
                            | false, _ -> failwith ("Add table to dummydata: " + pe)
                    member this.GetIndividual(arg1: string, arg2: obj): SqlEntity = raise (System.NotImplementedException())
                    member this.GetPendingEntities(): SqlEntity list = (CommonTasks.sortEntities pendingChanges) |> Seq.toList
                    member this.GetPrimaryKeyDefinition(arg1: string): string = ""
                    member this.ReadEntities(arg1: string, arg2: FSharp.Data.Sql.Schema.ColumnLookup, arg3: Data.IDataReader): SqlEntity array = raise (System.NotImplementedException())
                    member this.ReadEntitiesAsync(arg1: string, arg2: FSharp.Data.Sql.Schema.ColumnLookup, arg3: Data.Common.DbDataReader): Threading.Tasks.Task<SqlEntity array> = raise (System.NotImplementedException())
                    member _.SaveContextSchema(arg1: string): unit = ()
                    member _.SqlOperationsInSelect = FSharp.Data.Sql.SelectOperations.DotNetSide
                    member _.SubmitChangedEntity(arg1: SqlEntity): unit = pendingChanges.AddOrUpdate(arg1, DateTime.UtcNow, fun oldE dt -> DateTime.UtcNow) |> ignore
                    member _.SubmitPendingChanges(): unit = ()
                    member _.SubmitPendingChangesAsync(): Threading.Tasks.Task<unit> = task {return ()}
                    member _.ConnectionString = ""
                    member _.IsReadOnly = false
               }
        x :> obj |> unbox<'T>
