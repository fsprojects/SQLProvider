namespace FSharp.Data.Sql.Common

open System
open System.Collections.Generic
open System.ComponentModel
open System.Data
open System.Data.Common
open System.Linq.Expressions
open System.Reflection
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
type RelationshipDirection = Children = 0 | Parents = 1

type CaseSensitivityChange =
    | ORIGINAL = 0
    | TOUPPER = 1
    | TOLOWER = 2

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
    // .NET Framework default
    | SystemDataSQLite = 0
    // Mono version
    | MonoDataSQLite = 1
    // Auto-select by environment
    | AutoSelect = 2
    // Microsoft.Data.Sqlite. May support .NET Standard 2.0 contract in the future.
    | MicrosoftDataSqlite = 3

module public QueryEvents =
   open System.Data.SqlClient

   let private expressionEvent = new Event<System.Linq.Expressions.Expression>()
   
   type SqlEventData = {
       Command: string
       Parameters: (string*obj) seq
   }
   with 
      override x.ToString() =
        let paramsString = x.Parameters |> Seq.fold (fun acc (pName, pValue) -> acc + (sprintf "%s - %A; " pName pValue)) ""
        sprintf "%s - params %s" x.Command paramsString

   let private sqlEvent = new Event<SqlEventData>()

   [<CLIEvent>]
   let LinqExpressionEvent = expressionEvent.Publish

   [<CLIEvent>]
   let SqlQueryEvent = sqlEvent.Publish

   let PublishExpression(e) = expressionEvent.Trigger(e)
   let PublishSqlQuery qry (spc:IDbDataParameter seq) = sqlEvent.Trigger( {Command = qry; Parameters = spc |> Seq.map(fun p -> p.ParameterName, p.Value) })
   let PublishSqlQueryCol qry (spc:DbParameterCollection) = 
        sqlEvent.Trigger( {Command = qry; Parameters = [for p in spc do yield (p.ParameterName, p.Value)] })
   let PublishSqlQueryICol qry (spc:IDataParameterCollection) = 
        sqlEvent.Trigger(
            { Command = qry; 
              Parameters = [ for op in spc do
                               let p = op :?> IDataParameter
                               yield (p.ParameterName, p.Value)] })

type EntityState =
    | Unchanged
    | Created
    | Modified of string list
    | Delete
    | Deleted

[<System.Runtime.Serialization.DataContract(Name = "SqlEntity", Namespace = "http://schemas.microsoft.com/sql/2011/Contracts"); DefaultMember("Item")>]
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
    member __.HasColumn(key, ?comparison)= 
        let comparisonOption = defaultArg comparison StringComparison.InvariantCulture
        columns |> Seq.exists(fun kp -> (kp.Key |> SchemaProjections.buildFieldName).Equals(key, comparisonOption))
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
           // Postgres array types
           | :? Array as arr -> 
                unbox arr
           // This deals with an oracle specific case where the type mappings says it returns a System.Decimal but actually returns a float!?!?!  WTF...           
           | data when typeof<'T> <> data.GetType() && 
                       typeof<'T> <> typeof<obj>
                -> unbox <| Convert.ChangeType(data, typeof<'T>)
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

    member __.GetPkColumnOption<'T>(keys: string list) : 'T list =
        keys |> List.choose(fun key -> 
            __.GetColumnOption<'T>(key)) 

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
          if not (data.ContainsKey key) then data.Add(key,value)
          else data.[key] <- value
      | None -> data.Remove key |> ignore

    member __.SetPkColumnOptionSilent(keys,value) =
        keys |> List.iter(fun x -> 
            match value with
            | Some value ->
                if not (data.ContainsKey x) then data.Add(x,value)
                else data.[x] <- value
            | None -> data.Remove x |> ignore)

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
                let prefix5 = alias.ToUpper() + "_"
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
                    //this case for Firebird version<=2.1, all uppercase
                    elif  k.StartsWith prefix5 then 
                        let temp = replaceFirst k prefix5 ""
                        Some(temp,v)
                    elif not(String.IsNullOrEmpty(k)) then // this is for dynamic alias columns: [a].[City] as City
                        Some(k,v)
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
    abstract CommandTimeout             : Option<int>
    abstract CreateRelated              : SqlEntity * string * string * string * string * string * RelationshipDirection -> System.Linq.IQueryable<SqlEntity>
    abstract CreateEntities             : string -> System.Linq.IQueryable<SqlEntity>
    abstract CallSproc                  : RunTimeSprocDefinition * QueryParameter[] * obj[] -> obj
    abstract CallSprocAsync             : RunTimeSprocDefinition * QueryParameter[] * obj[] -> Async<SqlEntity>
    abstract GetIndividual              : string * obj -> SqlEntity
    abstract SubmitChangedEntity        : SqlEntity -> unit
    abstract SubmitPendingChanges       : unit -> unit
    abstract SubmitPendingChangesAsync  : unit -> Async<unit>
    abstract ClearPendingChanges        : unit -> unit
    abstract GetPendingEntities         : unit -> SqlEntity list
    abstract GetPrimaryKeyDefinition    : string -> string
    abstract CreateConnection           : unit -> IDbConnection
    abstract CreateEntity               : string -> SqlEntity
    abstract ReadEntities               : string * ColumnLookup * IDataReader -> SqlEntity[]
    abstract ReadEntitiesAsync          : string * ColumnLookup * DbDataReader -> Async<SqlEntity[]>

// LinkData is for joins with SelectMany
and LinkData =
    { PrimaryTable       : Table
      PrimaryKey         : SqlColumnType list
      ForeignTable       : Table
      ForeignKey         : SqlColumnType list
      OuterJoin          : bool
      RelDirection       : RelationshipDirection      }
    with
        member x.Rev() =
            { x with PrimaryTable = x.ForeignTable; PrimaryKey = x.ForeignKey; ForeignTable = x.PrimaryTable; ForeignKey = x.PrimaryKey }

// GroupData is for group-by projections
and GroupData =
    { PrimaryTable       : Table
      KeyColumns         : (alias * SqlColumnType) list
      AggregateColumns   : (alias * SqlColumnType) list
      Projection         : Expression option }

and alias = string
and table = string

and Condition =
    // this is  (table alias * column name * operator * right hand value ) list  * (the same again list)
    // basically any AND or OR expression can have N terms and can have N nested condition children
    // this is largely from my CRM type provider. I don't think in practice for the SQL provider
    // you will ever have more than what is representable in a traditional binary expression tree, but
    // changing it would be a lot of effort ;)
    | And of (alias * SqlColumnType * ConditionOperator * obj option) list * (Condition list) option
    | Or of (alias * SqlColumnType * ConditionOperator * obj option) list * (Condition list) option
    | ConstantTrue
    | ConstantFalse

and SelectData = LinkQuery of LinkData | GroupQuery of GroupData
and UnionType = NormalUnion | UnionAll | Intersect | Except
and internal SqlExp =
    | BaseTable    of alias * Table                         // name of the initiating IQueryable table - this isn't always the ultimate table that is selected
    | SelectMany   of alias * alias * SelectData * SqlExp   // from alias, to alias and join data including to and from table names. Note both the select many and join syntax end up here
    | FilterClause of Condition * SqlExp                    // filters from the where clause(es)
    | HavingClause of Condition * SqlExp                    // filters from the where clause(es)
    | Projection   of Expression * SqlExp                   // entire LINQ projection expression tree
    | Distinct     of SqlExp                                // distinct indicator
    | OrderBy      of alias * SqlColumnType * bool * SqlExp // alias and column name, bool indicates ascending sort
    | Union        of UnionType * string * SqlExp           // union type and subquery
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
                | Union(_,_,rest)
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
                | Union(_,_,rest)
                | Count(rest) 
                | AggregateOp(_,_,rest) -> isGroupBy rest
            isGroupBy this

and internal SqlQuery =
    { Filters       : Condition list
      HavingFilters : Condition list
      Links         : (alias * LinkData * alias) list
      Aliases       : Map<string, Table>
      Ordering      : (alias * SqlColumnType * bool) list
      Projection    : Expression list
      Grouping      : (list<alias * SqlColumnType> * list<alias * SqlColumnType>) list //key columns, aggregate columns
      Distinct      : bool
      UltimateChild : (string * Table) option
      Skip          : int option
      Take          : int option
      Union         : (UnionType*string) option
      Count         : bool 
      AggregateOp   : (alias * SqlColumnType) list }
    with
        static member Empty = { Filters = []; Links = []; Grouping = []; Aliases = Map.empty; Ordering = []; Count = false; AggregateOp = []
                                Projection = []; Distinct = false; UltimateChild = None; Skip = None; Take = None; Union = None; HavingFilters = [] }

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
                | SelectMany(a,b,dat,rest) ->
                   match dat with
                   | LinkQuery(link) when link.RelDirection = RelationshipDirection.Children ->
                         convert { q with Aliases = q.Aliases.Add(legaliseName b,link.ForeignTable).Add(legaliseName a,link.PrimaryTable);
                                          Links = (legaliseName a, link, legaliseName b)  :: q.Links } rest
                   | LinkQuery(link) ->
                         convert { q with Aliases = q.Aliases.Add(legaliseName a,link.ForeignTable).Add(legaliseName b,link.PrimaryTable);
                                         Links = (legaliseName a, link, legaliseName b) :: q.Links  } rest
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
                    else convert { q with Skip = Some(amount) } rest
                | Take(amount, rest) ->
                    if q.Union.IsSome then failwith "Union and take-limit is not yet supported as SQL-syntax varies."
                    match q.Take with
                    | Some x when amount <= x || amount = 1 -> convert { q with Take = Some(amount) } rest
                    | Some x -> failwith "take may only be specified once"
                    | None -> convert { q with Take = Some(amount) } rest
                | Count(rest) ->
                    if q.Count then failwith "count may only be specified once"
                    else convert { q with Count = true } rest
                | Union(all,subquery, rest) ->
                    if q.Union.IsSome then failwith "Union may only be specified once. However you can try: xs.Union(ys.Union(zs))"
                    elif q.Take.IsSome then failwith "Union and take-limit is not yet supported as SQL-syntax varies."
                    else convert { q with Union = Some(all,subquery) } rest
                | AggregateOp(alias, operationWithKey, rest) ->
                    convert { q with AggregateOp = (alias, operationWithKey)::q.AggregateOp } rest
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
    abstract GetRelationships : IDbConnection * Table -> (Relationship list * Relationship list)
    /// Returns a list of stored procedure metadata
    abstract GetSprocs  : IDbConnection -> Sproc list
    /// Returns the db vendor specific SQL  query to select the top x amount of rows from
    /// the given table
    abstract GetIndividualsQueryText : Table * int -> string
    /// Returns the db vendor specific SQL query to select a single row based on the table and column name specified
    abstract GetIndividualQueryText : Table * string -> string
    /// Writes all pending database changes to database
    abstract ProcessUpdates : IDbConnection * System.Collections.Concurrent.ConcurrentDictionary<SqlEntity,DateTime> * TransactionOptions * Option<int> -> unit
    /// Asynchronously writes all pending database changes to database
    abstract ProcessUpdatesAsync : System.Data.Common.DbConnection * System.Collections.Concurrent.ConcurrentDictionary<SqlEntity,DateTime> * TransactionOptions * Option<int> -> Async<unit>
    /// Accepts a SqlQuery object and produces the SQL to execute on the server.
    /// the other parameters are the base table alias, the base table, and a dictionary containing
    /// the columns from the various table aliases that are in the SELECT projection
    abstract GenerateQueryText : SqlQuery * string * Table * Dictionary<string,ResizeArray<string>>*bool -> string * ResizeArray<IDbDataParameter>
    ///Builds a command representing a call to a stored procedure
    abstract ExecuteSprocCommand : IDbCommand * QueryParameter[] * QueryParameter[] *  obj[] -> ReturnValueType
    ///Builds a command representing a call to a stored procedure, executing async
    abstract ExecuteSprocCommandAsync : System.Data.Common.DbCommand * QueryParameter[] * QueryParameter[] *  obj[] -> Async<ReturnValueType>

/// GroupResultItems is an item to create key-igrouping-structure.
/// From the select group-by projection, aggregate operations like Enumerable.Count() 
/// is replaced to GroupResultItems.AggregateCount call and this is used to fetch the 
/// SQL result instead of actually counting anything
type GroupResultItems<'key>(keyname:String*String, keyval, distinctItem:SqlEntity) as this =
    inherit ResizeArray<SqlEntity> ([|distinctItem|]) 
    new(keyname, keyval, distinctItem:SqlEntity) = GroupResultItems((keyname,""), keyval, distinctItem)
    member private __.fetchItem<'ret> itemType (columnName:Option<string>) =
        let fetchCol =
            match columnName with
            | None -> fst(keyname).ToUpperInvariant()
            | Some c -> c.ToUpperInvariant()
        let itm =
            distinctItem.ColumnValues 
            |> Seq.filter(fun (s,k) -> 
                let sUp = s.ToUpperInvariant()
                (sUp.Contains("_"+fetchCol)) && 
                    (sUp.Contains(itemType+"_"))) |> Seq.head |> snd
        if itm = box(DBNull.Value) then Unchecked.defaultof<'ret>
        else 
            let returnType = typeof<'ret>
            Utilities.convertTypes itm returnType :?> 'ret
    member __.Values = [|distinctItem|]
    interface System.Linq.IGrouping<'key, SqlEntity> with
        member __.Key = keyval
    member __.AggregateCount<'T>(columnName) = this.fetchItem<'T> "COUNT" columnName
    member __.AggregateSum<'T>(columnName) = 
        let x = this.fetchItem<'T> "SUM" columnName 
        x
    member __.AggregateAverage<'T>(columnName) = this.fetchItem<'T> "AVG" columnName
    member __.AggregateAvg<'T>(columnName) = this.fetchItem<'T> "AVG" columnName
    member __.AggregateMin<'T>(columnName) = this.fetchItem<'T> "MIN" columnName
    member __.AggregateMax<'T>(columnName) = this.fetchItem<'T> "MAX" columnName

module internal CommonTasks =

    let ``ensure columns have been loaded`` (provider:ISqlProvider) (con:IDbConnection) (entities:ConcurrentDictionary<SqlEntity, DateTime>) =
        entities |> Seq.map(fun e -> e.Key.Table)
                    |> Seq.distinct
                    |> Seq.iter(fun t -> provider.GetColumns(con,t) |> ignore )

    let checkKey (pkLookup:ConcurrentDictionary<string, string list>) id (e:SqlEntity) =
        if pkLookup.ContainsKey e.Table.FullName then
            match e.GetPkColumnOption pkLookup.[e.Table.FullName] with
            | [] ->  e.SetPkColumnSilent(pkLookup.[e.Table.FullName], id)
            | _  -> () // if the primary key exists, do nothing
                            // this is because non-identity columns will have been set
                            // manually and in that case scope_identity would bring back 0 "" or whatever

    let parseHaving fieldNotation (keys:(alias*SqlColumnType) list) (conditionList : Condition list) =
        if keys.Length <> 1 then
            failwithf "Unsupported having. Expected 1 key column, found: %i" keys.Length
        else
            let basealias, key = keys.[0]
            let replaceAlias = function "" -> basealias | x -> x
            let replaceEmptyKey = 
                match key with
                | KeyColumn keyName -> function GroupColumn (KeyOp k,c) when k = "" -> GroupColumn (KeyOp keyName,c) | x -> x
                | _ -> fun x -> x

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