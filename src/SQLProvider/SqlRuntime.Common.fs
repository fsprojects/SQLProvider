namespace FSharp.Data.Sql.Common

open System
open System.Collections.Generic
open System.ComponentModel
open System.Data
open System.Linq.Expressions
open FSharp.Data.Sql
open FSharp.Data.Sql.Patterns
open FSharp.Data.Sql.Schema

type DatabaseProviderTypes =
    | MSSQLSERVER = 0
    | SQLITE = 1
    | POSTGRESQL = 2
    | MYSQL = 3
    | ORACLE = 4
    | MSACCESS = 5
    | ODBC = 6
type RelationshipDirection = Children = 0 | Parents = 1 
    
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
    | Deleted

[<System.Runtime.Serialization.DataContractAttribute(Name = "SqlEntity", Namespace = "http://schemas.microsoft.com/sql/2011/Contracts");System.Reflection.DefaultMemberAttribute("Item")>]
type SqlEntity(dc:ISqlDataContext,tableName:string) =
    let table = Table.FromFullName tableName
    let propertyChanged = Event<_,_>()
    
    let data = Dictionary<string,obj>()
    let aliasCache = new Dictionary<string,SqlEntity>(HashIdentity.Structural)

    let replaceFirst (text:string) (oldValue:string) (newValue) =
        let position = text.IndexOf oldValue
        if position < 0 then
            text
        else
            text.Substring(0, position) + newValue + text.Substring(position + oldValue.Length)

    member val _State = Unchanged with get, set

    member e.Delete() = 
        e._State <- Deleted
        dc.SubmitChangedEntity e

    member internal e.TriggerPropertyChange(name) = 
        propertyChanged.Trigger(e,PropertyChangedEventArgs(name))

    member e.ColumnValues = seq { for kvp in data -> kvp.Key, kvp.Value }

    member e.Table= table

    member e.DataContext with get() = dc    

    member e.GetColumn<'T>(key) : 'T = 
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
    
    member e.GetColumnOption<'T>(key) : Option<'T> =   
       if data.ContainsKey key then
           match data.[key] with
           | null -> None
           | :? System.DBNull -> None
           | data when data.GetType() <> typeof<'T> && typeof<'T> <> typeof<obj> -> Some(unbox<'T> <| Convert.ChangeType(data, typeof<'T>))           
           | data -> Some(unbox data)           
       else None

    member private e.UpdateField key =
        match e._State with
        | Modified fields -> 
            e._State <- Modified (key::fields)
            e.DataContext.SubmitChangedEntity e
        | Unchanged -> 
            e._State <- Modified [key]
            e.DataContext.SubmitChangedEntity e
        | Deleted -> failwith "You cannot modify an entity that is pending deletion"
        | Created -> ()

    member e.SetColumnSilent(key,value) =
        data.[key] <- value                

    member e.SetColumn(key,value) =        
        data.[key] <- value
        e.UpdateField key        
        e.TriggerPropertyChange key
    
    member e.SetData(data) = data |> Seq.iter e.SetColumnSilent  

    member e.SetColumnOptionSilent(key,value) =
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
    
    member e.HasValue(key) = data.ContainsKey key

    static member internal FromDataReader(con,name,reader:System.Data.IDataReader) =  
        [ while reader.Read() = true do
          let e = SqlEntity(con,name)        
          for i = 0 to reader.FieldCount - 1 do 
              match reader.GetValue(i) with
              | null | :? DBNull -> ()
              | value -> e.SetColumnSilent(reader.GetName(i),value)
          yield e ]

    static member internal FromOutputParameters(con, name, parameters:IDataParameter[]) = 
        let e = SqlEntity(con, name)
        parameters 
        |> Array.iteri(fun i p ->
            e.SetColumnSilent((if (String.IsNullOrEmpty p.ParameterName) then "Column_" + (string i) else p.ParameterName), p.Value)
        )
        [e]

    /// creates a new SQL entity from alias data in this entity
    member internal e.GetSubTable(alias:string,tableName) =
        match aliasCache.TryGetValue alias with
        | true, entity -> entity
        | false, _ -> 
            let tableName = if tableName <> "" then tableName else e.Table.FullName 
            let newEntity = SqlEntity(dc,tableName)
            let pk = sprintf "%sid" tableName
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

            aliasCache.Add(alias,newEntity)
            newEntity    
    
    interface System.ComponentModel.INotifyPropertyChanged with
        [<CLIEvent>] member x.PropertyChanged = propertyChanged.Publish

    interface System.ComponentModel.ICustomTypeDescriptor with
        member e.GetComponentName() = TypeDescriptor.GetComponentName(e,true)
        member e.GetDefaultEvent() = TypeDescriptor.GetDefaultEvent(e,true)
        member e.GetClassName() = e.Table.FullName
        member e.GetEvents(attributes) = TypeDescriptor.GetEvents(e,true)
        member e.GetEvents() = TypeDescriptor.GetEvents(e,null,true)
        member e.GetConverter() = TypeDescriptor.GetConverter(e,true)
        member e.GetPropertyOwner(_) = upcast data
        member e.GetAttributes() = TypeDescriptor.GetAttributes(e,true)
        member e.GetEditor(typeBase) = TypeDescriptor.GetEditor(e,typeBase,true)
        member e.GetDefaultProperty() = null
        member e.GetProperties()  = (e :> ICustomTypeDescriptor).GetProperties(null)
        member e.GetProperties(attributes) = 
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
    abstract ConnectionString       : string
    abstract CreateRelated          : SqlEntity * string * string * string * string * string * RelationshipDirection -> System.Linq.IQueryable<SqlEntity>
    abstract CreateEntities         : string -> System.Linq.IQueryable<SqlEntity>
    abstract CallSproc              : string * (string * DbType * ParameterDirection * int)[]  * obj [] -> SqlEntity list
    abstract GetIndividual          : string * obj -> SqlEntity
    abstract SubmitChangedEntity    : SqlEntity -> unit
    abstract SubmitPendingChanges   : unit -> unit
    abstract ClearPendingChanges    : unit -> unit
    abstract GetPendingEntities     : unit -> SqlEntity list

         
type LinkData =
    { PrimaryTable       : Table
      PrimaryKey         : string
      ForeignTable       : Table
      ForeignKey         : string
      OuterJoin          : bool 
      RelDirection       : RelationshipDirection      }
    with 
        member x.Rev() = 
            { x with PrimaryTable = x.ForeignTable; PrimaryKey = x.ForeignKey; ForeignTable = x.PrimaryTable; ForeignKey = x.PrimaryKey }

type alias = string
type table = string 

type Condition = 
    // this is  (table alias * column name * operator * right hand value ) list  * (the same again list) 
    // basically any AND or OR expression can have N terms and can have N nested condition children
    // this is largely from my CRM type provider. I don't think in practice for the SQL provider 
    // you will ever have more than what is representable in a traditional binary expression tree, but 
    // changing it would be a lot of effort ;) 
    | And of (alias * string * ConditionOperator * obj option) list * (Condition list) option  
    | Or of (alias * string * ConditionOperator * obj option) list * (Condition list) option   

type SqlExp =
    | BaseTable    of alias * Table                      // name of the initiating IQueryable table - this isn't always the ultimate table that is selected 
    | SelectMany   of alias * alias * LinkData * SqlExp  // from alias, to alias and join data including to and from table names. Note both the select many and join syntax end up here
    | FilterClause of Condition * SqlExp                 // filters from the where clause(es) 
    | Projection   of Expression * SqlExp                // entire LINQ projection expression tree
    | Distinct     of SqlExp                             // distinct indicator
    | OrderBy      of alias * string * bool * SqlExp     // alias and column name, bool indicates ascending sort
    | Skip         of int * SqlExp
    | Take         of int * SqlExp
    | Count        of SqlExp
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
                | Count(rest) -> aux rest
            aux this
    
type SqlQuery =
    { Filters       : Condition list
      Links         : (alias * LinkData * alias) list 
      Aliases       : Map<string, Table>
      Ordering      : (alias * string * bool) list
      Projection    : Expression option
      Distinct      : bool
      UltimateChild : (string * Table) option
      Skip          : int option
      Take          : int option
      Count         : bool } 
    with
        static member Empty = { Filters = []; Links = []; Aliases = Map.empty; Ordering = []; Count = false
                                Projection = None; Distinct = false; UltimateChild = None; Skip = None; Take = None }

        static member ofSqlExp(exp,entityIndex: string ResizeArray) =
            let add key item map =
                match Map.tryFind key map with
                | None -> map |> Map.add key [item] 
                | Some(xs) -> map.Remove key |> Map.add key (item::xs)

            let legaliseName (alias:alias) = 
                if alias.StartsWith("_") then alias.TrimStart([|'_'|]) else alias

            let rec convert (q:SqlQuery) = function
                | BaseTable(a,e) -> match q.UltimateChild with
                                        | Some(a,e) -> q
                                        | None when q.Links.Length > 0 && q.Links |> List.exists(fun (a',_,_) -> a' = a) = false -> 
                                                // the check here relates to the special case as described in the FilterClause below.
                                                // need to make sure the pre-tuple alias (if applicable) is not used in the projection,
                                                // but rather the later alias of the same object after it has been tupled.
                                                  { q with UltimateChild = Some(legaliseName entityIndex.[0], e) }
                                        | None -> { q with UltimateChild = Some(legaliseName a,e) }
                | SelectMany(a,b,link,rest) as e -> 
                   match link.RelDirection with
                   | RelationshipDirection.Children -> 
                         convert { q with Aliases = q.Aliases.Add(legaliseName b,link.ForeignTable).Add(legaliseName a,link.PrimaryTable);
                                          Links = (legaliseName a, link, legaliseName b)  :: q.Links } rest
                   | _ ->
                         convert { q with Aliases = q.Aliases.Add(legaliseName a,link.ForeignTable).Add(legaliseName b,link.PrimaryTable);
                                         Links = (legaliseName a, link, legaliseName b) :: q.Links  } rest
                | FilterClause(c,rest) as e ->  convert { q with Filters = (c)::q.Filters } rest 
                | Projection(exp,rest) as e ->  
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
                    if q.Take.IsSome then failwith "take may only be specified once"
                    else convert { q with Take = Some(amount) } rest
                | Count(rest) -> 
                    if q.Take.IsSome then failwith "count may only be specified once"
                    else convert { q with Count = true } rest
            let sq = convert (SqlQuery.Empty) exp
            sq

type ISqlProvider =
    /// return a new, unopened connection using the provided connection string
    abstract CreateConnection : string -> IDbConnection
    /// return a new command associated with the provided connection and command text
    abstract CreateCommand : IDbConnection * string -> IDbCommand
    /// return a new command parameter with the provided name, value and optionally type, direction and length
    abstract CreateCommandParameter : string * obj * DbType option * ParameterDirection option * int option -> IDataParameter
    /// This function will be called when the provider is first created and should be used
    /// to generate a cache of type mappings, and to set the three mapping function properties
    abstract CreateTypeMappings : IDbConnection -> Unit
    /// Accepts a full CLR type name and returns the relevant value from the DbType enum
    abstract ClrToEnum : (string -> DbType option) with get
    /// Accepts SQL data type name and returns the relevant value from the DbType enum
    abstract SqlToEnum : (string -> DbType option) with get
    /// Accepts SQL data type name and returns the CLR type
    abstract SqlToClr : (string -> Type option)       with get
    /// Queries the information schema and returns a list of table information
    abstract GetTables  : IDbConnection -> Table list
    /// Queries the given table and returns a list of its columns
    /// this function should also populate a primary key cache for tables that
    /// have a single non-composite primary key
    abstract GetColumns : IDbConnection * Table -> Column list 
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
    abstract ProcessUpdates : IDbConnection * SqlEntity list -> unit
    /// Accepts a SqlQuery object and produces the SQL to execute on the server.
    /// the other parameters are the base table alias, the base table, and a dictionary containing 
    /// the columns from the various table aliases that are in the SELECT projection
    abstract GenerateQueryText : SqlQuery * string * Table * Dictionary<string,ResizeArray<string>> -> string * ResizeArray<IDataParameter>
    
