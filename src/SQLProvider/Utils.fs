﻿namespace FSharp.Data.Sql.Common
    
open System
open System.Collections.Generic

module internal Utilities = 
    
    open System.IO
    open System.Collections.Concurrent
    open FSharp.Data.Sql

    type TempFile(path:string) =
         member val Path = path with get
         interface IDisposable with 
            member this.Dispose() = File.Delete(path)

    let tempFile(extension : string) =
        let filename =
            let tempF = Path.GetTempFileName()
            let tempF' = Path.ChangeExtension(tempF, extension)
            if tempF <> tempF' then
                File.Delete tempF
            tempF'
        new TempFile(filename)
    
    let resolveTuplePropertyName (name:string) (tupleIndex:string ResizeArray) =
        // eg "Item1" -> tupleIndex.[0]
        let itemid = 
            if name.Length > 4 then
                (int <| name.Remove(0, 4))
            else Int32.MaxValue
        if(tupleIndex.Count < itemid) then ""
        else tupleIndex.[itemid - 1]


    let quoteWhiteSpace (str:String) = 
        (if str.Contains(" ") then sprintf "\"%s\"" str else str)

    let uniqueName()= 
        let dict = new ConcurrentDictionary<string, int>()
        (fun name -> 
            match dict.AddOrUpdate(name,(fun n -> 0),(fun n v -> v + 1)) with
            | 0 -> name
            | count -> name + "_" + (string count)
        )

    /// DB-connections are not usually supporting parallel SQL-query execution, so even when
    /// async thread is available, it can't be used to execute another SQL at the same time.
    let rec executeOneByOne asyncFunc entityList =
        match entityList with
        | h::t -> 
            async {
                do! asyncFunc h
                return! executeOneByOne asyncFunc t
            }
        | [] -> async { () }

    let parseAggregates fieldNotat fieldNotationAlias query =
        let rec parseAggregates' fieldNotation fieldNotationAlias query (selectColumns:string list) =
            match query with
            | [] -> selectColumns |> Seq.distinct |> Seq.toList
            | (opAlias, (aggCol:SqlColumnType))::tail ->
                let parsed = 
                         ((fieldNotation opAlias aggCol) + " as " + fieldNotationAlias(opAlias, aggCol)) :: selectColumns
                parseAggregates' fieldNotation fieldNotationAlias tail parsed
        parseAggregates' fieldNotat fieldNotationAlias query []

    let rec convertTypes (itm:obj) (returnType:Type) =
        if returnType.Name.StartsWith("Option") && returnType.GenericTypeArguments.Length = 1 then
            if itm = null then None |> box
            else Option.Some(convertTypes itm (returnType.GenericTypeArguments.[0]) |> unbox) |> box
        elif returnType.Name.StartsWith("Nullable") && returnType.GenericTypeArguments.Length = 1 then
            if itm = null then null |> box
            else convertTypes itm (returnType.GenericTypeArguments.[0])
        else
        match itm, returnType with
        | :? string as s, t when t = typeof<Int32> && Int32.TryParse s |> fst -> Int32.Parse s |> box
        | :? string as s, t when t = typeof<Decimal> && Decimal.TryParse s |> fst -> Decimal.Parse s |> box
        | :? string as s, t when t = typeof<DateTime> && DateTime.TryParse s |> fst -> DateTime.Parse s |> box
        | :? string as s, t when t = typeof<Int64> && Int64.TryParse s |> fst -> Int64.Parse s |> box
        | :? string as s, t when t = typeof<UInt32> && UInt32.TryParse s |> fst -> UInt32.Parse s |> box
        | :? string as s, t when t = typeof<UInt64> && UInt64.TryParse s |> fst -> UInt64.Parse s |> box
        | :? string as s, t when t = typeof<float32> && Single.TryParse s |> fst -> Single.Parse s |> box
        | :? string as s, t when t = typeof<Int16> && Int16.TryParse s |> fst -> Int16.Parse s |> box
        | :? string as s, t when t = typeof<Boolean> && Boolean.TryParse s |> fst -> Boolean.Parse s |> box
        | _ -> 
            if returnType = typeof<Int32> then Convert.ToInt32 itm |> box
            elif returnType = typeof<decimal> then Convert.ToDecimal itm |> box
            elif returnType = typeof<Int64> then Convert.ToInt64 itm |> box
            elif returnType = typeof<float32> then Convert.ToSingle itm |> box
            elif returnType = typeof<UInt32> then Convert.ToUInt32 itm |> box
            elif returnType = typeof<double> then Convert.ToDouble itm |> box
            elif returnType = typeof<UInt64> then Convert.ToUInt64 itm |> box
            elif returnType = typeof<Int16> then Convert.ToInt16 itm |> box
            elif returnType = typeof<UInt16> then Convert.ToUInt16 itm |> box
            elif returnType = typeof<DateTime> then Convert.ToDateTime itm |> box
            elif returnType = typeof<Boolean> then Convert.ToBoolean itm |> box
            else itm |> box

    /// Standard SQL. Provider spesific overloads can be done before this.
    let genericFieldNotation (recursionBase:SqlColumnType->string) (colSprint:string->string) = function
        | SqlColumnType.KeyColumn col -> colSprint col
        | SqlColumnType.CanonicalOperation(op,key) ->
            let column = recursionBase key
            match op with // These are very standard:
            | ToUpper -> sprintf "UPPER(%s)" column
            | ToLower -> sprintf "LOWER(%s)" column
            | Replace(SqlStr(searchItm),SqlStr(toItm)) -> sprintf "REPLACE(%s,'%s','%s')" column searchItm toItm
            | Abs -> sprintf "ABS(%s)" column
            | Ceil -> sprintf "CEILING(%s)" column
            | Floor -> sprintf "FLOOR(%s)" column
            | Round -> sprintf "ROUND(%s)" column
            | RoundDecimals x -> sprintf "ROUND(%s,%d)" column x
            | BasicMath(o, c) -> sprintf "(%s %s %O)" column o c
            | _ -> failwithf "Not yet supported: %O %s" op (key.ToString())
        | GroupColumn (KeyOp key) -> colSprint key
        | GroupColumn (CountOp _) -> sprintf "COUNT(1)"
        | GroupColumn (AvgOp key) -> sprintf "AVG(%s)" (colSprint key)
        | GroupColumn (MinOp key) -> sprintf "MIN(%s)" (colSprint key)
        | GroupColumn (MaxOp key) -> sprintf "MAX(%s)" (colSprint key)
        | GroupColumn (SumOp key) -> sprintf "SUM(%s)" (colSprint key)

    let rec genericAliasNotation aliasSprint = function
        | SqlColumnType.KeyColumn col -> aliasSprint col
        | SqlColumnType.CanonicalOperation(op,col) -> 
            let subItm = genericAliasNotation aliasSprint col
            aliasSprint (sprintf "%s_%O" (op.ToString().Replace(" ", "_")) subItm)
        | GroupColumn (KeyOp key) -> aliasSprint key
        | GroupColumn (CountOp key) -> aliasSprint (sprintf "COUNT_%s" key)
        | GroupColumn (AvgOp key) -> aliasSprint (sprintf "AVG_%s" key)
        | GroupColumn (MinOp key) -> aliasSprint (sprintf "MIN_%s" key)
        | GroupColumn (MaxOp key) -> aliasSprint (sprintf "MAX_%s" key)
        | GroupColumn (SumOp key) -> aliasSprint (sprintf "SUM_%s" key)


module ConfigHelpers = 
    
    open System
    open System.IO
#if !NETCORE
    open System.Configuration

    let internal getConStringFromConfig isRuntime root (connectionStringName : string) =
                let entryAssembly =
                    match Reflection.Assembly.GetEntryAssembly() with null -> None | x -> Some x

                let root, paths =
                    if isRuntime && entryAssembly.IsSome
                    then entryAssembly.Value.Location, [
                            entryAssembly.Value.GetName().Name + ".exe.config";
                            Path.Combine(root, entryAssembly.Value.GetName().Name + ".exe.config")
                        ]
                    else root, []

                let configFilePath =
                    paths @ [
                        Path.Combine(root, "app.config")
                        Path.Combine(root, "web.config")
                        "app.config"
                        "web.config"
                    ]|> List.tryFind File.Exists

                match configFilePath with
                | Some(configFilePath) ->
                    use tempFile = Utilities.tempFile "config"
                    File.Copy(configFilePath, tempFile.Path)
                    let fileMap = new ExeConfigurationFileMap(ExeConfigFilename = tempFile.Path)
                    let config = ConfigurationManager.OpenMappedExeConfiguration(fileMap, ConfigurationUserLevel.None)
                    match config.ConnectionStrings.ConnectionStrings.[connectionStringName] with
                    | null -> ""
                    | a -> a.ConnectionString
                | None -> ""
#endif

    let cachedConStrings = System.Collections.Concurrent.ConcurrentDictionary<string, string>()

    let tryGetConnectionString isRuntime root (connectionStringName:string) (connectionString:string) =
#if !NETCORE
        if String.IsNullOrWhiteSpace(connectionString)
        then
            match isRuntime with
            | false -> getConStringFromConfig isRuntime root connectionStringName
            | _ -> cachedConStrings.GetOrAdd(connectionStringName, fun name ->
                    let fromFile = getConStringFromConfig isRuntime root connectionStringName
                    fromFile)
        else
#endif
            connectionString

module internal SchemaProjections = 
    
    //Creatviely taken from FSharp.Data (https://github.com/fsharp/FSharp.Data/blob/master/src/CommonRuntime/NameUtils.fs)
    let private tryAt (s:string) i = if i >= s.Length then None else Some s.[i]
    let private sat f (c:option<char>) = match c with Some c when f c -> Some c | _ -> None
    let private (|EOF|_|) c = match c with Some _ -> None | _ -> Some ()
    let private (|LetterDigit|_|) = sat Char.IsLetterOrDigit
    let private (|Upper|_|) = sat (fun c -> Char.IsUpper c || Char.IsDigit c)
    let private (|Lower|_|) = sat (fun c -> Char.IsLower c || Char.IsDigit c)
    
    // --------------------------------------------------------------------------------------
    
    /// Turns a given non-empty string into a nice 'PascalCase' identifier
    let nicePascalName (s:string) = 
      if s.Length = 1 then s.ToUpperInvariant() else
      // Starting to parse a new segment 
      let rec restart i = seq {
        match tryAt s i with 
        | EOF -> ()
        | LetterDigit _ & Upper _ -> yield! upperStart i (i + 1)
        | LetterDigit _ -> yield! consume i false (i + 1)
        | _ -> yield! restart (i + 1) }
      // Parsed first upper case letter, continue either all lower or all upper
      and upperStart from i = seq {
        match tryAt s i with 
        | Upper _ -> yield! consume from true (i + 1) 
        | Lower _ -> yield! consume from false (i + 1) 
        | _ ->
            yield from, i
            yield! restart (i + 1) }
      // Consume are letters of the same kind (either all lower or all upper)
      and consume from takeUpper i = seq {
        match tryAt s i with
        | Lower _ when not takeUpper -> yield! consume from takeUpper (i + 1)
        | Upper _ when takeUpper -> yield! consume from takeUpper (i + 1)
        | Lower _ when takeUpper ->
            yield from, (i - 1)
            yield! restart (i - 1)
        | _ -> 
            yield from, i
            yield! restart i }
        
      // Split string into segments and turn them to PascalCase
      seq { for i1, i2 in restart 0 do 
              let sub = s.Substring(i1, i2 - i1) 
              if Array.forall Char.IsLetterOrDigit (sub.ToCharArray()) then
                yield sub.[0].ToString().ToUpperInvariant() + sub.ToLowerInvariant().Substring(1) }
      |> String.concat ""
    
    /// Turns a given non-empty string into a nice 'camelCase' identifier
    let niceCamelName (s:string) = 
      let name = nicePascalName s
      if name.Length > 0 then
        name.[0].ToString().ToLowerInvariant() + name.Substring(1)
      else name
    
    let buildTableName (tableName:string) = 
        //Current Name = [SCHEMA].[TABLE_NAME]
        if(tableName.Contains("."))
        then 
            let tableName = tableName.Replace("[", "").Replace("]", "")
            let startIndex = tableName.IndexOf(".")
            nicePascalName (tableName.Substring(startIndex))
        else nicePascalName tableName

    let buildFieldName (fieldName:string) = nicePascalName fieldName
    
    let buildSprocName (sprocName:string) = nicePascalName sprocName

    let buildTableNameWhereFilter columnName (tableNames : string) =
        let trim (s:string) = s.Trim()
        let names = tableNames.Split([|","|], StringSplitOptions.RemoveEmptyEntries)
                    |> Seq.map trim
                    |> Seq.toArray
        match names with
        | [||] -> ""
        | [|name|] -> sprintf "and %s like '%s'" columnName name
        | _ -> names |> Array.map (sprintf "%s like '%s'" columnName)
                     |> String.concat " or "
                     |> sprintf "and (%s)"

module internal Reflection = 
    
    open System.Reflection
    open System.IO

    let tryLoadAssembly path = 
         try 
#if NETCORE
             let loadedAsm = Assembly.Load(path) 
#else
             let loadedAsm = Assembly.LoadFrom(path) 
#endif
             if loadedAsm <> null
             then Some(Choice1Of2 loadedAsm)
             else None
         with e ->
             Some(Choice2Of2 e)

    let tryLoadAssemblyFrom resolutionPath (referencedAssemblies:string[]) assemblyNames =
        let referencedPaths = 
            referencedAssemblies 
            |> Array.filter (fun ra -> assemblyNames |> List.exists(fun a -> ra.Contains(a)))
            |> Array.toList
        
        let resolutionPaths =
            assemblyNames 
            |> List.map (fun asm ->
                if String.IsNullOrEmpty resolutionPath 
                then asm
                else Path.Combine(resolutionPath,asm))

        let myPath = 
#if INTERACTIVE
            __SOURCE_DIRECTORY__
#else
#if NETCORE
                System.Reflection.Assembly.GetEntryAssembly().Location
                |> System.IO.Path.GetDirectoryName
#else
            System.Reflection.Assembly.GetExecutingAssembly().Location
            |> Path.GetDirectoryName
#endif
#endif
        let currentPaths =
            assemblyNames 
            |> List.map (fun asm -> System.IO.Path.Combine(myPath,asm))

        let allPaths =
            (assemblyNames @ resolutionPaths @ referencedPaths @ currentPaths) 
            |> Seq.distinct |> Seq.toList
        
        let result = 
            allPaths
            |> List.tryPick (fun p -> 
                match tryLoadAssembly p with
                | Some(Choice1Of2 ass) -> Some ass
                | _ -> None
            )

        // Some providers have additional references to other libraries.
        // https://stackoverflow.com/questions/18942832/how-can-i-dynamically-reference-an-assembly-that-looks-for-another-assembly
        System.AppDomain.CurrentDomain.add_AssemblyResolve (
            System.ResolveEventHandler (fun _ args ->
                
                let extraPathDirs =[resolutionPath; myPath]
                let loaded = 
                    extraPathDirs |> List.tryPick(fun dllPath ->
                        let fileName = args.Name.Split(',').[0] + ".dll"
                        let assemblyPath = Path.Combine(dllPath,fileName)
                        if File.Exists assemblyPath then
                            Some(Assembly.LoadFrom assemblyPath)
                        else None)
                match loaded with
                | Some x -> x
                | None -> null))
        
        match result with
        | Some asm -> Choice1Of2 asm
        | None ->
            let folders = 
                allPaths
                |> Seq.map (Path.GetDirectoryName)
                |> Seq.distinct
            let errors = 
                allPaths
                |> List.map (fun p -> 
                    match tryLoadAssembly p with
                    | Some(Choice2Of2 err) when (err :? System.IO.FileNotFoundException) -> None //trivial
                    | Some(Choice2Of2 err) -> Some err
                    | _ -> None
                ) |> List.filter Option.isSome
                |> List.map(fun o -> o.Value.GetBaseException().Message)
                |> Seq.distinct |> Seq.toList
            if not(String.IsNullOrEmpty resolutionPath) && not(System.IO.Directory.Exists(resolutionPath)) then
                let x = "" :: errors
                Choice2Of2(folders, ("resolutionPath directory doesn't exist:" + resolutionPath::errors))
            else
                Choice2Of2(folders, errors)

module Sql =
    
    open System
    open System.Data

    let private collectfunc(reader:IDataReader) = 
        [|
            for i = 0 to reader.FieldCount - 1 do 
                match reader.GetValue(i) with
                | null | :? DBNull ->  yield (reader.GetName(i),null)
                | value -> yield (reader.GetName(i),value)
        |]
        
    let dataReaderToArray (reader:IDataReader) = 
        [| 
            while reader.Read() do
               yield collectfunc reader
        |]

    let dataReaderToArrayAsync (reader:System.Data.Common.DbDataReader) = 

        let rec readitems acc =
            async {
                let! moreitems = reader.ReadAsync() |> Async.AwaitTask
                match moreitems with
                | true -> return! readitems (collectfunc(reader)::acc)
                | false -> return acc
            }
        async {
            let! items = readitems []
            return items |> List.toArray
        }

    let dbUnbox<'a> (v:obj) : 'a = 
        if Convert.IsDBNull(v) then Unchecked.defaultof<'a> else unbox v
    
    let dbUnboxWithDefault<'a> def (v:obj) : 'a = 
        if Convert.IsDBNull(v) then def else unbox v

    let connect (con:IDbConnection) f =
        if con.State <> ConnectionState.Open then con.Open()
        let result = f con
        con.Close(); result

    let connectAsync (con:System.Data.Common.DbConnection) f =
        async {
            if con.State <> ConnectionState.Open then 
                do! con.OpenAsync() |> Async.AwaitIAsyncResult |> Async.Ignore
            let result = f con
            con.Close(); result
        }

    let executeSql createCommand sql (con:IDbConnection) =        
        use com : IDbCommand = createCommand sql con   
        com.ExecuteReader()    

    let executeSqlAsync createCommand sql (con:IDbConnection) =
        use com : System.Data.Common.DbCommand = createCommand sql con   
        com.ExecuteReaderAsync() |> Async.AwaitTask  

    let executeSqlAsDataTable createCommand sql con = 
        use r = executeSql createCommand sql con
        let dt = new DataTable()
        dt.Load(r)
        dt

    let executeSqlAsDataTableAsync createCommand sql con = 
        async{
            use! r = executeSqlAsync createCommand sql con
            let dt = new DataTable()
            dt.Load(r)
            return dt
        }

    let ensureOpen (con:IDbConnection) =
        if con.State <> ConnectionState.Open
        then con.Open()
