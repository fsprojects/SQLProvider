namespace FSharp.Data.Sql.Common
    
open System
open System.Collections.Generic

module internal Utilities = 
    
    open System.IO
    open System.Collections.Concurrent

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
        tupleIndex.[(int <| name.Remove(0, 4)) - 1]

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


    let ensureTransaction() =
        // Todo: Take TransactionScopeAsyncFlowOption into use when implemented in Mono.
        // Without it, transactions are not thread-safe over threads e.g. using async can be dangerous)
        // However, default option for TransactionScopeOption is Required, so you can create top level transaction
        // and this Mono-transaction will have its properties.
        let isMono = Type.GetType ("Mono.Runtime") <> null
        match isMono with
        | true -> new Transactions.TransactionScope()
        | false ->
            // Mono would fail to compilation, so we have to construct this via reflection:
            // new Transactions.TransactionScope(Transactions.TransactionScopeAsyncFlowOption.Enabled)
            let transactionAssembly = System.Reflection.Assembly.GetAssembly typeof<System.Transactions.TransactionScope>
            let asynctype = transactionAssembly.GetType "System.Transactions.TransactionScopeAsyncFlowOption"
            let transaction = typeof<System.Transactions.TransactionScope>.GetConstructor [|asynctype|]
            transaction.Invoke [|1|] :?> System.Transactions.TransactionScope

    type internal AggregateOperation =
    | Max
    | Min
    | Sum
    | Avg

    let parseAggregates fieldNotation fieldNotationAlias query =
        let rec parseAggregates' fieldNotation fieldNotationAlias query (selectColumns:string list) =
            match query with
            | [] -> selectColumns
            | (agop, opAlias, sumCol)::tail ->
                let aggregate = 
                    match agop with
                    | Sum -> "SUM"
                    | Max -> "MAX"
                    | Min -> "MIN"
                    | Avg -> "AVG"
                let parsed = 
                         (aggregate + "(" + fieldNotation(opAlias, sumCol) + ") as " + fieldNotationAlias(sumCol, aggregate)) :: selectColumns
                parseAggregates' fieldNotation fieldNotationAlias tail parsed
        parseAggregates' fieldNotation fieldNotationAlias query []

module ConfigHelpers = 
    
    open System
    open System.IO
    open System.Configuration

    let internal getConStringFromConfig isRuntime root (connectionStringName : string) =
                let entryAssembly =
                    match Reflection.Assembly.GetEntryAssembly() with null -> None | x -> Some x

                let root, paths =
                    if isRuntime && entryAssembly.IsSome
                    then entryAssembly.Value.Location, [entryAssembly.Value.GetName().Name + ".exe.config"]
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

    let cachedConStrings = System.Collections.Concurrent.ConcurrentDictionary<string, string>()

    let tryGetConnectionString isRuntime root (connectionStringName:string) (connectionString:string) =
        if String.IsNullOrWhiteSpace(connectionString)
        then
            match isRuntime with
            | false -> getConStringFromConfig isRuntime root connectionStringName
            | _ -> cachedConStrings.GetOrAdd(connectionStringName, fun name ->
                    let fromFile = getConStringFromConfig isRuntime root connectionStringName
                    fromFile)
        else connectionString

module internal SchemaProjections = 
    
    //Creatviely taken from FSharp.Data (https://github.com/fsharp/FSharp.Data/blob/master/src/CommonRuntime/NameUtils.fs)
    let private tryAt (s:string) i = if i >= s.Length then None else Some s.[i]
    let private sat f (c:option<char>) = match c with Some c when f c -> Some c | _ -> None
    let private (|EOF|_|) c = match c with Some _ -> None | _ -> Some ()
    let private (|LetterDigit|_|) = sat Char.IsLetterOrDigit
    let private (|Upper|_|) = sat Char.IsUpper
    let private (|Lower|_|) = sat Char.IsLower
    
    // --------------------------------------------------------------------------------------
    
    /// Turns a given non-empty string into a nice 'PascalCase' identifier
    let nicePascalName (s:string) = 
      if s.Length = 1 then s.ToUpper() else
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
                yield sub.[0].ToString().ToUpper() + sub.ToLower().Substring(1) }
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

module internal Reflection = 
    
    open System.Reflection

    let tryLoadAssembly path = 
         try 
             let loadedAsm = Assembly.LoadFrom(path) 
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
                else System.IO.Path.Combine(resolutionPath,asm))

        let currentPaths =
            let myPath = 
#if INTERACTIVE
                __SOURCE_DIRECTORY__
#else
                System.Reflection.Assembly.GetExecutingAssembly().Location
                |> System.IO.Path.GetDirectoryName
#endif
            assemblyNames 
            |> List.map (fun asm -> System.IO.Path.Combine(myPath,asm))

        let allPaths =
            (assemblyNames @ resolutionPaths @ referencedPaths @ currentPaths) 
        
        let result = 
            allPaths
            |> List.tryPick (fun p -> 
                match tryLoadAssembly p with
                | Some(Choice1Of2 ass) -> Some ass
                | _ -> None
            )
        
        match result with
        | Some asm -> Choice1Of2 asm
        | None ->
            let folders = 
                allPaths
                |> Seq.map (IO.Path.GetDirectoryName)
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
