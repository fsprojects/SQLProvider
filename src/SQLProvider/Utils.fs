namespace FSharp.Data.Sql.Common
    
open System
open System.Collections.Generic

module internal Utilities = 
    
    open System.IO

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
        let dict = new Dictionary<string, int>()
        (fun name -> 
            match dict.TryGetValue(name) with
            | true, count -> 
                  dict.[name] <- count + 1
                  name + "_" + (string count)
            | false, _ -> 
                  dict.[name] <- 0
                  name
               
        )

module ConfigHelpers = 
    
    open System
    open System.IO
    open System.Configuration

    let internal getConStringFromConfig isRuntime root (connectionStringName : string) =
                let entryAssembly =
                    Reflection.Assembly.GetEntryAssembly()

                let root, paths =
                    if isRuntime
                    then entryAssembly.Location, [entryAssembly.GetName().Name + ".exe.config"]
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

    let cachedConStrings = Dictionary<string, string>()

    let tryGetConnectionString isRuntime root (connectionStringName:string) (connectionString:string) =
        if String.IsNullOrWhiteSpace(connectionString)
        then
            match isRuntime with
            | false -> getConStringFromConfig isRuntime root connectionStringName
            | _ -> match cachedConStrings.TryGetValue connectionStringName with
                   | (true, cached) -> cached
                   | _ ->
                       lock cachedConStrings (fun () ->
                           match cachedConStrings.TryGetValue connectionStringName with
                           | (true, cached) -> cached
                           | _ ->
                                let fromFile = getConStringFromConfig isRuntime root connectionStringName
                                cachedConStrings.Add(connectionStringName, fromFile)
                                fromFile
                        )
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

        let allPaths =
            (assemblyNames @ resolutionPaths @ referencedPaths) 
        
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
            allPaths
            |> Seq.map (IO.Path.GetDirectoryName)
            |> Seq.distinct
            |> Choice2Of2

module Sql =
    
    open System
    open System.Data

    let dataReaderToArray (reader:IDataReader) = 
        [| 
            while reader.Read() do
               yield [|      
                    for i = 0 to reader.FieldCount - 1 do 
                        match reader.GetValue(i) with
                        | null | :? DBNull ->  yield (reader.GetName(i),null)
                        | value -> yield (reader.GetName(i),value)
               |]
        |]

    let dbUnbox<'a> (v:obj) : 'a = 
        if Convert.IsDBNull(v) then Unchecked.defaultof<'a> else unbox v
    
    let dbUnboxWithDefault<'a> def (v:obj) : 'a = 
        if Convert.IsDBNull(v) then def else unbox v

    let connect (con:IDbConnection) f =
        if con.State <> ConnectionState.Open then con.Open()
        let result = f con
        con.Close(); result

    let executeSql createCommand sql (con:IDbConnection) =        
        use com : IDbCommand = createCommand sql con   
        com.ExecuteReader()    

    let executeSqlAsDataTable createCommand sql con = 
        use r = executeSql createCommand sql con
        let dt = new DataTable()
        dt.Load(r)
        dt
