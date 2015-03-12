namespace FSharp.Data.Sql.Common
    
open System

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

module ConfigHelpers = 
    
    open System
    open System.IO
    open System.Configuration

    let tryGetConnectionString isRuntime root (connectionStringName:string) (connectionString:string) =
        if String.IsNullOrWhiteSpace(connectionString)
        then
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
                | null -> None
                | a -> Some(a.ConnectionString)
            | None -> None
        else Some(connectionString)

module internal SchemaProjections = 
    
    let buildTableName (tableName:string) = tableName.Substring(0,tableName.LastIndexOf("]")+1).ToUpper()

    let buildFieldName (fieldName:string) = fieldName.ToUpper()

    let buildSprocName (sprocName:string) = sprocName.ToUpper()

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
                if String.IsNullOrEmpty resolutionPath then asm
                else System.IO.Path.Combine(resolutionPath,asm))

        (assemblyNames @ resolutionPaths @ referencedPaths) 
        |> List.tryPick (fun p -> 
            match tryLoadAssembly p with
            | Some(Choice1Of2 ass) -> Some ass
            | _ -> None
         )

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