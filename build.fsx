// --------------------------------------------------------------------------------------
// FAKE build script
// --------------------------------------------------------------------------------------

#r @"packages/FAKE/tools/FakeLib.dll"
open Fake
open Fake.Git
open Fake.AssemblyInfoFile
open Fake.ReleaseNotesHelper
open System
open System.IO

#if MONO
#else
#load @"packages/SourceLink.Fake/tools/SourceLink.fsx"
#endif

#r @"packages/scripts/Npgsql/lib/net451/Npgsql.dll"

// --------------------------------------------------------------------------------------
// START TODO: Provide project-specific details below
// --------------------------------------------------------------------------------------

// Information about the project are used
//  - for version and project name in generated AssemblyInfo file
//  - by the generated NuGet package
//  - to run tests and to publish documentation on GitHub gh-pages
//  - for documentation, you also need to edit info in "docs/tools/generate.fsx"

// The name of the project
// (used by attributes in AssemblyInfo, name of a NuGet package and directory in 'src')

let project = "SQLProvider"

// Short summary of the project
// (used as description in AssemblyInfo and as a short summary for NuGet package)
let summary = "Type providers for SQL database access."

// Longer description of the project
// (used as a description for NuGet package; line breaks are automatically cleaned up)
let description = "Type providers for SQL database access."

// List of author names (for NuGet package)
let authors = [ "Ross McKinlay, Colin Bull" ]

// Tags for your project (for NuGet package)
let tags = "F#, fsharp, typeprovider, sql, sqlserver"

// Pattern specifying assemblies to be tested using NUnit
let testAssemblies = "tests/**/bin/Release/*Tests*.dll"

// Git configuration (used for publishing documentation in gh-pages branch)
// The profile where the project is posted
let gitOwner = "fsprojects"
let gitHome = "https://github.com/" + gitOwner

// The name of the project on GitHub
let gitName = "SQLProvider"

// The url for the raw files hosted
let gitRaw = environVarOrDefault "gitRaw" "https://raw.github.com/fsprojects"

// --------------------------------------------------------------------------------------
// END TODO: The rest of the file includes standard build steps
// --------------------------------------------------------------------------------------
// Read additional information from the release notes document
Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
let release = parseReleaseNotes (IO.File.ReadAllLines "RELEASE_NOTES.md")

// Generate assembly info files with the right version & up-to-date information
Target "AssemblyInfo" (fun _ ->
  let fileName = "src/" + project + "/AssemblyInfo.fs"
  CreateFSharpAssemblyInfo fileName
      [ Attribute.Title project
        Attribute.Product project
        Attribute.Description summary
        Attribute.Version release.AssemblyVersion
        Attribute.FileVersion release.AssemblyVersion ] 
)

// --------------------------------------------------------------------------------------
// Clean build results & restore NuGet packages

Target "RestorePackages" RestorePackages

Target "Clean" (fun _ ->
    DeleteDirs ["bin"; "temp"]
)

Target "CleanDocs" (fun _ ->
    DeleteDirs ["docs/output"]
)

// --------------------------------------------------------------------------------------
// Build library & test project

Target "Build" (fun _ ->

    // Build .NET Framework solution
    !!"SQLProvider.sln" ++ "SQLProvider.Tests.sln"
    |> MSBuildReleaseExt "" [ "DefineConstants", buildServer.ToString().ToUpper()] "Rebuild"
    |> ignore
)

Target "BuildCore" (fun _ ->

    // Build .NET Core solution
    DotNetCli.Restore(fun p -> 
        { p with 
            Project = "src/SQLProvider.Standard/SQLProvider.Standard.fsproj"
            NoCache = true})

    DotNetCli.Build(fun p -> 
        { p with 
            Project = "src/SQLProvider.Standard/SQLProvider.Standard.fsproj"
            Configuration = "Release"})
)

// --------------------------------------------------------------------------------------
// Set up a PostgreSQL database in the CI pipeline to run Postgres tests

Target "SetupPostgreSQL" (fun _ ->
      let connBuilder = Npgsql.NpgsqlConnectionStringBuilder()

      connBuilder.Host <- "localhost"
      connBuilder.Port <- 5432
      connBuilder.Database <- "postgres"
      connBuilder.Username <- "postgres"
      connBuilder.Password <- 
        match buildServer with
        | Travis -> ""
        | AppVeyor -> "Password12!"
        | _ -> "postgres"      
  
      let runCmd query = 
        // We wait up to 30 seconds for PostgreSQL to be initialized
        let rec runCmd' attempt = 
          try
            use conn = new Npgsql.NpgsqlConnection(connBuilder.ConnectionString)
            conn.Open()
            use cmd = new Npgsql.NpgsqlCommand(query, conn)
            cmd.ExecuteNonQuery() |> ignore 
          with e -> 
            printfn "Connection attempt %i: %A" attempt e
            Threading.Thread.Sleep 1000
            if attempt < 30 then runCmd' (attempt + 1)

        runCmd' 0
              
      let testDbName = "sqlprovider"
      printfn "Creating test database %s on connection %s" testDbName connBuilder.ConnectionString
      runCmd (sprintf "CREATE DATABASE %s" testDbName)
      connBuilder.Database <- testDbName

      (!! "src/DatabaseScripts/PostgreSQL/*.sql")
      |> Seq.map (fun file -> printfn "Running script %s on connection %s" file connBuilder.ConnectionString; file)
      |> Seq.map IO.File.ReadAllText      
      |> Seq.iter runCmd
)


// --------------------------------------------------------------------------------------
// Run the unit tests using test runner

Target "RunTests" (fun _ ->
    !! testAssemblies 
    |> NUnit (fun p ->
        { p with
            DisableShadowCopy = true
            TimeOut = TimeSpan.FromMinutes 20.
            OutputFile = "TestResults.xml" })
)

// --------------------------------------------------------------------------------------
// Build a NuGet package

Target "NuGet" (fun _ ->
    // Before release, set your API-key as instructed in the bottom of page https://www.nuget.org/account

#if MONO
#else
    let dotnetSdk = @"C:\Program Files\dotnet\sdk\2.0.0\Microsoft\Microsoft.NET.Build.Extensions\net461\lib\"
    if directoryExists dotnetSdk then
       CopyFile "bin/netstandard2.0" (dotnetSdk + @"netstandard.dll")
       CopyFile "bin/netstandard2.0" (dotnetSdk + @"System.Console.dll")
       CopyFile "bin/netstandard2.0" (dotnetSdk + @"System.IO.dll")
       CopyFile "bin/netstandard2.0" (dotnetSdk + @"System.Reflection.dll")
       CopyFile "bin/netstandard2.0" (dotnetSdk + @"System.Runtime.dll")
    CopyFile "bin/netstandard2.0" "packages/System.Data.SqlClient/lib/net461/System.Data.SqlClient.dll" 
#endif

    CopyDir @"temp/lib" "bin" allFiles

    NuGet (fun p ->
        { p with
            Authors = authors
            Project = project
            Summary = summary
            Description = description
            Version = release.NugetVersion
            ReleaseNotes = String.Join(Environment.NewLine, release.Notes)
            Tags = tags
            WorkingDir = "temp"
            OutputPath = "bin"
            AccessKey = getBuildParamOrDefault "nugetkey" ""
            Publish = hasBuildParam "nugetkey"
            Dependencies = [] })
        (project + ".nuspec")

    CleanDir "Temp"
    Branches.tag "" release.NugetVersion

    // push manually: nuget.exe push bin\SQLProvider.1.*.nupkg -Source https://www.nuget.org/api/v2/package
    //Branches.pushTag "" "upstream" release.NugetVersion
)

Target "PackNuGet" (fun _ -> 
    Paket.Pack(fun p -> 
        { p with 
            Version = release.NugetVersion
            ReleaseNotes = String.Join(Environment.NewLine, release.Notes)
            Symbols = true
            OutputPath = "bin" })
) 

// --------------------------------------------------------------------------------------
// Generate the documentation

Target "GenerateReferenceDocs" (fun _ ->
    if not <| executeFSIWithArgs "docs/tools" "generate.fsx" ["--define:RELEASE"; "--define:REFERENCE"] [] then
      failwith "generating reference documentation failed"
)

let generateHelp' fail debug =
    let args =
        if debug then ["--define:HELP"]
        else ["--define:RELEASE"; "--define:HELP"]
    if executeFSIWithArgs "docs/tools" "generate.fsx" args [] then
        traceImportant "Help generated"
    else
        if fail then
            failwith "generating help documentation failed"
        else
            traceImportant "generating help documentation failed"

let generateHelp fail =
    generateHelp' fail false

Target "GenerateHelp" (fun _ ->
    DeleteFile "docs/content/release-notes.md"
    CopyFile "docs/content/" "RELEASE_NOTES.md"
    Rename "docs/content/release-notes.md" "docs/content/RELEASE_NOTES.md"

    DeleteFile "docs/content/license.md"
    CopyFile "docs/content/" "LICENSE.txt"
    Rename "docs/content/license.md" "docs/content/LICENSE.txt"

    CopyFile "bin/net451" "packages/FSharp.Core/lib/net40/FSharp.Core.sigdata"
    CopyFile "bin/net451" "packages/FSharp.Core/lib/net40/FSharp.Core.optdata"

    generateHelp true
)

Target "GenerateHelpDebug" (fun _ ->
    DeleteFile "docs/content/release-notes.md"
    CopyFile "docs/content/" "RELEASE_NOTES.md"
    Rename "docs/content/release-notes.md" "docs/content/RELEASE_NOTES.md"

    DeleteFile "docs/content/license.md"
    CopyFile "docs/content/" "LICENSE.txt"
    Rename "docs/content/license.md" "docs/content/LICENSE.txt"

    generateHelp' true true
)

Target "KeepRunning" (fun _ ->    
    use watcher = new FileSystemWatcher(DirectoryInfo("docs/content").FullName,"*.*")
    watcher.EnableRaisingEvents <- true
    watcher.Changed.Add(fun e -> generateHelp false)
    watcher.Created.Add(fun e -> generateHelp false)
    watcher.Renamed.Add(fun e -> generateHelp false)
    watcher.Deleted.Add(fun e -> generateHelp false)

    traceImportant "Waiting for help edits. Press any key to stop."

    System.Console.ReadKey() |> ignore

    watcher.EnableRaisingEvents <- false
    watcher.Dispose()
)

Target "GenerateDocs" DoNothing

#if MONO
Target "SourceLink" <| fun () -> ()
#else
open SourceLink
Target "SourceLink" <| fun () ->
    let baseUrl = sprintf "%s/%s/{0}/%%var2%%" gitRaw project
    !! "src/*.fsproj"
    |> Seq.iter (fun file ->
        let proj = VsProj.LoadRelease file
        SourceLink.Index proj.CompilesNotLinked proj.OutputFilePdb __SOURCE_DIRECTORY__ baseUrl
       )
#endif

// --------------------------------------------------------------------------------------
// Release Scripts

Target "ReleaseDocs" (fun _ ->
    let tempDocsDir = "temp/gh-pages"
    CleanDir tempDocsDir
    Repository.cloneSingleBranch "" (gitHome + "/" + gitName + ".git") "gh-pages" tempDocsDir

    fullclean tempDocsDir
    CopyRecursive "docs/output" tempDocsDir true |> tracefn "%A"
    StageAll tempDocsDir
    Commit tempDocsDir (sprintf "Update generated documentation for version %s" release.NugetVersion)
    Branches.push tempDocsDir
)

Target "Release" DoNothing

// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override

Target "All" DoNothing


Target "BuildDocs" DoNothing

"Clean"
  ==> "AssemblyInfo"  
  // In CI mode, we setup a Postgres database before building
  =?> ("SetupPostgreSQL", not isLocalBuild)
  ==> "Build"
  =?> ("BuildCore", isLocalBuild || not isMono)
  ==> "RunTests"
  ==> "CleanDocs"
  // Travis doesn't support mono+dotnet:
  =?> ("GenerateReferenceDocs", isLocalBuild && not isMono)
  =?> ("GenerateHelp", isLocalBuild && not isMono)
  ==> "All"

"All"
  ==> "BuildDocs"

"All" 
#if MONO
#else
  =?> ("SourceLink", Pdbstr.tryFind().IsSome )
#endif
  =?> ("NuGet", not(hasBuildParam "onlydocs"))
  ==> "ReleaseDocs"
  ==> "Release"

"All" 
  ==> "PackNuGet"

RunTargetOrDefault "All"
