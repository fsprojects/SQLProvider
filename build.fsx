// --------------------------------------------------------------------------------------
// FAKE build script 
// --------------------------------------------------------------------------------------

#I @"packages/FAKE/tools/"

#r @"FakeLib.dll"
open Fake 
open Fake.Git
open Fake.AssemblyInfoFile
open Fake.ReleaseNotesHelper
open System

type Project = { name:string; summary:string; description:string; dependencies:(string * string) list }

let projects =
    [{name="OdbcProvider";summary="Type provider for ODBC access";description="Type providers for ODBC access";dependencies=[]}]

let authors = ["Ross McKinlay" ]
let tags = "F# fsharp typeproviders sql sqlserver"

let solutionFile  = "SQLProvider"

let testAssemblies = "tests/**/bin/Release/*Tests*.dll"
let gitHome = "https://github.com/fsprojects"
let gitName = "SQLProvider"
let nugetDir = "./nuget/"

// Read additional information from the release notes document
Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
let release = parseReleaseNotes (IO.File.ReadAllLines "RELEASE_NOTES.md")

// Generate assembly info files with the right version & up-to-date information
Target "AssemblyInfo" (fun _ ->
    projects
    |> Seq.iter (fun project ->
      let fileName = "src/" + project.name + "/AssemblyInfo.fs"
      CreateFSharpAssemblyInfo fileName
          [ Attribute.Title project.name
            Attribute.Product project.name
            Attribute.Description project.summary
            Attribute.Version release.AssemblyVersion
            Attribute.FileVersion release.AssemblyVersion ])
)

// --------------------------------------------------------------------------------------
// Clean build results & restore NuGet packages

Target "RestorePackages" (fun _ ->
    !! "./**/packages.config"
    |> Seq.iter (RestorePackage (fun p -> { p with ToolPath = "./.nuget/NuGet.exe" }))
)

Target "Clean" (fun _ ->
    CleanDirs ["bin"; "temp"; nugetDir]
)

Target "CleanDocs" (fun _ ->
    CleanDirs ["docs/output"]
)

// --------------------------------------------------------------------------------------
// Build library & test project

Target "Build" (fun _ ->
    !! (solutionFile + ".sln")
    |> MSBuildRelease "" "Rebuild"
    |> ignore

    !! (solutionFile + ".Tests.sln")
    |> MSBuildRelease "" "Rebuild"
    |> ignore    
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
    let nugetDocsDir = nugetDir @@ "docs"
    let nugetlibDir = nugetDir @@ "lib/net40"

    CleanDir nugetDocsDir
    CleanDir nugetlibDir
        
    CopyDir nugetlibDir "bin" (fun file -> file.Contains "FSharp.Core." |> not)
    CopyDir nugetDocsDir "./docs/output" allFiles
    
    projects
    |> Seq.iter (fun project -> 
        NuGet (fun p -> 
            { p with   
                Authors = authors
                Project = project.name
                Summary = project.summary
                Description = project.description
                Version = release.NugetVersion
                ReleaseNotes = release.Notes |> toLines
                Tags = tags
                OutputPath = nugetDir
                AccessKey = getBuildParamOrDefault "nugetkey" ""
                Publish = hasBuildParam "nugetkey"
                Dependencies = project.dependencies })
            "SqlProvider.nuspec")
)

// --------------------------------------------------------------------------------------
// Generate the documentation

Target "GenerateDocs" (fun _ ->
    executeFSIWithArgs "docs/tools" "generate.fsx" ["--define:RELEASE"] [] |> ignore
)

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
    Branches.pushBranch tempDocsDir "origin" "gh-pages"
)


Target "Release" DoNothing

// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override

Target "All" DoNothing

"Clean"
  ==> "RestorePackages"
  ==> "AssemblyInfo"
  ==> "Build"
  ==> "RunTests"
  ==> "All"

"All" 
  ==> "CleanDocs"
  ==> "GenerateDocs"
  ==> "ReleaseDocs"
  ==> "NuGet"
  ==> "Release"

RunTargetOrDefault "All"
