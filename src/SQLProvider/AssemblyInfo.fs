namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("SQLProvider")>]
[<assembly: AssemblyProductAttribute("SQLProvider")>]
[<assembly: AssemblyDescriptionAttribute("Type providers for SQL database access.")>]
[<assembly: AssemblyVersionAttribute("1.0.16")>]
[<assembly: AssemblyFileVersionAttribute("1.0.16")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "1.0.16"
    let [<Literal>] InformationalVersion = "1.0.16"
