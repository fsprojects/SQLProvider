namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("OracleProvider")>]
[<assembly: AssemblyProductAttribute("OracleProvider")>]
[<assembly: AssemblyDescriptionAttribute("Type provider for Oracle access")>]
[<assembly: AssemblyVersionAttribute("0.0.9")>]
[<assembly: AssemblyFileVersionAttribute("0.0.9")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.0.9"
