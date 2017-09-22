(**

# .NET Standard / .NET Core support #

There are some limitations in .NET-standard version.

You need your database connection driver to also support .NET Core.
If your database connection driver has external dependencies, they has to be also present.

The providers that are not included in .NET Core version:

* Ms-Access in not supported
* Odbc is not supported

Connection string can be passed as hard-coded static parameter (development) or `GetDataContext(connectionstring)` but fetching it automatically from the application configuration is not supported.

.NET Standard solution is located at `/src/SQLProvider.Standard/SQLProvider.Standard.fsproj`

The target frameworks are defined in the project file: `<TargetFrameworks>net461;netcoreapp2.0;netstandard2.0</TargetFrameworks>`
Corresponding files goes to root bin paths, e.g.: \bin\netcoreapp2.0

Does build ok: netcoreapp2.0 
Doesn't build ok: netstandard2.0
The reason is TypedReference used by FieldInfo: https://github.com/Microsoft/visualfsharp/issues/3303#issuecomment-329983262

Current modifications needed: ProvidedTypes.fs needs a type hint at line 94:

```fsharp
|> Seq.forall (fun (originalName:string, newName:string) ->
```

(PR: https://github.com/fsprojects/FSharp.TypeProviders.StarterPack/pull/130)

*)
