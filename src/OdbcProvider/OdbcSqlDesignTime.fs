namespace FSharp.Data.Sql

open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Providers

open Microsoft.FSharp.Core.CompilerServices
open Samples.FSharp.ProvidedTypes

[<TypeProvider>]
type OdbcSqlProvider(config) as this =     
    inherit SqlTypeProvider(config)

    override this.CreateSqlProvider() = OdbcProvider() :> ISqlProvider
   
[<assembly:TypeProviderAssembly>] 
do()