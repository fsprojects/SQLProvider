namespace FSharp.Data.Sql

open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Providers

open Microsoft.FSharp.Core.CompilerServices
open Samples.FSharp.ProvidedTypes

[<TypeProvider>]
type MySQLProvider(config) as this =
    inherit TypeProviderForNamespaces()
    let sqlRuntimeInfo = SqlRuntimeInfo(config)
    let ns = "FSharp.Data.Sql";   

    let paramSqlType = ProvidedTypeDefinition(sqlRuntimeInfo.RuntimeAssembly, ns, "MySQLProvider", Some(typeof<obj>), HideObjectMethods = true)
    
    let conString = ProvidedStaticParameter("ConnectionString",typeof<string>)    
    let optionTypes = ProvidedStaticParameter("UseOptionTypes",typeof<bool>,false)
    let individualsAmount = ProvidedStaticParameter("IndividualsAmount",typeof<int>,1000)
    let helpText = "<summary>Typed representation of a database</summary>
                    <param name='ConnectionString'>The connection string for the SQL database</param>
                    <param name='IndividualsAmount'>The amount of sample entities to project into the type system for each SQL entity type. Default 1000.</param>
                    <param name='UseOptionTypes'>If true, F# option types will be used in place of nullable database columns.  If false, you will always receive the default value of the column's type even if it is null in the database.</param>"
        
    do paramSqlType.DefineStaticParameters([conString;individualsAmount;optionTypes], fun typeName args -> 
        SqlTypeProvider.createType(args.[0] :?> string,   // OrganizationServiceUrl
                    "",                                   // Resolution path
                    args.[1] :?> int,                     // Individuals Amount
                    args.[2] :?> bool,                    // Use option types?
                    "",                                   // Schema owner currently only used for oracle
                    DatabaseProviderTypes.MYSQL,
                    sqlRuntimeInfo,
                    ns,
                    typeName))

    do paramSqlType.AddXmlDoc helpText    

    // add them to the namespace    
    do this.AddNamespace(ns, [paramSqlType])  
    
   
[<assembly:TypeProviderAssembly>] 
do()