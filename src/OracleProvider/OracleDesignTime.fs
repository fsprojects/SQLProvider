namespace FSharp.Data.Sql

open FSharp.Data.Sql.Common
open FSharp.Data.Sql.Providers

open Microsoft.FSharp.Core.CompilerServices
open Samples.FSharp.ProvidedTypes

[<TypeProvider>]
type OracleProvider(config) as this =
    inherit TypeProviderForNamespaces()
    let sqlRuntimeInfo = SqlRuntimeInfo(config)
    let ns = "FSharp.Data.Sql";   

    let paramSqlType = ProvidedTypeDefinition(sqlRuntimeInfo.RuntimeAssembly, ns, "OracleProvider", Some(typeof<obj>), HideObjectMethods = true)
    
    let conString = ProvidedStaticParameter("ConnectionString",typeof<string>)    
    let optionTypes = ProvidedStaticParameter("UseOptionTypes",typeof<bool>,false)
    let individualsAmount = ProvidedStaticParameter("IndividualsAmount",typeof<int>,1000)
    let owner = ProvidedStaticParameter("Owner", typeof<string>, "")
    let resolutionPath = ProvidedStaticParameter("ResolutionPath",typeof<string>,"")   
    let helpText = "<summary>Typed representation of a database</summary>
                    <param name='ConnectionString'>The connection string for the SQL database</param>
                    <param name='IndividualsAmount'>The amount of sample entities to project into the type system for each SQL entity type. Default 1000.</param>
                    <param name='UseOptionTypes'>If true, F# option types will be used in place of nullable database columns.  If false, you will always receive the default value of the column's type even if it is null in the database.</param>
                    <param name='ResolutionPath'>The location to look for dynamically loaded assemblies containing database vendor specific connections and custom types.</param>
                    <param name='Owner'>The owner of the schema for this provider to resolve</param>"
        
    do paramSqlType.DefineStaticParameters([conString;individualsAmount;optionTypes;resolutionPath;owner], fun typeName args -> 
        SqlTypeProvider.createType(args.[0] :?> string,   // OrganizationServiceUrl
                    args.[4] :?> string,                  // Resolution path
                    args.[1] :?> int,                     // Individuals Amount
                    args.[2] :?> bool,                    // Use option types?
                    args.[5] :?> string,                  // Schema owner currently only used for oracle
                    DatabaseProviderTypes.ORACLE,
                    sqlRuntimeInfo,
                    ns,
                    typeName))

    do paramSqlType.AddXmlDoc helpText    

    // add them to the namespace    
    do this.AddNamespace(ns, [paramSqlType])  
    
   
[<assembly:TypeProviderAssembly>] 
do()