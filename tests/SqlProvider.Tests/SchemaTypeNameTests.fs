module SchemaTypeNameTests

// A saved schema's type names come from the runtime that saved it; both runtimes must read both kinds.

open System
open System.Net
open FSharp.Data.Sql.Common
open NUnit.Framework

let private savedOnNet10 name assembly token = sprintf "%s, %s, Version=10.0.0.0, Culture=neutral, PublicKeyToken=%s" name assembly token
let private savedOnNetFramework name assembly token = sprintf "%s, %s, Version=4.0.0.0, Culture=neutral, PublicKeyToken=%s" name assembly token
let private coreLib = "7cec85d7bea7798e"
let private mscorlib = "b77a5c561934e089"

[<Test; SqlCheck.NoSql>]
let ``a name saved on this runtime resolves`` () =
    Assert.AreEqual(typeof<Guid>, Utilities.getType typeof<Guid>.AssemblyQualifiedName)
    Assert.AreEqual(typeof<string>, Utilities.getType "System.String")

[<Test; SqlCheck.NoSql>]
let ``core library types saved on .NET resolve`` () =
    Assert.AreEqual(typeof<string>, Utilities.getType (savedOnNet10 "System.String" "System.Private.CoreLib" coreLib))
    Assert.AreEqual(typeof<DateTime>, Utilities.getType (savedOnNet10 "System.DateTime" "System.Private.CoreLib" coreLib))
    Assert.AreEqual(typeof<byte[]>, Utilities.getType (savedOnNet10 "System.Byte[]" "System.Private.CoreLib" coreLib))

[<Test; SqlCheck.NoSql>]
let ``core library types saved on .NET Framework resolve`` () =
    Assert.AreEqual(typeof<decimal>, Utilities.getType (savedOnNetFramework "System.Decimal" "mscorlib" mscorlib))
    Assert.AreEqual(typeof<int[]>, Utilities.getType (savedOnNetFramework "System.Int32[]" "mscorlib" mscorlib))

[<Test; SqlCheck.NoSql>]
let ``a later version of the saving runtime still resolves`` () =
    Assert.AreEqual(typeof<string>, Utilities.getType "System.String, System.Private.CoreLib, Version=99.0.0.0, Culture=neutral, PublicKeyToken=7cec85d7bea7798e")

[<Test; SqlCheck.NoSql>]
let ``a type in another assembly on the other runtime resolves`` () =
    let ipAddress = typeof<IPAddress>
    Assert.AreEqual(ipAddress, Utilities.getType (savedOnNet10 "System.Net.IPAddress" "System.Net.Primitives" "b03f5f7f11d50a3a"))
    Assert.AreEqual(ipAddress, Utilities.getType (savedOnNetFramework "System.Net.IPAddress" "System" mscorlib))

[<Test; SqlCheck.NoSql>]
let ``the arguments of a generic type resolve as well`` () =
    let stringOnNet10 = savedOnNet10 "System.String" "System.Private.CoreLib" coreLib
    let hstore = sprintf "System.Collections.Generic.IDictionary`2[[%s],[%s]]" stringOnNet10 stringOnNet10
    Assert.AreEqual(typeof<Collections.Generic.IDictionary<string,string>>, Utilities.getType (savedOnNet10 hstore "System.Private.CoreLib" coreLib))
    let stringOnNetFramework = savedOnNetFramework "System.String" "mscorlib" mscorlib
    let hstore = sprintf "System.Collections.Generic.IDictionary`2[[%s],[%s]]" stringOnNetFramework stringOnNetFramework
    Assert.AreEqual(typeof<Collections.Generic.IDictionary<string,string>>, Utilities.getType (savedOnNetFramework hstore "mscorlib" mscorlib))
    let nullableInts = sprintf "System.Nullable`1[[%s]][]" (savedOnNet10 "System.Int32" "System.Private.CoreLib" coreLib)
    Assert.AreEqual(typeof<Nullable<int>[]>, Utilities.getType (savedOnNet10 nullableInts "System.Private.CoreLib" coreLib))
    let ipAddress = savedOnNet10 "System.Net.IPAddress" "System.Net.Primitives" "b03f5f7f11d50a3a"
    Assert.AreEqual(typeof<Lazy<IPAddress>>, Utilities.getType (savedOnNet10 (sprintf "System.Lazy`1[[%s]]" ipAddress) "System.Private.CoreLib" coreLib))

[<Test; SqlCheck.NoSql>]
let ``a name without an assembly is not searched in the loaded assemblies`` () =
    Assert.AreEqual(typeof<Assert>, Utilities.getType "NUnit.Framework.Assert, No.Such.Assembly, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null")
    Assert.IsNull(Utilities.getType "NUnit.Framework.Assert")

[<Test; SqlCheck.NoSql>]
let ``an unknown type is null`` () =
    Assert.IsNull(Utilities.getType "No.Such.Type")
    Assert.IsNull(Utilities.getType "No.Such.Type, NoSuchAssembly, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null")
    Assert.IsNull(Utilities.getType (savedOnNet10 "System.NoSuchType" "System.Private.CoreLib" coreLib))
    Assert.IsNull(Utilities.getType "")
