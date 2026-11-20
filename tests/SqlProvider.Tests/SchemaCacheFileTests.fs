module SchemaCacheFileTests

// The individuals of a saved schema (ContextSchemaPath) are in their own file beside it.

open System
open System.IO
open System.Text
open System.Collections.Generic
open System.Collections.Concurrent
open FSharp.Data.Sql.Common
open NUnit.Framework

let private tempSchemaPath () =
    let dir = Path.Combine(Path.GetTempPath(), "SqlProviderSchemaTests", Guid.NewGuid().ToString("N"))
    Directory.CreateDirectory dir |> ignore
    Path.Combine(dir, "schema.json")

let private customers () : (obj * IDictionary<string,obj>) array =
    [| for i in 1 .. 3 -> box i, (dict [ "Id", box i; "Name", box (sprintf "Customer %d" i) ]) |]

let private withCustomers (cache:SchemaCache) =
    cache.Tables.["main.Customers"] <- { Schema = "main"; Name = "Customers"; Type = "TABLE" }
    cache.Individuals.["main.Customers_Id"] <- customers ()
    cache

/// A schema file as versions before 1.5.34 saved it: individuals in the schema file itself
let private saveOldFormat (cache:SchemaCache) (filePath:string) =
    use ms = new MemoryStream()
    let ser = System.Runtime.Serialization.Json.DataContractJsonSerializer(typeof<SchemaCache>)
    ser.WriteObject(ms, { cache with IsOffline = true })
    File.WriteAllText(filePath, Encoding.UTF8.GetString(ms.ToArray()))

let private names (entities:(obj * IDictionary<string,obj>) array) =
    entities |> Array.map (fun (_, cols) -> string cols.["Name"])

[<Test; SqlCheck.NoSql>]
let ``the individuals file is beside the schema file`` () =
    Assert.AreEqual(Path.Combine("c:\\db", "schema.individuals.json"), SchemaCache.IndividualsFilePath(Path.Combine("c:\\db", "schema.json")))
    Assert.AreEqual("schema.individuals", SchemaCache.IndividualsFilePath "schema")

[<Test; SqlCheck.NoSql>]
let ``saving the schema leaves out the individuals of an online cache`` () =
    let path = tempSchemaPath ()
    let cache = withCustomers SchemaCache.Empty
    cache.Save path
    Assert.IsFalse(File.Exists(SchemaCache.IndividualsFilePath path))
    let loaded = SchemaCache.Load path
    Assert.IsTrue(loaded.Tables.ContainsKey "main.Customers")
    Assert.AreEqual(0, loaded.Individuals.Count)
    StringAssert.DoesNotContain("Customer 1", File.ReadAllText path)

[<Test; SqlCheck.NoSql>]
let ``saved individuals are read from their own file`` () =
    let path = tempSchemaPath ()
    let cache = withCustomers SchemaCache.Empty
    cache.Save path
    let individualsFile = cache.SaveIndividuals path
    Assert.AreEqual(SchemaCache.IndividualsFilePath path, individualsFile)
    let loaded = SchemaCache.Load path
    Assert.AreEqual(0, loaded.Individuals.Count)
    loaded.LoadIndividuals path
    CollectionAssert.AreEqual(names (customers ()), names loaded.Individuals.["main.Customers_Id"])

[<Test; SqlCheck.NoSql>]
let ``tables without individuals are not saved`` () =
    let path = tempSchemaPath ()
    let cache = withCustomers SchemaCache.Empty
    cache.Individuals.["main.Orders_Id"] <- [||]
    cache.SaveIndividuals path |> ignore
    let loaded = SchemaCache.Empty
    loaded.LoadIndividuals path
    Assert.IsFalse(loaded.Individuals.ContainsKey "main.Orders_Id")
    Assert.IsTrue(loaded.Individuals.ContainsKey "main.Customers_Id")

[<Test; SqlCheck.NoSql>]
let ``column values the file can't hold as such are saved as text`` () =
    let path = tempSchemaPath ()
    let cache = SchemaCache.Empty
    let changed = DateTimeOffset(2026, 11, 20, 12, 0, 0, TimeSpan.FromHours 2.)
    cache.Individuals.["main.Customers_Id"] <-
        [| box 1, (dict [ "Id", box 1; "Name", box "Customer 1"; "Changed", box changed
                          "Tags", box [| "a"; "b" |]; "Address", box (Net.IPAddress.Parse "10.0.0.1")
                          "Missing", box DBNull.Value; "Nothing", null ]) |]
    cache.SaveIndividuals path |> ignore
    let loaded = SchemaCache.Empty
    loaded.LoadIndividuals path
    let columns = snd loaded.Individuals.["main.Customers_Id"].[0]
    Assert.AreEqual("Customer 1", columns.["Name"])
    Assert.AreEqual(changed.ToString(), columns.["Changed"])
    CollectionAssert.AreEqual([| "a"; "b" |], columns.["Tags"] :?> obj[])
    Assert.AreEqual("10.0.0.1", columns.["Address"])
    Assert.AreEqual("", columns.["Missing"])
    Assert.IsNull(columns.["Nothing"])
    // The cache itself keeps the values as they were read
    Assert.AreEqual(box changed, (snd cache.Individuals.["main.Customers_Id"].[0]).["Changed"])

[<Test; SqlCheck.NoSql>]
let ``a schema file with individuals in it still loads them`` () =
    let path = tempSchemaPath ()
    saveOldFormat (withCustomers SchemaCache.Empty) path
    let loaded = SchemaCache.Load path
    CollectionAssert.AreEqual(names (customers ()), names loaded.Individuals.["main.Customers_Id"])

[<Test; SqlCheck.NoSql>]
let ``re-saving an old schema file moves its individuals to their own file`` () =
    let path = tempSchemaPath ()
    saveOldFormat (withCustomers SchemaCache.Empty) path
    let loaded = SchemaCache.Load path
    loaded.Save path
    StringAssert.DoesNotContain("Customer 1", File.ReadAllText path)
    let reloaded = SchemaCache.Load path
    reloaded.LoadIndividuals path
    CollectionAssert.AreEqual(names (customers ()), names reloaded.Individuals.["main.Customers_Id"])

[<Test; SqlCheck.NoSql>]
let ``the individuals file wins over individuals of an old schema file`` () =
    let path = tempSchemaPath ()
    saveOldFormat (withCustomers SchemaCache.Empty) path
    let newer = SchemaCache.Empty
    newer.Individuals.["main.Customers_Id"] <- [| box 9, (dict [ "Id", box 9; "Name", box "Newer" ]) |]
    newer.SaveIndividuals path |> ignore
    let loaded = SchemaCache.Load path
    loaded.Save path
    let reloaded = SchemaCache.Load path
    reloaded.LoadIndividuals path
    CollectionAssert.AreEqual([| "Newer" |], names reloaded.Individuals.["main.Customers_Id"])

[<Test; SqlCheck.NoSql>]
let ``merging schema files merges their individuals files`` () =
    let path1, path2, target = tempSchemaPath (), tempSchemaPath (), tempSchemaPath ()
    let cache1 = withCustomers SchemaCache.Empty
    cache1.Save path1
    cache1.SaveIndividuals path1 |> ignore
    let cache2 = SchemaCache.Empty
    cache2.Individuals.["main.Orders_Id"] <- [| box 1, (dict [ "Id", box 1; "Name", box "Order 1" ]) |]
    cache2.Save path2
    cache2.SaveIndividuals path2 |> ignore
    OfflineTools.mergeCacheFiles(path1, path2, target) |> ignore
    StringAssert.DoesNotContain("Customer 1", File.ReadAllText target)
    let merged = SchemaCache.Load target
    merged.LoadIndividuals target
    Assert.IsTrue(merged.Individuals.ContainsKey "main.Customers_Id")
    Assert.IsTrue(merged.Individuals.ContainsKey "main.Orders_Id")

// Table and column descriptions (comments) are in their own file too, for tooltips in offline mode.

let private columnDescriptions (pairs: (string * string) list) =
    ConcurrentDictionary<string,string>(pairs |> Seq.map (fun (k, v) -> KeyValuePair(k, v)))

let private describedCustomers () =
    let cache = withCustomers SchemaCache.Empty
    cache.TableDescriptions.["main.Customers"] <- "People who buy"
    cache.TableDescriptions.["main.Orders"] <- ""
    cache.ColumnDescriptions.["main.Customers"] <- columnDescriptions [ "Name", "Full name"; "Id", "" ]
    cache

[<Test; SqlCheck.NoSql>]
let ``the descriptions file is beside the schema file`` () =
    Assert.AreEqual(Path.Combine("c:\\db", "schema.descriptions.json"), SchemaCache.DescriptionsFilePath(Path.Combine("c:\\db", "schema.json")))

[<Test; SqlCheck.NoSql>]
let ``saving the schema saves the descriptions to their own file`` () =
    let path = tempSchemaPath ()
    (describedCustomers ()).Save path
    StringAssert.DoesNotContain("People who buy", File.ReadAllText path)
    let loaded = SchemaCache.Load path
    Assert.AreEqual(0, loaded.TableDescriptions.Count)
    loaded.LoadDescriptions path
    Assert.AreEqual("People who buy", loaded.TableDescriptions.["main.Customers"])
    Assert.AreEqual("", loaded.TableDescriptions.["main.Orders"])
    Assert.AreEqual("Full name", loaded.ColumnDescriptions.["main.Customers"].["Name"])
    Assert.AreEqual("", loaded.ColumnDescriptions.["main.Customers"].["Id"])

[<Test; SqlCheck.NoSql>]
let ``without descriptions there is no descriptions file`` () =
    let path = tempSchemaPath ()
    (withCustomers SchemaCache.Empty).Save path
    Assert.IsFalse(File.Exists(SchemaCache.DescriptionsFilePath path))
    Assert.AreEqual(None, SchemaCache.Empty.SaveDescriptions path)

[<Test; SqlCheck.NoSql>]
let ``saving the descriptions read so far keeps the others in the file`` () =
    let path = tempSchemaPath ()
    (describedCustomers ()).Save path
    let later = SchemaCache.Empty
    later.TableDescriptions.["main.Customers"] <- "Changed"
    later.ColumnDescriptions.["main.Customers"] <- columnDescriptions [ "Phone", "Phone number" ]
    later.SaveDescriptions path |> ignore
    let loaded = SchemaCache.Empty
    loaded.LoadDescriptions path
    Assert.AreEqual("Changed", loaded.TableDescriptions.["main.Customers"])
    Assert.AreEqual("", loaded.TableDescriptions.["main.Orders"])
    Assert.AreEqual("Full name", loaded.ColumnDescriptions.["main.Customers"].["Name"])
    Assert.AreEqual("Phone number", loaded.ColumnDescriptions.["main.Customers"].["Phone"])

[<Test; SqlCheck.NoSql>]
let ``re-saving an offline schema keeps its descriptions`` () =
    let path = tempSchemaPath ()
    (describedCustomers ()).Save path
    // Offline, descriptions are read only for the first tooltip: a save before that must not lose them
    (SchemaCache.Load path).Save path
    let loaded = SchemaCache.Load path
    loaded.LoadDescriptions path
    Assert.AreEqual("People who buy", loaded.TableDescriptions.["main.Customers"])

[<Test; SqlCheck.NoSql>]
let ``merging schema files merges their descriptions files`` () =
    let path1, path2, target = tempSchemaPath (), tempSchemaPath (), tempSchemaPath ()
    let cache1 = SchemaCache.Empty
    cache1.TableDescriptions.["main.Customers"] <- "People who buy"
    cache1.ColumnDescriptions.["main.Customers"] <- columnDescriptions [ "Name", "Full name" ]
    cache1.Save path1
    let cache2 = SchemaCache.Empty
    cache2.TableDescriptions.["main.Orders"] <- "What they buy"
    cache2.ColumnDescriptions.["main.Customers"] <- columnDescriptions [ "Phone", "Phone number" ]
    cache2.Save path2
    OfflineTools.mergeCacheFiles(path1, path2, target) |> ignore
    let merged = SchemaCache.Load target
    merged.LoadDescriptions target
    Assert.AreEqual("People who buy", merged.TableDescriptions.["main.Customers"])
    Assert.AreEqual("What they buy", merged.TableDescriptions.["main.Orders"])
    Assert.AreEqual("Full name", merged.ColumnDescriptions.["main.Customers"].["Name"])
    Assert.AreEqual("Phone number", merged.ColumnDescriptions.["main.Customers"].["Phone"])

