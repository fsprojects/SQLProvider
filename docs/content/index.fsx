(*** hide ***)
#I "../../bin"

(**
SQLProvider
===========

A general SQL database type provider, supporting LINQ queries, schema exploration, individuals and much more besides.
<div class="row">
  <div class="span1"></div>
  <div class="span6">
    <div class="well well-small" id="nuget">
      The library can be <a href="https://nuget.org/packages/SQLProvider">installed from NuGet</a>:
      <pre>PM> Install-Package SQLProvider -prerelease</pre>
    </div>
  </div>
  <div class="span1"></div>
</div>

Example
-------

This example demonstrates the use of the type provider:

*)
// reference the type provider dll
#r "FSharp.Data.SQLProvider.dll"
open System
open System.Linq
open FSharp.Data.Sql


(**

Samples & documentation
-----------------------

The library comes with comprehensible documentation.

 * [API Reference](reference/index.html) contains automatically generated documentation for all types, modules
   and functions in the library. This includes additional brief samples on using most of the
   functions.
 
Contributing and copyright
--------------------------

The project is hosted on [GitHub][gh] where you can [report issues][issues], fork 
the project and submit pull requests. If you're adding new public API, please also 
consider adding [samples][content] that can be turned into a documentation. You might
also want to read [library design notes][readme] to understand how it works.

The library is available under Public Domain license, which allows modification and 
redistribution for both commercial and non-commercial purposes. For more information see the 
[License file][license] in the GitHub repository. 

  [content]: https://github.com/pezipink/SQLProvider/tree/master/docs/content
  [gh]: https://github.com/pezipink/SQLProvider
  [issues]: https://github.com/pezipink/SQLProvider/issues
  [readme]: https://github.com/pezipink/SQLProvider/blob/master/README.md
  [license]: https://github.com/pezipink/SQLProvider/blob/master/LICENSE.md
*)
