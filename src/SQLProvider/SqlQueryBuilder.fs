namespace FSharp.Data.Sql

open System.Linq
open System.Collections.Generic
open Microsoft.FSharp.Linq
open Microsoft.FSharp.Quotations

/// The type used to support queries against a database. Use 'sqlQuery { ... }' to use the query syntax.
type SqlQueryBuilder() =

    let qb = QueryBuilder()

    // The user should not have any need to access the underlying builder, 
    // but it needs to be public so we can inline the numeric operators (sumBy etc.)

    /// <summary>
    /// The LINQ query builder underlying this SQL query builder.
    /// </summary>    
    member __.QueryBuilder = qb
        
    /// <summary>
    /// A method used to support queries against a database.  Inputs to queries are implicitly wrapped by a call to one of the overloads of this method.
    /// </summary>
    member inline this.Source (source : IQueryable<_>) = this.QueryBuilder.Source source

    /// <summary>
    /// A method used to support queries against a database.  Inputs to queries are implicitly wrapped by a call to one of the overloads of this method.
    /// </summary>
    member inline this.Source (source : IEnumerable<_>) = this.QueryBuilder.Source source

    /// <summary>
    /// A method used to support queries against a database.  Projects each element of a sequence to another sequence and combines the resulting sequences into one sequence.
    /// </summary>
    member inline this.For (source, body) = this.QueryBuilder.For (source, body)

    /// <summary>
    /// A method used to support queries against a database.  Returns an empty sequence that has the specified type argument.
    /// </summary>
    member inline this.Zero () = this.QueryBuilder.Zero ()

    /// <summary>
    /// A method used to support queries against a database.  Returns a sequence of length one that contains the specified value.
    /// </summary>
    member inline this.Yield (value) = this.QueryBuilder.Yield (value)

    /// <summary>
    /// A method used to support queries against a database.  Returns a sequence that contains the specified values.
    /// </summary>
    member inline this.YieldFrom (computation) = this.QueryBuilder.YieldFrom (computation)

    /// <summary>
    /// A method used to support queries against a database.  Indicates that the query should be passed as a quotation to the Run method.
    /// </summary>
    member inline this.Quote (query) = this.QueryBuilder.Quote (query)

    /// <summary>
    /// A method used to support queries against a database.  Runs the given quotation as a query using LINQ rules.
    /// </summary>
    member inline internal this.RunQueryAsValue (query: Expr<'T>) : 'T = this.QueryBuilder.Run (query)
    
    /// <summary>
    /// A method used to support queries against a database.  Runs the given quotation as a query using LINQ IQueryable rules.
    /// </summary>
    member inline internal this.RunQueryAsEnumerable (query: Expr<QuerySource<_, System.Collections.IEnumerable>>) : IEnumerable<'T> = this.QueryBuilder.Run (query)

    /// <summary>
    /// A method used to support queries against a database.  Runs the given quotation as a query using LINQ IEnumerable rules.
    /// </summary>
    member inline internal this.RunQueryAsQueryable (expression: Expr<QuerySource<'T, IQueryable>>) : IQueryable<'T> = this.QueryBuilder.Run(expression)

    /// <summary>
    /// A method used to support queries against a database.  Runs the given quotation as a query using LINQ IEnumerable rules.
    /// </summary>
    member inline this.Run (expression) = this.RunQueryAsQueryable (expression)

    /// <summary>A query operator that determines whether the selected elements contains a specified element.
    /// </summary>
    [<CustomOperation("contains")>] 
    member inline this.Contains (source, key) = this.QueryBuilder.Contains (source, key)

    /// <summary>A query operator that returns the number of selected elements.
    /// </summary>
    [<CustomOperation("count")>] 
    member inline this.Count (source) = this.QueryBuilder.Count (source)

    ///// <summary>A query operator that selects the last element of those selected so far.
    ///// </summary>
    //[<CustomOperation("last")>] 
    //member inline this.Last (source) = qb.Last (source)

    ///// <summary>A query operator that selects the last element of those selected so far, or a default value if no element is found.
    ///// </summary>
    //[<CustomOperation("lastOrDefault")>] 
    //member inline this.LastOrDefault (source) = qb.LastOrDefault (source)

    /// <summary>A query operator that selects the single, specific element selected so far
    /// </summary>
    [<CustomOperation("exactlyOne")>] 
    member inline this.ExactlyOne (source) = this.QueryBuilder.ExactlyOne (source)

    /// <summary>A query operator that selects the single, specific element of those selected so far, or a default value if that element is not found.
    /// </summary>
    [<CustomOperation("exactlyOneOrDefault")>] 
    member inline this.ExactlyOneOrDefault (source) = this.QueryBuilder.ExactlyOneOrDefault (source)

    /// <summary>A query operator that selects the first element of those selected so far, or a default value if the sequence contains no elements.
    /// </summary>
    [<CustomOperation("headOrDefault")>] 
    member inline this.HeadOrDefault (source) = this.QueryBuilder.HeadOrDefault (source)

    /// <summary>A query operator that projects each of the elements selected so far.
    /// </summary>
    [<CustomOperation("select",AllowIntoPattern=true)>] 
    member inline this.Select (source, [<ProjectionParameter>] projection) = this.QueryBuilder.Select (source, projection)

    /// <summary>A query operator that selects those elements based on a specified predicate. 
    /// </summary>
    [<CustomOperation("where",MaintainsVariableSpace=true,AllowIntoPattern=true)>] 
    member inline this.Where (source, [<ProjectionParameter>] predicate) = this.QueryBuilder.Where (source, predicate)

    /// <summary>A query operator that selects a value for each element selected so far and returns the minimum resulting value. 
    /// </summary>
    [<CustomOperation("minBy")>] 
    member inline this.MinBy (source, [<ProjectionParameter>] valueSelector) = this.QueryBuilder.MinBy (source, valueSelector)

    /// <summary>A query operator that selects a value for each element selected so far and returns the maximum resulting value. 
    /// </summary>
    [<CustomOperation("maxBy")>] 
    member inline this.MaxBy (source, [<ProjectionParameter>] valueSelector) = this.QueryBuilder.MaxBy (source, valueSelector)

    /// <summary>A query operator that groups the elements selected so far according to a specified key selector.
    /// </summary>
    [<CustomOperation("groupBy",AllowIntoPattern=true)>] 
    member inline this.GroupBy (source, [<ProjectionParameter>] keySelector) = this.QueryBuilder.GroupBy (source, keySelector)

    /// <summary>A query operator that sorts the elements selected so far in ascending order by the given sorting key.
    /// </summary>
    [<CustomOperation("sortBy",MaintainsVariableSpace=true,AllowIntoPattern=true)>] 
    member inline this.SortBy (source, [<ProjectionParameter>] keySelector) = this.QueryBuilder.SortBy (source, keySelector)

    /// <summary>A query operator that sorts the elements selected so far in descending order by the given sorting key.
    /// </summary>
    [<CustomOperation("sortByDescending",MaintainsVariableSpace=true,AllowIntoPattern=true)>] 
    member inline this.SortByDescending (source, [<ProjectionParameter>] keySelector) = this.QueryBuilder.SortByDescending (source, keySelector)

    /// <summary>A query operator that performs a subsequent ordering of the elements selected so far in ascending order by the given sorting key.
    /// This operator may only be used immediately after a 'sortBy', 'sortByDescending', 'thenBy' or 'thenByDescending', or their nullable variants.
    /// </summary>
    [<CustomOperation("thenBy",MaintainsVariableSpace=true,AllowIntoPattern=true)>] 
    member inline this.ThenBy (source, [<ProjectionParameter>] keySelector) = this.QueryBuilder.ThenBy (source, keySelector)

    /// <summary>A query operator that performs a subsequent ordering of the elements selected so far in descending order by the given sorting key.
    /// This operator may only be used immediately after a 'sortBy', 'sortByDescending', 'thenBy' or 'thenByDescending', or their nullable variants.
    /// </summary>
    [<CustomOperation("thenByDescending",MaintainsVariableSpace=true,AllowIntoPattern=true)>] 
    member inline this.ThenByDescending (source, [<ProjectionParameter>] keySelector) = this.QueryBuilder.ThenByDescending (source, keySelector)

    ///// <summary>A query operator that selects a value for each element selected so far and groups the elements by the given key.
    ///// </summary>
    //[<CustomOperation("groupValBy",AllowIntoPattern=true)>] 
    //member inline this.GroupValBy<'T,'Key,'Value,'Q when 'Key : equality> (source, [<ProjectionParameter>] resultSelector, [<ProjectionParameter>] keySelector) = qb.GroupValBy<'T,'Key,'Value,'Q> (source, resultSelector, keySelector)

    /// <summary>A query operator that correlates two sets of selected values based on matching keys. 
    /// Normal usage is 'join y in elements2 on (key1 (source) = key2)'. (source) 
    /// </summary>
    [<CustomOperation("join",IsLikeJoin=true,JoinConditionWord="on")>] 
    member inline this.Join (outerSource, innerSource, outerKeySelector, innerKeySelector, resultSelector) = this.QueryBuilder.Join (outerSource, innerSource, outerKeySelector, innerKeySelector, resultSelector)

    ///// <summary>A query operator that correlates two sets of selected values based on matching keys and groups the results. 
    ///// Normal usage is 'groupJoin y in elements2 on (key1 (source) = key2) (source) into group'. 
    ///// </summary>
    //[<CustomOperation("groupJoin",IsLikeGroupJoin=true,JoinConditionWord="on")>] 
    //member inline this.GroupJoin (outerSource, innerSource, outerKeySelector, innerKeySelector, resultSelector) = qb.GroupJoin (outerSource, innerSource, outerKeySelector, innerKeySelector, resultSelector)

    ///// <summary>A query operator that correlates two sets of selected values based on matching keys and groups the results.
    ///// If any group is empty, a group with a single default value is used instead. 
    ///// Normal usage is 'leftOuterJoin y in elements2 on (key1 (source) = key2) (source) into group'. 
    ///// </summary>
    //[<CustomOperation("leftOuterJoin",IsLikeGroupJoin=true,JoinConditionWord="on")>] 
    //member inline this.LeftOuterJoin (outerSource, innerSource, outerKeySelector, innerKeySelector, resultSelector) = qb.LeftOuterJoin (outerSource, innerSource, outerKeySelector, innerKeySelector, resultSelector)

    /// <summary>A query operator that selects a nullable value for each element selected so far and returns the sum of these values. 
    /// If any nullable does not have a value, it is ignored.
    /// </summary>
    [<CustomOperation("sumByNullable")>] 
    member inline this.SumByNullable (source, [<ProjectionParameter>] valueSelector) = this.QueryBuilder.SumByNullable (source, valueSelector)

    /// <summary>A query operator that selects a nullable value for each element selected so far and returns the minimum of these values. 
    /// If any nullable does not have a value, it is ignored.
    /// </summary>
    [<CustomOperation("minByNullable")>] 
    member inline this.MinByNullable (source, [<ProjectionParameter>] valueSelector) = this.QueryBuilder.MinByNullable (source, valueSelector)

    /// <summary>A query operator that selects a nullable value for each element selected so far and returns the maximum of these values. 
    /// If any nullable does not have a value, it is ignored.
    /// </summary>
    [<CustomOperation("maxByNullable")>] 
    member inline this.MaxByNullable (source, [<ProjectionParameter>] valueSelector) = this.QueryBuilder.MaxByNullable (source, valueSelector)

    /// <summary>A query operator that selects a nullable value for each element selected so far and returns the average of these values. 
    /// If any nullable does not have a value, it is ignored.
    /// </summary>
    [<CustomOperation("averageByNullable")>] 
    member inline this.AverageByNullable (source, [<ProjectionParameter>] projection) = this.QueryBuilder.AverageByNullable (source, projection)


    /// <summary>A query operator that selects a value for each element selected so far and returns the average of these values. 
    /// </summary>
    [<CustomOperation("averageBy")>] 
    member inline this.AverageBy (source, [<ProjectionParameter>] projection) = this.QueryBuilder.AverageBy (source, projection)


    /// <summary>A query operator that selects distinct elements from the elements selected so far. 
    /// </summary>
    [<CustomOperation("distinct",MaintainsVariableSpace=true,AllowIntoPattern=true)>] 
    member inline this.Distinct (source) = this.QueryBuilder.Distinct (source)

    /// <summary>A query operator that determines whether any element selected so far satisfies a condition.
    /// </summary>
    [<CustomOperation("exists")>] 
    member inline this.Exists (source, [<ProjectionParameter>] predicate) = this.QueryBuilder.Exists (source, predicate)

    /// <summary>A query operator that selects the first element selected so far that satisfies a specified condition.
    /// </summary>
    [<CustomOperation("find")>] 
    member inline this.Find (source, [<ProjectionParameter>] predicate) = this.QueryBuilder.Find (source, predicate)


    /// <summary>A query operator that determines whether all elements selected so far satisfies a condition.
    /// </summary>
    [<CustomOperation("all")>] 
    member inline this.All (source, [<ProjectionParameter>] predicate) = this.QueryBuilder.All (source, predicate)

    /// <summary>A query operator that selects the first element from those selected so far.
    /// </summary>
    [<CustomOperation("head")>] 
    member inline this.Head (source) = this.QueryBuilder.Head (source)

    /// <summary>A query operator that selects the element at a specified index amongst those selected so far.
    /// </summary>
    [<CustomOperation("nth")>] 
    member inline this.Nth (source, index) = this.QueryBuilder.Nth (source, index)

    /// <summary>A query operator that bypasses a specified number of the elements selected so far and selects the remaining elements.
    /// </summary>
    [<CustomOperation("skip",MaintainsVariableSpace=true,AllowIntoPattern=true)>] 
    member inline this.Skip (source, count) = this.QueryBuilder.Skip (source, count)

    ///// <summary>A query operator that bypasses elements in a sequence as long as a specified condition is true and then selects the remaining elements.
    ///// </summary>
    //[<CustomOperation("skipWhile",MaintainsVariableSpace=true,AllowIntoPattern=true)>] 
    //member inline this.SkipWhile (source, predicate) = qb.SkipWhile (source, predicate)

    /// <summary>A query operator that selects a value for each element selected so far and returns the sum of these values. 
    /// </summary>
    [<CustomOperation("sumBy")>] 
    member inline this.SumBy (source, [<ProjectionParameter>] projection) = this.QueryBuilder.SumBy (source, projection)

    /// <summary>A query operator that selects a specified number of contiguous elements from those selected so far.
    /// </summary>
    [<CustomOperation("take",MaintainsVariableSpace=true,AllowIntoPattern=true)>] 
    member inline this.Take (source, count) = this.QueryBuilder.Take (source, count)

    ///// <summary>A query operator that selects elements from a sequence as long as a specified condition is true, and then skips the remaining elements.
    ///// </summary>
    //[<CustomOperation("takeWhile",MaintainsVariableSpace=true,AllowIntoPattern=true)>] 
    //member inline this.TakeWhile (source, predicate) = qb.TakeWhile (source, predicate)

    /// <summary>A query operator that sorts the elements selected so far in ascending order by the given nullable sorting key.
    /// </summary>
    [<CustomOperation("sortByNullable",MaintainsVariableSpace=true,AllowIntoPattern=true)>] 
    member inline this.SortByNullable (source, [<ProjectionParameter>] keySelector) = this.QueryBuilder.SortByNullable (source, keySelector)

    /// <summary>A query operator that sorts the elements selected so far in descending order by the given nullable sorting key.
    /// </summary>
    [<CustomOperation("sortByNullableDescending",MaintainsVariableSpace=true,AllowIntoPattern=true)>] 
    member inline this.SortByNullableDescending (source, [<ProjectionParameter>] keySelector) = this.QueryBuilder.SortByNullableDescending (source, keySelector)

    /// <summary>A query operator that performs a subsequent ordering of the elements selected so far in ascending order by the given nullable sorting key.
    /// This operator may only be used immediately after a 'sortBy', 'sortByDescending', 'thenBy' or 'thenByDescending', or their nullable variants.
    /// </summary>
    [<CustomOperation("thenByNullable",MaintainsVariableSpace=true,AllowIntoPattern=true)>] 
    member inline this.ThenByNullable (source, [<ProjectionParameter>] keySelector) = this.QueryBuilder.ThenByNullable (source, keySelector)

    /// <summary>A query operator that performs a subsequent ordering of the elements selected so far in descending order by the given nullable sorting key.
    /// This operator may only be used immediately after a 'sortBy', 'sortByDescending', 'thenBy' or 'thenByDescending', or their nullable variants.
    /// </summary>
    [<CustomOperation("thenByNullableDescending",MaintainsVariableSpace=true,AllowIntoPattern=true)>] 
    member inline this.ThenByNullableDescending (source, [<ProjectionParameter>] keySelector) = this.QueryBuilder.ThenByNullableDescending (source, keySelector)
      
    //// WIP
    
    ///// <summary>
    ///// A query operator that selects elements from another table based on a SQL foreign key relationship'. 
    ///// </summary>
    //[<CustomOperation("naturalJoin")>] 
    //member inline this.NaturalJoin (source:QuerySource<'T,'Q>, body: 'T -> QuerySource<'Result,'Q2>) : QuerySource<'Result,'Q> = 
    //  // QueryBuilder.For is defined as :
    //  // QuerySource (Seq.collect (fun x -> (body x).Source) source.Source)
    
    //  // QueryBuilder.Join is defined as :
    //  // QuerySource (System.Linq.Enumerable.Join(outerSource.Source, innerSource.Source, Func<_,_>(outerKeySelector), Func<_,_>(innerKeySelector), Func<_,_,_>(elementSelector)))

    //  this.QueryBuilder.For(QuerySource (!! (source)), body)

[<AutoOpen>]
module ExtraTopLevelOperators = 
    /// Builds a SQL query using query syntax and operators
    let sqlQuery = SqlQueryBuilder()
  
    [<AutoOpen>]
    module LowPriority = 
        type SqlQueryBuilder with

            /// <summary>
            /// A method used to support the F# query syntax.  Runs the given quotation as a query using LINQ rules.
            /// </summary>
            [<CompiledName("RunQueryAsValue")>]
            member inline this.Run query = this.RunQueryAsValue query

    [<AutoOpen>]
    module HighPriority = 
        type SqlQueryBuilder with

            /// <summary>
            /// A method used to support the F# query syntax.  Runs the given quotation as a query using LINQ IEnumerable rules.
            /// </summary>
            [<CompiledName("RunQueryAsEnumerable")>]
            member inline this.Run query = this.RunQueryAsEnumerable query
