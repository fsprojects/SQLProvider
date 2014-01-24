namespace FSharp.Data.Sql

open System.Collections.Generic
open System.Data

module DataTable =
     
    let map f (dt:DataTable) = 
        [
            for row in dt.Rows do
                yield row |>  f
        ]
    
    let iter f (dt:DataTable) = 
        for row in dt.Rows do
            f row.ItemArray

    let groupBy f (dt:DataTable) = 
        map f dt
        |> Seq.groupBy (fst) 
        |> Seq.map (fun (k, v) -> k, Seq.map snd v)
    
    let cache (cache:IDictionary<string,'a>) f (dt:DataTable) = 
        for row in dt.Rows do
            match f row with
            | Some(key,item) -> cache.Add(key,item)
            | None -> ()
        cache.Values |> Seq.map id |> Seq.toList
    
    let choose f (dt:DataTable) =
        [
            for row in dt.Rows do
                match row |>  f with
                | Some(a) -> yield a
                | None -> ()
        ]
    
    let printDataTable (dt:System.Data.DataTable) = 
        for col in dt.Columns do
            printf "%s " col.ColumnName
        printfn ""
        for row in dt.Rows do
            printfn "%A" row.ItemArray

