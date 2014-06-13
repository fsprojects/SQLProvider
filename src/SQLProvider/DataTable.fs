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
        let maxLength, rows = 
            dt
            |> map (fun r -> r.ItemArray)
            |> List.fold (fun (maxLen, rows) row ->
                            let values = row |> Array.map (fun x -> x.ToString()) |> List.ofArray
                            let rowMax = values |> List.maxBy (fun x -> x.Length)
                            (max maxLen rowMax.Length), (values :: rows) 
                        ) (0,[])
        for col in dt.Columns do
            printf "%*s  " (maxLength + 2) col.ColumnName
        printfn ""
        for row in rows |> List.rev do
            for (index,value) in row |> List.mapi (fun i x -> i,x) do
                printf "%*s  " (maxLength + 2) value
            printfn ""

