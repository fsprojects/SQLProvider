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

    let toList(dt:DataTable) = map id dt

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
    
    let mapChoose (f:DataRow -> 'a option) (dt:DataTable) = 
        if dt <> null
        then
            [
                for row in dt.Rows do
                    match f row with
                    | Some(a) -> yield a
                    | None -> ()
            ]
        else []

    let choose (f : DataRow -> DataRow option) (dt:DataTable) =
        let copy = dt.Clone()
        copy.Rows.Clear()
        for row in dt.Rows do
            match row |>  f with
            | Some(a) -> copy.Rows.Add(a.ItemArray) |> ignore
            | None -> ()
        copy

    let filter f (dt:DataTable) =
        choose (fun r -> 
            if f r 
            then Some r
            else None
        ) dt

    let headers (dt:System.Data.DataTable) = 
        [
            for col in dt.Columns do
                yield col.ColumnName
        ]
    
    let printDataTable (dt:System.Data.DataTable) = 
        if dt <> null
        then
            let widths = new Dictionary<int, int>()

            let computeMaxWidth indx length = 
                let len =
                    match widths.TryGetValue(indx) with
                    | true, len -> max len length
                    | false, _ -> length
                widths.[indx] <- len
            
            let i = ref 0
            for col in dt.Columns do
                computeMaxWidth !i col.ColumnName.Length
                incr i

            let rows = 
                dt
                |> map (fun r -> r.ItemArray)
                |> List.fold (fun rows row ->
                                let values = row |> Array.map (fun x -> x.ToString().Trim()) |> List.ofArray
                                values |> List.iteri (fun i x -> computeMaxWidth i x.Length)
                                values :: rows
                            ) []
            
            i := 0
            for col in dt.Columns do
                printf "%*s  " (widths.[!i] + 2) col.ColumnName
                incr i            
            printfn ""
            for row in rows |> List.rev do
                for (index,value) in row |> List.mapi (fun i x -> i,x) do
                    printf "%*s  " (widths.[index] + 2) value
                printfn ""

