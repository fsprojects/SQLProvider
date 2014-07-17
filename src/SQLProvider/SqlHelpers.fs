namespace FSharp.Data.Sql

open System
open System.Data

module SqlHelpers =
    
    let dataReaderToArray (reader:IDataReader) = 
        [| 
            while reader.Read() do
               yield [|      
                    for i = 0 to reader.FieldCount - 1 do 
                        match reader.GetValue(i) with
                        | null | :? DBNull ->  yield (reader.GetName(i),null)
                        | value -> yield (reader.GetName(i),value)
               |]
        |]

