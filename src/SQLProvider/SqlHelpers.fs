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

    let dbUnbox<'a> (v:obj) : 'a = 
        if Convert.IsDBNull(v) then Unchecked.defaultof<'a> else unbox v
    
    let dbUnboxWithDefault<'a> def (v:obj) : 'a = 
        if Convert.IsDBNull(v) then def else unbox v

    let connect (con:IDbConnection) f =
        if con.State <> ConnectionState.Open then con.Open()
        let result = f con
        con.Close(); result

