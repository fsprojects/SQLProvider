/// Implements functions that already exist in netstandard.
namespace FSharp.Data.Sql.Ssdt

#if !NETSTANDARD
module Option =
    let defaultValue<'T> (value: 'T) (option: 'T option) =
        match option with
        | Some optValue -> optValue
        | None -> value

    let ofObj<'T when 'T : null> (value: 'T) =
        match value with
        | null -> None
        | _ -> Some value

module Array =
    let last<'T> (arr: 'T[]) =
        (Array.rev arr).[0]

module List =
    let tryHead<'T> (lst: 'T list) =
        match lst with
        | [] -> None
        | head::tail -> Some head

    let tryLast<'T> (lst: 'T list) =
        match List.rev lst with
        | [] -> None
        | head::tail -> Some head
#endif
