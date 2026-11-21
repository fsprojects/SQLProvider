module ColumnValueTests

// A driver may give a date or a time in another type than the column is mapped to:
// DuckDB gives a DATE as DateOnly and a TIME as TimeOnly on .NET 6 and later, and a TIME as its own DuckDBTimeOnly before that.

open System
open FSharp.Data.Sql.Common
open NUnit.Framework

/// Stands for a driver's own time type, such as DuckDBTimeOnly
[<Struct>]
type DriverTimeOnly(ticks: int64) =
    member __.Ticks = ticks

type DayAndTime =
    { Day: DateTime
      Time: TimeSpan
      MaybeDay: DateTime option
      MaybeTime: TimeSpan voption }

let private entityOf (values: (string * obj) list) =
    let entity = SqlEntity(Unchecked.defaultof<_>, "main.events", Map.empty, values.Length)
    entity.SetData values
    entity

let private day = DateTime(2024, 2, 29)
let private time = TimeSpan(0, 13, 45, 10, 123)

[<Test; SqlCheck.NoSql>]
let ``a time in a driver's own type is read as a TimeSpan`` () =
    let row = entityOf [ "time", box (DriverTimeOnly time.Ticks) ] :> IColumnHolder
    Assert.AreEqual(time, row.GetColumn<TimeSpan> "time")
    Assert.AreEqual(Some time, row.GetColumnOption<TimeSpan> "time")
    Assert.AreEqual(ValueSome time, row.GetColumnValueOption<TimeSpan> "time")

[<Test; SqlCheck.NoSql>]
let ``a date and a time in their mapped types are read as they are`` () =
    let row = entityOf [ "day", box day; "time", box time; "nothing", null ] :> IColumnHolder
    Assert.AreEqual(day, row.GetColumn<DateTime> "day")
    Assert.AreEqual(time, row.GetColumn<TimeSpan> "time")
    Assert.AreEqual(Some day, row.GetColumnOption<DateTime> "day")
    Assert.AreEqual(ValueSome time, row.GetColumnValueOption<TimeSpan> "time")
    Assert.AreEqual(None, row.GetColumnOption<DateTime> "nothing")
    Assert.AreEqual(DateTime.MinValue, row.GetColumn<DateTime> "nothing")

#if NET6_0_OR_GREATER
[<Test; SqlCheck.NoSql>]
let ``a DateOnly is read as a DateTime and a TimeOnly as a TimeSpan`` () =
    let row = entityOf [ "day", box (DateOnly.FromDateTime day); "time", box (TimeOnly.FromTimeSpan time) ] :> IColumnHolder
    Assert.AreEqual(day, row.GetColumn<DateTime> "day")
    Assert.AreEqual(time, row.GetColumn<TimeSpan> "time")
    Assert.AreEqual(Some day, row.GetColumnOption<DateTime> "day")
    Assert.AreEqual(Some time, row.GetColumnOption<TimeSpan> "time")
    Assert.AreEqual(ValueSome day, row.GetColumnValueOption<DateTime> "day")
    Assert.AreEqual(ValueSome time, row.GetColumnValueOption<TimeSpan> "time")
    // As it is, when that is what was asked for
    Assert.AreEqual(DateOnly.FromDateTime day, row.GetColumn<DateOnly> "day")
    Assert.AreEqual(box (TimeOnly.FromTimeSpan time), row.GetColumn<obj> "time")

[<Test; SqlCheck.NoSql>]
let ``a DateOnly and a TimeOnly map to DateTime and TimeSpan fields`` () =
    let row =
        entityOf [ "day", box (DateOnly.FromDateTime day); "time", box (TimeOnly.FromTimeSpan time)
                   "maybe_day", box (DateOnly.FromDateTime day); "maybe_time", box (TimeOnly.FromTimeSpan time) ]
    Assert.AreEqual({ Day = day; Time = time; MaybeDay = Some day; MaybeTime = ValueSome time }, row.MapTo<DayAndTime>())
#endif
