(**
# Programmability
*)

type AdventureWorks = SqlDataProvider<Common.DatabaseProviderTypes.MSSQLSERVER, connStr, ResolutionPath = resolutionFolder>
let ctx = AdventureWorks.GetDataContext()

(**
Execute a function in the Adventure Works database
*)

ctx.Functions.UfnGetSalesOrderStatusText.Invoke(0uy)

(**
Execute a stored procedure in the Adventure Works database
*)

ctx.Procedures.UspLogError.Invoke(1)
