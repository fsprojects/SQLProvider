@echo off
rem change the target via -t, e.g.:
rem build -t PackNuget
cls
dotnet tool restore
dotnet paket restore
dotnet fake run build.fsx %*
