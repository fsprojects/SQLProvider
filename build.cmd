@echo off
rem change the target via -t, e.g.:
rem build -t PackNuget
cls
dotnet tool restore
dotnet paket restore
set FAKE_SDK_RESOLVER_CUSTOM_DOTNET_VERSION=8.0
rem Build with Fake or FSI:
rem dotnet fake run build.fsx %*
dotnet fsi build.fsx %*
