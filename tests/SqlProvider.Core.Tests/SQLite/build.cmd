..\..\..\.paket\paket.bootstrapper.exe
..\..\..\.paket\paket.exe restore
rd /s /q obj
rd /s /q bin
dotnet restore
dotnet build
dotnet run
