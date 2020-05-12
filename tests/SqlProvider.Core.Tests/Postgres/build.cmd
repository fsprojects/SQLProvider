..\..\..\.paket\paket.bootstrapper.exe
..\..\..\.paket\paket.exe restore
rd /s /q obj
rd /s /q bin
dotnet restore -f
dotnet build
dotnet run
