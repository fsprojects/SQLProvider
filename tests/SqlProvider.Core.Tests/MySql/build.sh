# May need sudo on Linux: sh ./build.sh
mono ../../../.paket/paket.bootstrapper.exe
mono ../../../.paket/paket.exe restore
rm -r -f obj
rm -r -f bin
dotnet restore
dotnet build
dotnet run