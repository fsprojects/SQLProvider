#!/bin/bash
if test "$OS" = "Windows_NT"
then
  cmd /C build.cmd
else
  which dotnet > /dev/null || { echo "ERROR: 'dotnet' not found. Please ensure you have installed .NETv6 or newer" >&2 && exit 1; }
  dotnet tool restore
  dotnet paket restore
  #dotnet fsi build.fsx -t Build $@
  dotnet fake run build.fsx -t Build $@
fi