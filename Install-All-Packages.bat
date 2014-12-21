@ECHO OFF
Tools\NuGet.exe install Nake -Version 2.3.0 -o Packages
CALL Nake.bat install