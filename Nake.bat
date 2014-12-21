@ECHO OFF
SET DIR=%~dp0%
%DIR%\Packages\Nake.2.3.0\tools\net45\Nake.exe -f %DIR%\Nake.csx -d %DIR% %*