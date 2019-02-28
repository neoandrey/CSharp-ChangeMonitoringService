C:\Windows\Microsoft.NET\Framework\v4.0.30319\csc.exe /r:"lib\System.Data.SQLite.dll","lib\Newtonsoft.Json.dll"  /t:library  /out:lib\resources.dll src\ConnectionCipher.cs  src\ConnectionProperty.cs src\DataCheckConfig.cs src\DataUtilLibrary.cs
C:\Windows\Microsoft.NET\Framework\v4.0.30319\csc.exe /r:"lib\System.Data.SQLite.dll","lib\Newtonsoft.Json.dll","lib\MongoDB.Bson.dll","lib\MongoDB.Driver.Core.dll","lib\MongoDB.Driver.dll","lib\MongoDB.Driver.Legacy.dll","lib\MySql.Data.dll","lib\resources.dll" /out:bin\SqlTableChangeMonitor.exe src\DataCheckProcess.cs
xcopy /e /f /y %cd%\lib\report_resources.dll %cd%\bin\
pause
if not exist .git\ (
   git init 
   git config  user.email  "neoandrey@yahoo.com"
   git config  user.name   "neoandrey@yahoo.com"
   echo "log" >>.gitignore
   echo "bin" >>.gitignore
   git add . 
   git commit -m "Initialize Application " 
)

