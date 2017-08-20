#r "System.Xml"
#r "System.Xml.Linq"

using System.Linq;
using System.Net;
using System.Xml.Linq;
using System.Diagnostics;
using System.Text.RegularExpressions;

using static Nake.App;
using static Nake.Env;
using static Nake.FS;
using static Nake.Log;
using static Nake.Run;

const string Project = "Streamstone";
const string RootPath = "%NakeScriptDirectory%";
const string OutputPath = RootPath + @"\Output";

var AppVeyor = Var["APPVEYOR"] == "True";

/// Builds sources in Debug mode
[Task] void Default() => Build();

/// Builds sources using specified configuration and output path
[Step] void Build(string config = "Debug") =>    
    Exec("dotnet", "build {Project}.sln /p:Configuration={config}");

/// Runs unit tests 
[Step] void Test()
{  
	Exec("dotnet", "test Source/Streamstone.Tests/Streamstone.Tests.csproj --configuration Debug -l:trx;LogFileName=nunit-test-results.trx --results-directory \"{OutputPath}\"");
    
    var results = @"{OutputPath}\nunit-test-results.trx";
    if (AppVeyor)
        new WebClient().UploadFile("https://ci.appveyor.com/api/testresults/mstest/%APPVEYOR_JOB_ID%", results);
}

/// Builds official NuGet package 
[Step] void Package()
{
    Test();

    Build("Release");

    Cmd(@"Tools\Nuget.exe pack Build\{Project}.nuspec -Version {Version()} " +
        @"-OutputDirectory {OutputPath} -BasePath {RootPath}\Source\Streamstone\bin\Release -NoPackageAnalysis");
}

/// Publishes package to NuGet gallery
[Step] void Publish() => Cmd(@"Tools\Nuget.exe push {OutputPath}\{Project}.{Version()}.nupkg %NuGetApiKey% -Source https://nuget.org/");

string Version() 
{ 
    var version = File
        .ReadAllLines(@"{RootPath}\Source\Streamstone.Version.cs")
        .First(x => x.StartsWith("[assembly: AssemblyVersion("));

    return new Regex(@"AssemblyVersion\(\""([^\""]*)\""\)").Match(version).Groups[1].Value;
}