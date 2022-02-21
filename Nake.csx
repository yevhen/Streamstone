#r "nuget: Nake.Meta, 3.0.0"
#r "nuget: Nake.Utility, 3.0.0"

#r "System.Xml"
#r "System.Xml.Linq"
#r "System.IO.Compression"
#r "System.IO.Compression.FileSystem"
#r "System.Net.WebClient"

using Nake;
using static Nake.FS;
using static Nake.Run;
using static Nake.Log;
using static Nake.Env;
using static Nake.App;

using System.Linq;
using System.Net;
using System.Xml.Linq;
using System.Diagnostics;
using System.IO.Compression;

const string CoreProject = "Streamstone";

var RootPath = "%NakeScriptDirectory%";
var ArtifactsPath = $@"{RootPath}\Artifacts";
var ReleasePackagesPath = $@"{ArtifactsPath}\Release";

var AppVeyor = false;
var Version = "2.0.0-dev";

/// Installs dependencies and builds sources in Debug mode
[Nake] void Default()
{
    Restore();
    Build();
}

/// Builds sources using specified configuration
[Step] void Build(string config = "Debug", bool verbose = false) => 
    Exec("dotnet", $"build {CoreProject}.sln /p:Configuration={config}" + (verbose ? "/v:d" : ""));

/// Runs unit tests 
[Step] void Test(bool slow = false)
{
    Build("Debug");

    var tests = new FileSet{$@"{RootPath}\**\bin\Debug\**\*.Tests.dll"}.ToString(" ");
    var results = $@"{ArtifactsPath}\nunit-test-results.xml";

    try
    {
        Exec("dotnet", 
            $@"vstest {tests} --logger:trx;LogFileName={results} " +
            (AppVeyor||slow ? "" : "--TestCaseFilter:TestCategory!=Slow"));
    }
    finally
    {    	
	    if (AppVeyor)
	        new WebClient().UploadFile("https://ci.appveyor.com/api/testresults/nunit/%APPVEYOR_JOB_ID%", results);
	}
}

/// Builds official NuGet packages 
[Step] void Pack(bool skipFullCheck = false)
{
    Test(!skipFullCheck);
    Build("Release");
    Exec("dotnet", $"pack --no-build -c Release -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg -p:PackageVersion={Version} {CoreProject}.sln");
}

/// Publishes package to NuGet gallery
[Step] void Publish() => Push(CoreProject); 

void Push(string package) => Exec("dotnet", 
    @"nuget push {ReleasePackagesPath}\{package}.{Version}.nupkg " +
    "-k %NuGetApiKey% -s https://nuget.org/ --skip-duplicate");

/// Installs binary dependencies 
[Nake] void Restore() => Exec("dotnet", "restore {CoreProject}.sln");