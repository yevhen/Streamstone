#r "System.Xml"
#r "System.Xml.Linq"

using System.Linq;
using System.Net;
using System.Xml.Linq;
using System.Diagnostics;

using static Nake.App;
using static Nake.Env;
using static Nake.FS;
using static Nake.Log;
using static Nake.Run;

const string Project = "Streamstone";
const string RootPath = "%NakeScriptDirectory%";
const string OutputPath = RootPath + @"\Output";
const string PackagePath = OutputPath + @"\Package";
const string ReleasePath = PackagePath + @"\Release";

var AppVeyor = Var["APPVEYOR"] == "True";

/// Builds sources in Debug mode
[Task] void Default()
{
    Build();
}

/// Wipeout all build output and temporary build files
[Step] void Clean(string path = OutputPath)
{
    Delete(@"{path}\*.*|-:*.vshost.exe");
}

/// Builds sources using specified configuration and output path
[Step] void Build(string config = "Debug", string outDir = OutputPath)
{
    Clean(outDir);
    
    Exec("dotnet", "build {Project}.sln /p:Configuration={config};OutDir={outDir};ReferencePath={outDir}");
}

/// Runs unit tests 
[Step] void Test(string outDir = OutputPath)
{   	
	Exec("dotnet", "test -l:trx;LogFileName=nunit-test-results.trx -r:\"{outDir}\"");

    var tests = new FileSet{@"{outDir}\*.Tests.dll"}.ToString(" ");
    var results = @"{outDir}\nunit-test-results.trx";
    if (AppVeyor)
        new WebClient().UploadFile("https://ci.appveyor.com/api/testresults/mstest/%APPVEYOR_JOB_ID%", results);
}

/// Builds official NuGet package 
[Step] void Package()
{
    Test(PackagePath + @"\Debug");
    Build("Release", ReleasePath);

    var version = FileVersionInfo
        .GetVersionInfo(@"{ReleasePath}\{Project}.dll")
        .FileVersion;

    Cmd(@"Tools\Nuget.exe pack Build\{Project}.nuspec -Version {version} " +
        "-OutputDirectory {PackagePath} -BasePath {RootPath} -NoPackageAnalysis");
}

/// Publishes package to NuGet gallery
[Step] void Publish()
{
    // Cmd(@"Tools\Nuget.exe push {PackagePath}\{Project}.{Version()}.nupkg %NuGetApiKey%");
}

string Version()
{
    return FileVersionInfo
            .GetVersionInfo(@"{ReleasePath}\{Project}.dll")
            .FileVersion;
}