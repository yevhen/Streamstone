#r "System.Xml"
#r "System.Xml.Linq"

using Nake.FS;
using Nake.Run;
using Nake.Log;
using Nake.Env;

using System.Linq;
using System.Net;
using System.Xml.Linq;
using System.Diagnostics;

const string Project = "Streamstone";
const string RootPath = "$NakeScriptDirectory$";
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
    Install();

    Clean(outDir);
    
    Exec(@"dotnet build", 
          "{Project}.sln /p:Configuration={config};OutDir={outDir};ReferencePath={outDir}");

    //Exec(@"$ProgramFiles(x86)$\MSBuild\14.0\Bin\MSBuild.exe",
    //      "{Project}.sln /p:Configuration={config};OutDir={outDir};ReferencePath={outDir}");
}

/// Runs unit tests 
[Step] void Test(string outDir = OutputPath)
{
    Build("Debug", outDir);

    var tests = new FileSet{@"{outDir}\*.Tests.dll"}.ToString(" ");
    var results = @"{outDir}\nunit-test-results.xml";
    
    Cmd(@"Packages\NUnit.Runners.2.6.3\tools\nunit-console.exe " + 
        @"/xml:{results} /framework:net-4.0 /noshadow /nologo {tests}");

    if (AppVeyor)
        new WebClient().UploadFile("https://ci.appveyor.com/api/testresults/nunit/$APPVEYOR_JOB_ID$", results);
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
    Cmd(@"Tools\Nuget.exe push {PackagePath}\{Project}.{Version()}.nupkg $NuGetApiKey$");
}

string Version()
{
    return FileVersionInfo
            .GetVersionInfo(@"{ReleasePath}\{Project}.dll")
            .FileVersion;
}

/// Installs dependencies (packages) from NuGet 
[Task] void Install()
{
    Cmd(@"Tools\NuGet.exe restore {Project}.sln");
    Cmd(@"Tools\NuGet.exe install Build/Packages.config -o {RootPath}\Packages");
}