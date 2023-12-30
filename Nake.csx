#r "nuget: Nake.Meta, 3.0.0"
#r "nuget: Nake.Utility, 3.0.0"

#r "System.Net.WebClient"

using Nake;
using static Nake.FS;
using static Nake.Log;
using static Nake.Env;

using System.Linq;
using System.Net;

const string CoreProject = "Streamstone";

var RootPath = "%NakeScriptDirectory%";
var ArtifactsPath = $@"{RootPath}\Artifacts";
var ReleasePackagesPath = $@"{ArtifactsPath}\Release";

var AppVeyorJobId = Var["APPVEYOR_JOB_ID"];
var TargetFramework = "net8";
var Version = "3.0.0-dev";

/// Installs dependencies and builds sources in Debug mode
[Nake] async Task Default() => await Build();

/// Builds sources using specified configuration
[Step] async Task Build(string config = "Debug", bool verbose = false) => 
    await $@"dotnet build {CoreProject}.sln /p:Configuration={config} {(verbose ? "/v:d" : "")}";

/// Runs unit tests 
// Runs unit tests 
[Nake] async Task Test(bool slow = false)
{
    await Build("Debug");

    var tests = new FileSet{$"{RootPath}/**/bin/Debug/{TargetFramework}/*.Tests.dll"}.ToString(" ");
    var results = $@"{ArtifactsPath}/nunit-test-results.xml";    

    try
    {
        await $@"dotnet vstest {tests} --logger:trx;LogFileName={results} \
              {(AppVeyorJobId != null||slow ? "" : "--TestCaseFilter:TestCategory!=Slow")}";
    }
    finally
    {    	
        if (AppVeyorJobId != null)
        {
            var workerApi = $"https://ci.appveyor.com/api/testresults/mstest/{AppVeyorJobId}";
            Info($"Uploading {results} to {workerApi} using job id {AppVeyorJobId} ...");
            
            var response = new WebClient().UploadFile(workerApi, results);
            var result = Encoding.UTF8.GetString(response);
                      
            Info($"Appveyor response is: {result}");
        }
    }
}

/// Builds official NuGet packages 
[Step] async Task Pack(bool skipFullCheck = false)
{
    await Test(!skipFullCheck);
    await Build("Release");
    
    await $@"dotnet pack --no-build -c Release -p:IncludeSymbols=true \
            -p:SymbolPackageFormat=snupkg -p:PackageVersion={Version} {CoreProject}.sln";
}

/// Publishes package to NuGet gallery
[Step] async Task Publish() => await Push(CoreProject); 

async Task Push(string package) => 
    await $@"dotnet nuget push {ReleasePackagesPath}\{package}.{Version}.nupkg \
            -k %NuGetApiKey% -s https://nuget.org/ --skip-duplicate";