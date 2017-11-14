PROJECT=Streamstone
OUT_DIR=$(CURDIR)/Output
VERSION=$(shell cat $(CURDIR)/Source/Streamstone.Version.cs | grep -oP [0-9]\.[0-9]\.[0-9] | uniq)

nuget=$(CURDIR)/Tools/Nuget.exe
nunit-test-results=nunit-test-results.trx

default: package

build-debug:
	@dotnet build ${PROJECT}.sln /p:Configuration=Debug

build-release:
	@dotnet build ${PROJECT}.sln /p:Configuration=Release

verify:
	@dotnet test Source/**/*.Tests.csproj --configuration Debug \
	-l:trx\;LogFileName=${nunit-test-results} \
	--results-directory ${OUT_DIR}

package: verify build-release
	@${nuget} pack ${CURDIR}/Source/${PROJECT}/${PROJECT}.nuspec \
	-Version ${VERSION} \
	-OutputDirectory ${OUT_DIR} \
	-BasePath ${CURDIR}/Source/Streamstone/bin/Release \
	-NoPackageAnalysis

	$(if $(filter $(APPVEYOR),True), \
		curl -F '-data=@$(OUT_DIR)/$(nunit-test-results)' \
		https://ci.appveyor.com/api/testresults/mstest/$(APPVEYOR_JOB_ID))

publish:
	@dotnet nuget push ${OUT_DIR}/${PROJECT}.${VERSION}.nupkg --api-key $(NuGetApiKey) --source https://nuget.org/	

version:
	@echo $(VERSION)
