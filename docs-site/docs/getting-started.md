---
id: getting-started
title: Getting Started
---

## Installation

You can install Streamstone via NuGet:

```powershell
PM> Install-Package Streamstone
```

[NuGet Package](https://www.nuget.org/packages/Streamstone/)

## Building from Source

To build Streamstone binaries:

- **Windows:** Visual Studio 2017 Update 3 or higher and .NET Core SDK 2.0 or higher are required.
- **Linux/MacOS:** Use the .NET CLI tooling:

```bash
dotnet build
```

## Running Unit Tests

Streamstone uses [Azurite](https://www.npmjs.com/package/azurite#npm) to emulate Azure Table Storage for tests and examples.

> **Warning:** Azurite does not fully emulate Azure Table Storage, so some tests may fail.

Alternatively, you can run tests against a real Azure account by setting the `Streamstone-Test-Storage` environment variable to your storage account connection string.

## Next Steps

- [Usage](usage.md)
- [Design](design.md)
- [Limitations](limitations.md) 