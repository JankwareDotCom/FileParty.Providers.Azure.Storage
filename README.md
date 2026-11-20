# FileParty.Providers.Azure.Storage
Azure Storage Nuget for FileParty

Azure Blob Storage Provider for [FileParty](https://github.com/JankwareDotCom/FileParty) leveraging [Microsoft's Azure SDK](https://github.com/Azure/azure-sdk-for-net/tree/master/sdk/storage/Azure.Storage.Blobs)

![dotnet_test](https://github.com/JankwareDotCom/FileParty.Providers.Azure.Storage/workflows/dotnet_test/badge.svg)
[![Nuget Package](https://badgen.net/nuget/v/FileParty.Providers.Azure.Storage)](https://www.nuget.org/packages/FileParty.Providers.Azure.Storage/)

## Supported frameworks

The package ships these assets:

| Asset | Used by |
| ----- | ------- |
| `netstandard2.0` | .NET Framework 4.6.2+ (including 4.8), and anything else netstandard2.0-compatible |
| `netstandard2.1` | .NET Core 3.x, .NET 5–7 |
| `net8.0` | .NET 8 and 9 |
| `net10.0` | .NET 10+ |

Behavior is the same on every target.

## Dependency ranges

FileParty keeps its dependency floors low so you can upgrade FileParty without being forced to upgrade
Azure or Microsoft.Extensions packages. NuGet picks the lowest version that satisfies every package in
your graph, so your own pins win as long as they fall inside these ranges. The ranges are the same on
every target framework.

| Dependency | Range |
| ---------- | ----- |
| Azure.Storage.Blobs | [12.13.0, 13.0.0) |
| FileParty.Core | [2.1.0, 3.0.0) |

Notes:

- All FileParty packages (`FileParty.Core`, `FileParty.Providers.FileSystem`, `FileParty.Providers.AWS.S3`,
  `FileParty.Providers.Azure.Storage`) share one version number. Use the same version for each.
  FileParty.Core's own dependency ranges are listed in the [FileParty README](https://github.com/JankwareDotCom/FileParty#dependency-ranges).
- `Azure.Storage.Blobs` flows to your app at runtime, but its compile-time API does not. To call
  `AzureBlobBaseConfiguration.SetClientOptions(Action<BlobClientOptions>)`, reference `Azure.Storage.Blobs`
  in your own project. Any 12.x version from 12.13.0 up works.
- `samples/compat` is a .NET Framework 4.8 + .NET 6 consumer that pins `Azure.Storage.Blobs` 12.13.0 and
  older AWS / Microsoft.Extensions versions, and restores all four FileParty packages from a local feed.
  CI runs it on every push. Any forced upgrade or downgrade (NU1605/NU1608) fails the build.
- Every pack runs [package validation](https://learn.microsoft.com/dotnet/fundamentals/apicompat/package-validation/overview)
  against the latest release on nuget.org, and CI also validates against 1.3.1, so public API breaks fail the build.

## How To Use

Register `AzureModule` as you would any other FileParty Module.

```csharp
services.AddFileParty(c => c.AddModule<AzureModule>(new AzureBlobConfiguration
{
    ConnectionString = "<storage account connection string>",
    ContainerName = "my-container",
}));
```

## Building

The .NET 10 SDK is pinned in `global.json`. The integration tests need a `CONNECTION_STRING` environment
variable that points at a storage account (or Azurite) with a `tests` container.

```bash
dotnet test -c Release
dotnet pack src/FileParty.Providers.Azure.Storage -c Release -p:GeneratePackageOnBuild=false -o artifacts/packages
```

To build against FileParty packages that are not on nuget.org yet, pack them from the FileParty repo and add
that folder as an extra restore source for your shell only, for example
`export RestoreAdditionalProjectSources=/path/to/FileParty/artifacts/packages`.

## Contributions / Issues / Requests Welcome
Check FileParty Core Repository for information
