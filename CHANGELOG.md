# Changelog

All FileParty packages (`FileParty.Core`, `FileParty.Providers.FileSystem`, `FileParty.Providers.AWS.S3`,
`FileParty.Providers.Azure.Storage`) share one version number.

## 2.1.0

A drop-in upgrade from 1.3.x and 2.0.0: change only the FileParty version. There are no public API changes,
and no Azure or Microsoft.Extensions upgrades are forced on you.

### Supported frameworks

- `netstandard2.0` (.NET Framework 4.6.2+ including 4.8), `netstandard2.1`, `net8.0`, `net10.0` (unchanged from 2.0.0).

### Dependency ranges

| Dependency | 2.0.0 | 2.1.0 |
| ---------- | ----- | ----- |
| Azure.Storage.Blobs | [12.13.0, 13.0.0) | [12.13.0, 13.0.0) (unchanged) |
| FileParty.Core | >= 1.3.0 | [2.1.0, 3.0.0) |

2.0.0 declared FileParty.Core >= 1.3.0, so NuGet could pair it with a 1.x Core. 2.1.0 requires the matching
2.x Core and stops at the next major version.

Checked against consumers pinned to Azure.Storage.Blobs 12.13.0, AWSSDK.S3 3.7.415.26, AWSSDK.Core 3.7.402.51,
AWSSDK.SecurityToken 3.7.1.199–3.7.400.33, Microsoft.Extensions.DependencyInjection 9.0.10–10.0.12 and
System.Threading.Tasks.Extensions 4.5.4–4.6.3, on net48 and net6.0.

### Security

- NuGet audit (direct and transitive dependencies, every project and build configuration, including the
  net48 test target and both compat profiles) reports no known vulnerabilities. Nothing needed patching.
- The Azure.Storage.Blobs floor stays at 12.13.0: earlier versions are affected by
  [GHSA-64x4-9hc6-r2h6](https://github.com/advisories/GHSA-64x4-9hc6-r2h6) (moderate).
- Restores now audit transitive packages too (`NuGetAuditMode=all`).

### Build and CI

- SDK pinned to .NET 10 via `global.json`. CI and publish use it.
- Package validation runs on every pack against the latest nuget.org release (2.0.0). CI also validates against 1.3.1.
- New `samples/compat` consumer project (net48 + net6.0, low/high profiles) runs in CI and fails on NU1605/NU1608.
  It takes FileParty.Core, FileSystem and AWS.S3 2.1.0 from nuget.org, so FileParty 2.1.0 must be published first.
- Tests also run on `net48` on Windows CI, which exercises the `netstandard2.0` build.
- Package description fixed (it said "AWS S3 Module").

## 2.0.0

- Added `net10.0`; dropped `net6.0` (net6.0 consumers now use the `netstandard2.1` asset).

## 1.3.1

- Azure.Storage.Blobs floor raised from 12.5.0 to 12.13.0.

## 1.3.0

- Added `net8.0`.
