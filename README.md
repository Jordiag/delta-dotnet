<picture>
  <img alt="delta lake dotnet" src="https://user-images.githubusercontent.com/8865104/208916475-c45562fa-d28a-40f0-bdb4-745f2ed94b42.png" height="250">
</picture>

# Delta lake for .NET

Delta lake for .NET (or delta-dotnet) is a .NET native, high-performance, [delta lake](https://delta.io) ([delta-io](https://github.com/delta-io)) interface library that runs on Linux, Windows, and macOS. Enjoy reliable access from .NET to [ACID](https://en.wikipedia.org/wiki/ACID) Delta Lake big data tables.

## Status

Currently in [pre-alpha](https://en.wikipedia.org/wiki/Software_release_life_cycle#Pre-alpha) release stage located in the [initial-research branch](https://github.com/Jordiag/delta-dotnet/tree/initial-research).

| Build | Unit Tests | Code Smells | Code coverage | Maintainability rating| Security rating | Bugs | Vulnerabilities | Nuget |  
|-------|------------|-----------------|-------|-------|-------|-------|-------|-------|
|   [![.NET Build](https://github.com/Jordiag/delta-dotnet/actions/workflows/build.yml/badge.svg?branch=initial-research)](https://github.com/Jordiag/delta-dotnet/actions/workflows/build.yml)   |    <img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A  yet      |        [![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=Jordiag_DeltaLake.Net&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=Jordiag_DeltaLake.Net)       |  [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=Jordiag_DeltaLake.Net&metric=coverage)](https://sonarcloud.io/summary/new_code?id=Jordiag_DeltaLake.Net)| [![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=Jordiag_DeltaLake.Net&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=Jordiag_DeltaLake.Net)|[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=Jordiag_DeltaLake.Net&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=Jordiag_DeltaLake.Net)| [![Bugs](https://sonarcloud.io/api/project_badges/measure?project=Jordiag_DeltaLake.Net&metric=bugs)](https://sonarcloud.io/summary/new_code?id=Jordiag_DeltaLake.Net)| [![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=Jordiag_DeltaLake.Net&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=Jordiag_DeltaLake.Net)|<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A yet  |

## Why

Delta lake ([delta-io](https://github.com/delta-io)) has some public implementations with languages like [Rust](https://github.com/delta-io/delta-rs), [Python](https://github.com/delta-io/delta-rs/tree/main/python)... but no [.NET](https://dotnet.microsoft.com/en-us/) which somewhat limits .NET platform in big data applications. We still don't have anything good and .NET native in this area, for instance, it isn't possible to read a Databricks delta lake table natively from .NET platform (December 2022).

## Features

Delta lake for .NET is still in pre-alpha release stage (started in December 2022) where only "read table" feature is going to be implemented but many others are explored. In the future, all the other features will be implemented and updated in these tables. Current development is done using .NET 7 version.

| Operation/Feature                                 | main | initial-research |
| ------------------------------------------------- | ---- | ----------------- |
| Read table                                        |<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|<img src="https://user-images.githubusercontent.com/8865104/208909673-02f96934-a836-4141-84aa-db2dc7686d5c.svg" alt="in-progress" width="20" height="20" align="center" /> in-progress|
| Stream table update                               |<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|
| Filter files with partitions                      |<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|
| Vacuum (delete stale files)                       |<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|
| History                                           |<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|
| Write transactions                                |<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|
| Checkpoint creation                               |<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|
| High-level file writer                            |<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|
| Optimize                                          |<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|

| Supported backends                                | Status |
| ------------------------------------------------- | ---- |
| Local file system                                 |<img src="https://user-images.githubusercontent.com/8865104/208909673-02f96934-a836-4141-84aa-db2dc7686d5c.svg" alt="in-progress" width="20" height="20" align="center" /> in-progress|
| AWS S3                                            |<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|
| Azure Blob Storage / Azure  Datalake Storage Gen2 |<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|
| Google Cloud Storage                              |<img src="https://user-images.githubusercontent.com/8865104/208910828-d9a283f0-d8f4-4fc2-ac45-a8b5ac65b2e7.svg" alt="not-available" width="20" height="20" align="center" /> N/A|


## Documentation

Delta-dotnet documentation will start to be available at [Docs folder](https://github.com/Jordiag/delta-dotnet/tree/main/Docs) when we reach [beta](https://en.wikipedia.org/wiki/Software_release_life_cycle#Beta) release stage.

## Download a release

Release builds will be available on the [Releases page](https://github.com/Jordiag/delta-dotnet/releases) when we reach [alpha](https://en.wikipedia.org/wiki/Software_release_life_cycle#Alpha) release stage.

### Build

```sh
git clone https://github.com/Jordiag/delta-dotnet --recursive
cd delta-dotnet
dotnet build -c release
```

## Test

Run the delta-dotnet tests as follows:

```sh
dotnet test Delta.sln -c debug
```

## License

Delta-dotnet is an open-source software licensed under the [MIT License](https://github.com/git/git-scm.com/blob/main/MIT-LICENSE.txt).

## Code of Conduct

This project has adopted the
[Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the
[Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
or contact [opencode@microsoft.com](mailto:opencode@microsoft.com)
with any additional questions or comments.
