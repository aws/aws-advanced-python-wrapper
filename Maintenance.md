# Release Schedule

| Release Date     | Release                                                                                    |
|------------------|--------------------------------------------------------------------------------------------|
| May 16, 2024     | [Release 1.0.0](https://github.com/awslabs/aws-advanced-python-wrapper/releases/tag/1.0.0) |
| July 31, 2024    | [Release 1.1.0](https://github.com/awslabs/aws-advanced-python-wrapper/releases/tag/1.1.0) |
| October 18, 2024 | [Release 1.1.1](https://github.com/awslabs/aws-advanced-python-wrapper/releases/tag/1.1.1) |

`aws-advanced-python-wrapper` [follows semver](https://semver.org/#semantic-versioning-200) which means we will only
release breaking changes in major versions. Generally speaking patches will be released to fix existing problems without
adding new features. Minor version releases will include new features as well as fixes to existing features. We will do
our best to deprecate existing features before removing them completely.

For minor version releases, `aws-advanced-python-wrapper` uses a “release-train” model. Approximately every four weeks we
release a new minor version which includes all the new features and fixes that are ready to go.
Having a set release schedule makes sure `aws-advanced-python-wrapper` is released in a predictable way and prevents a
backlog of unreleased changes.

In contrast, `aws-advanced-python-wrapper` releases new major versions only when there are a critical mass of
breaking changes (e.g. changes that are incompatible with existing APIs). This tends to happen if we need to
change the way the driver is currently working. Fortunately, the JDBC API is fairly mature and has not changed, however
in the event that the API changes we will release a version to be compatible.

Please note: Both the roadmap and the release dates reflect intentions rather than firm commitments and may change
as we learn more or encounter unexpected issues. If dates do need to change, we will be as transparent as possible,
and log all changes in the changelog at the bottom of this page.

# Maintenance Policy

For `aws-advanced-python-wrapper` new features and active development always takes place against the newest version.
The `aws-advanced-python-wrapper` project follows the semantic versioning specification for assigning version numbers
to releases, so you should be able to upgrade to the latest minor version of that same major version of the
software without encountering incompatible changes (e.g., 1.1.0 → 1.3.x).

Sometimes an incompatible change is unavoidable. When this happens, the software’s maintainers will increment
the major version number (e.g., increment from `aws-advanced-python-wrapper` 1.1.1 to `aws-advanced-python-wrapper` 2.0.0).
The last minor version of the previous major version of the software will then enter a maintenance window
(e.g., 1.3.x). During the maintenance window, the software will continue to receive bug fixes and security patches,
but no new features.

We follow OpenSSF’s best practices for patching publicly known vulnerabilities, and we make sure that there are
no unpatched vulnerabilities of medium or higher severity that have been publicly known for more than 60 days
in our actively maintained versions.

The duration of the maintenance window will vary from product to product and release to release.
By default, versions will remain under maintenance until the next major version enters maintenance,
or 1-year passes, whichever is longer. Therefore, at any given time, the current major version and
previous major version will both be supported, as well as older major versions that have been in maintenance
for less than 12 months. Please note that maintenance windows are influenced by the support schedules for
dependencies the software includes, community input, the scope of the changes introduced by the new version,
and estimates for the effort required to continue maintenance of the previous version.

The software maintainers will not back-port fixes or features to versions outside the maintenance window.
That said, PRs with said back-ports are welcome and will follow the project's review process.
No new releases will result from these changes, but interested parties can create their own distribution
from the updated source after the PRs are merged.

| Major Version | Latest Minor Version | Status  | Initial Release | Maintenance Window Start | Maintenance Window End |
|---------------|----------------------|---------|-----------------|--------------------------|------------------------|
| 1             | 1.1.1                | Current | May 16, 2024    | May 16, 2024             | N/A                    | 
