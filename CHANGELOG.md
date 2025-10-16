# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/#semantic-versioning-200).

## [1.4.0] - 2025-10-17
### :magic_wand: Added
* [EFM v2](https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-driver/using-plugins/UsingTheHostMonitoringPlugin.md#host-monitoring-plugin-v2), an improved alternate version of the `efm` plugin which addresses issues such as garbage collection and monitoring stability, is now live!

### :bug: Fixed
* Update subscribed methods to explicit methods ([PR #960](https://github.com/aws/aws-advanced-python-wrapper/pull/960))
* Limitless Connection Plugin to properly round the load metric values for Limitless transaction routers ([PR #988](https://github.com/aws/aws-advanced-python-wrapper/pull/988)).

### :crab: Changed
* Update documentation for Limitless Plugin ([PR #914](https://github.com/aws/aws-advanced-python-wrapper/pull/914)).
* Update documentation for Blue/Green Support ([PR #995](https://github.com/aws/aws-advanced-python-wrapper/pull/995)).
* Add qualifiers to PostgreSQL SQL statements ([PR #1007](https://github.com/aws/aws-advanced-python-wrapper/pull/1007)).

## [1.3.0] - 2025-07-28
### :magic_wand: Added
* [Blue/Green Plugin](https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-driver/using-plugins/UsingTheBlueGreenPlugin.md), which adds support for blue/green deployments ([PR #911](https://github.com/aws/aws-advanced-python-wrapper/pull/911)).
* Limitless Plugin, which adds support for limitless deployments ([PR #912](https://github.com/aws/aws-advanced-python-wrapper/pull/912)).
* Add weighted random host selection strategy ([PR #907](https://github.com/aws/aws-advanced-python-wrapper/pull/907)).
* Add expiration time for secrets cache in the Secrets Manager Plugin ([PR #906](https://github.com/aws/aws-advanced-python-wrapper/pull/906)).
* Allow custom secret keys for database credentials retrieval ([PR #843](https://github.com/aws/aws-advanced-python-wrapper/pull/843)).

### :bug: Fixed
* Separate plugin chain cache based on whether a plugin needs to be skipped or not ([PR #916](https://github.com/aws/aws-advanced-python-wrapper/pull/916)).
* Check the cached token and exception type before retrying connection in the auth plugins ([PR #902](https://github.com/aws/aws-advanced-python-wrapper/pull/902)).
* Set the default SSL Secure setting to True ([PR #848](https://github.com/aws/aws-advanced-python-wrapper/pull/848)).

### :crab: Changed
* Use poetry version compatible with Python 3.8 ([PR #913](https://github.com/aws/aws-advanced-python-wrapper/pull/913)).
* Port over PluginService API changes from JDBC ([PR #901](https://github.com/aws/aws-advanced-python-wrapper/pull/901)).
* Verify links in markdown documentation ([PR #909](https://github.com/aws/aws-advanced-python-wrapper/pull/909)).
* Replace poetry installation with bash for GitHub actions ([PR #903](https://github.com/aws/aws-advanced-python-wrapper/pull/903)).
* Update python requirement and environment variable information in documentation([PR #900](https://github.com/aws/aws-advanced-python-wrapper/pull/900)).

## [1.2.0] - 2024-12-12
### :magic_wand: Added
* [Custom endpoint plugin](https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-driver/using-plugins/UsingTheCustomEndpointPlugin.md), which adds support for RDS custom endpoints.

## [1.1.1] - 2024-10-18
### :magic_wand: Added
* Support for MySQL version 9+ ([PR #713](https://github.com/aws/aws-advanced-python-wrapper/pull/713)).

### :bug: Fixed
* Extended support for China endpoints ([Issue #700](https://github.com/aws/aws-advanced-python-wrapper/issues/700)).
* Removed unused SQLAlchemy dialect from documentation ([PR #714](https://github.com/aws/aws-advanced-python-wrapper/pull/714)).

## [1.1.0] - 2024-07-31
### :magic_wand: Added
* Okta authentication support. See the [documentation](./docs/using-the-python-driver/using-plugins/UsingTheOktaAuthenticationPlugin.md) for more details and sample code.

## [1.0.0] - 2024-05-23
The Amazon Web Services (AWS) Advanced Python Wrapper allows an application to take advantage of the features of clustered Aurora databases.

### :magic_wand: Added
* Support for PostgreSQL
* Support for MySQL

[1.4.0]: https://github.com/aws/aws-advanced-python-wrapper/compare/1.3.0...1.4.0
[1.3.0]: https://github.com/aws/aws-advanced-python-wrapper/compare/1.2.0...1.3.0
[1.2.0]: https://github.com/aws/aws-advanced-python-wrapper/compare/1.1.1...1.2.0
[1.1.1]: https://github.com/aws/aws-advanced-python-wrapper/compare/1.1.0...1.1.1
[1.1.0]: https://github.com/aws/aws-advanced-python-wrapper/compare/1.0.0...1.1.0
[1.0.0]: https://github.com/aws/aws-advanced-python-wrapper/releases/tag/1.0.0
