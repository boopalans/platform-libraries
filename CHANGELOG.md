# Change Log
All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.1.4] 2017-11-24
### Added
- PNDA-2445: Support for Hortonworks HDP hadoop distro.
## Fixed
- PNDA-3499: Cleanup CHANGELOG with missing release info.

## [0.1.3] 2017-05-23
### Changed
- PNDA-2577: review python deps versions
- PNDA-2807: Update README.md in order to use yarn-client mode

## [0.1.2] 2016-12-12
### Changed
- Externalized build logic from Jenkins to shell script so it can be reused
- PNDA-2441: Up Spark version to 1.6

## [0.1.1] 2016-09-09
### Changed
- Project renamed to PNDA
### Added
- Jenkinsfile for CI

## [0.1.0] 2016-07-01
### First version

## [Pre-release]

- Separate simple data handler and Json data handler with configuration helper library
- Support for HDFS HA mode.
- Fully supported generic data handler (replaced netflow data handler). 
- Refactored code structure with unittests
- Added support of DNS resolving
- Added support of reasoning PNDA namenode URL
- First release of platform-libraries API.
