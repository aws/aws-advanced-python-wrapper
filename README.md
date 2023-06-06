# Amazon Web Services (AWS) Advanced Python Driver

[![build_status](https://github.com/awslabs/aws-advanced-python-wrapper/actions/workflows/main.yml/badge.svg)](https://github.com/awslabs/aws-advanced-python-wrapper/actions/workflows/main.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## About the Driver

### What is Failover?
In an Amazon Aurora database (DB) cluster, failover is a mechanism by which Aurora automatically repairs the DB cluster status when a primary DB instance becomes unavailable.
It achieves this goal by electing an Aurora Replica to become the new primary DB instance, so that the DB cluster can provide maximum availability to a primary read-write DB instance.
The AWS Advanced Python Driver is designed to coordinate with this behavior in order to provide minimal downtime in the event of a DB instance failure.

## License
This software is released under the Apache 2.0 license.
