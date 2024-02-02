# Benchmarks for AWS Advanced Python Driver

This directory contains a set of benchmarks for the AWS Advanced Python Driver.
These benchmarks measure the overhead from executing Python method calls with multiple connection plugins enabled.
The benchmarks do not measure the performance of target Python drivers nor the performance of the failover process.

## Usage
1. Install `pytest-benchmark` with the following command `pip install pytest-benchmark`.
2. Run the benchmarks with the following command `pytest <path to benchmark file>`.
    1. You have to run the command separately for each benchmark file.

## Benchmark Results

Tests ran on commit [3d473f6c12f495762d3b7436a97a0e6bd76361d7](https://github.com/awslabs/aws-advanced-python-wrapper/commit/3d473f6c12f495762d3b7436a97a0e6bd76361d7)

## Summary
| Benchmark                                                                                                                   | Overhead |
|-----------------------------------------------------------------------------------------------------------------------------|----------|
| Execute query with execution time plugin vs without execution time plugin                                                   | 128%     |
| Init and release read write splitting plugin with vs without internal connection pools                                      | 103%     |
| Init and release read write splitting plugin and aurora connection tracker plugin with vs without internal connection pools | 102%     |
| Init plugin manager with vs without plugins                                                                                 | 271%     |
| Init host provider with vs without plugins                                                                                  | 100%     |
| Notify connection changed with vs without plugins                                                                           | 106%     |
| Execute with vs without plugins                                                                                             | 105%     |
| Connect with vs without plugins                                                                                             | 100%     |

## Raw Data
| Name (microseconds)                                                                                            | Min    | Max      | Mean   |
|----------------------------------------------------------------------------------------------------------------|--------|----------|--------|
| test_create_cursor_baseline                                                                                    | 103.46 | 21863.92 | 176.86 |
| test_init_and_release_with_read_write_splitting_plugin                                                         | 117.63 | 23399.29 | 155.63 |
| test_init_and_release_with_execution_time_plugin                                                               | 118.13 | 11647.50 | 153.55 |
| test_init_and_release_with_aurora_connection_tracker_plugin                                                    | 118.17 | 16153.21 | 173.76 |
| test_init_and_release_with_read_write_splitting_plugin_internal_connection_pools                               | 121.38 | 23713.75 | 175.25 |
| test_init_and_release_with_execute_time_and_aurora_connection_tracker_plugin                                   | 126.17 | 20433.96 | 175.94 |
| test_init_and_release_with_aurora_connection_tracker_and_read_write_splitting_plugin                           | 127.79 | 22220.75 | 181.27 |
| test_init_and_release_with_aurora_connection_tracker_and_read_write_splitting_plugin_internal_connection_pools | 130.75 | 27415.50 | 178.62 |
| test_execute_query_with_execute_time_plugin                                                                    | 132.79 | 37440.38 | 248.63 |


| Name (nanoseconds)                             | Min    | Max       | Mean       |
|------------------------------------------------|--------|-----------|------------|
| test_init_plugin_manager_with_no_plugins       | 875    | 34708     | 1040.50    |
| test_init_plugin_manager_with_plugins          | 2375   | 73917     | 2550.91    |
| test_release_resources_with_no_plugins         | 4625   | 67500     | 4889.21    |
| test_init_host_provider_with_no_plugins        | 49959  | 44311417  | 61497.79   |
| test_init_host_provider_with_plugins           | 50208  | 280917    | 58635.75   |
| test_release_resources_with_plugins            | 50250  | 634750    | 52090.03   |
| test_notify_connection_changed_with_no_plugins | 50542  | 9588125   | 60554.89   |
| test_notify_connection_changed_with_plugins    | 53625  | 9803958   | 63355.84   |
| test_execute_with_no_plugins                   | 78375  | 110642542 | 163380.40  |
| test_execute_with_plugins                      | 82458  | 47187834  | 142509.64  |
| test_connect_with_no_plugins                   | 802750 | 62966167  | 1316711.28 |
| test_connect_with_plugins                      | 805458 | 157731334 | 1619050.00 |
