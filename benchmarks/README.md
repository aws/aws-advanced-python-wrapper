# Benchmarks for AWS Advanced Python Driver

This directory contains a set of benchmarks for the AWS Advanced Python Driver.
These benchmarks measure the overhead from executing Python method calls with multiple connection plugins enabled.
The benchmarks do not measure the performance of target Python drivers nor the performance of the failover process.

## Usage
1. Install `pytest-benchmark` with the following command `pip install pytest-benchmark`.
2. Run the benchmarks with the following command `pytest <path to benchmark file>`.
    1. You have to run the command separately for each benchmark file.
