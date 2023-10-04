# Read-Write Splitting Plugin Performance Results

### Read-Write Splitting Plugin Postgres Performance Results

| Benchmark                                                                                                       | Average Test Run Overhead Time | Units       |
|-----------------------------------------------------------------------------------------------------------------|--------------------------------|-------------|
| Switch to Reader With Read-Write Splitting Plugin Enabled                                                       | +11.94                         | ms/test-run |
| Switch back to Writer (use cached connection) With Read-Write Splitting Plugin Enabled                          | +0.95                          | ms/test-run |
| Switch to Reader Read-Write Splitting Plugin with Connection Pooling Enabled                                    | +3.34                          | ms/test-run |
| Switch back to  Writer (use cached connection) With Read-Write Splitting Plugin with Connection Pooling Enabled | +0.55                          | ms/test-run |
