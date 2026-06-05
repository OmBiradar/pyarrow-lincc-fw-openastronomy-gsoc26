# Architecture

Currently a runner written in `go` ([can be found here](./runner/)) does the following:

- Builds arrow for C++ and Python
- Runs test (Both C++ and Python)

## TODO

- [ ] Cleanup the [notebook directory](./notebook/)
- [ ] Need to write proper python scripts and C++ scripts to test and benchmark.
- [ ] Update GitHub benchmarks to run parallel long running benchmark scripts.
- [ ] Write scripts to test other parquet readers with different core implementations.
- [ ] Write GitHub runners to benchmark the other scripts
- [ ] Submit a PR to the arrow repository with the changes and benchmark details.
