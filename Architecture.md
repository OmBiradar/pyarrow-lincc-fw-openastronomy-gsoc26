# Architecture

This project should follow a bit of a nested and complicated but elegant architecture
to do the following:

- Build arrow binaries from source
- Run scripts
- Benchmark the above script times

All of this can be orchestrated using some cli I believe. A strong candidate is
`go` with it's parallel processing go routines and it works across a lot of systems.

The source for the `arrow` files will be my own fork of arrow with the fixes/changes
within separate branches. This will keep everything separated and well maintained.

A benchmarking system is needed, something that can measure the execution time
to keep it simple for now. I guess google benchmark can be used here. Also `go`
should be able to orchestrate this too.

Overall the end result is the execution time analysis of the scripts given
certain changes are made in the `arrow` codebase.
