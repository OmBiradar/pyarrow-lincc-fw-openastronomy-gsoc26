# Instructions

> These are supposed to be automated and not user facing. It's just for reference.

## Orchestration

The whole workflow should be simple like

```bash
run issue_1
```

This should:

- Get the source code
- Build from source code
- Run scripts
- Benchmark scripts
- Collect results and display them

The source code of this orchestration platform will be stored at `runner` directory.

## Source code with changes

Need to get the latest changes from the following:

- <https://github.com/OmBiradar/arrow/tree/upgrade_parquet_list_struct_reading>
- <https://github.com/OmBiradar/arrow/tree/fix_list_struct_column_selection>
- <https://github.com/OmBiradar/arrow/tree/add_list_for_replace_with_mask>

## Testing scripts

The project relies around 3 major issues, thus a hierarchical structure of testing
script is needed.

Testing scripts need to be of C++ and Python.

For now, I think of this as the structure:

```
./
..../scripts
......../issue_1
........../cpp
........../python
........../results
.
.
.
```

## Benchmarking

Google benchmark automatically does the work of benchmarking, as it also
ensures that the benchmarks can be reproduced.
