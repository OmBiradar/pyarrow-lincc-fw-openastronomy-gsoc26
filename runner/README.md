# Runner for the whole complex process

## The start

Clone the apache arrow fork - OmBiradar/arrow

Check if the required tools and libraries are installed

- `uv` for python
- `cmake` for C++ build
- `snappy` for compression algorithms
- `gcc` and `g++` for C++ compiler

## The API

```bash
>> cpp clean
>> cpp build
>> cpp rebuild
```

```bash
>> py clean
>> py build
>> py rebuild
```

```bash
>> py add <PACKAGE_NAME>
>> py remove <PACKAGE_NAME>
```

```bash
>> run main.cpp
>> run main.py
```

```bash
>> run jupyter
```
