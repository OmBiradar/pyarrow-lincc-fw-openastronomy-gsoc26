#include <benchmark/benchmark.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <iostream>

// =========================================================
// Generic Worker Function
// =========================================================
static void BM_ParquetWorker(benchmark::State &state, const std::string &file_path, bool use_threads)
{
  // 1. SETUP PHASE
  auto input_result = arrow::io::ReadableFile::Open(file_path);
  if (!input_result.ok())
  {
    state.SkipWithError(("Failed to open: " + file_path).c_str());
    return;
  }
  std::shared_ptr<arrow::io::RandomAccessFile> input = *input_result;

  parquet::arrow::FileReaderBuilder builder;
  if (!builder.Open(input).ok())
  {
    state.SkipWithError("Failed to open Parquet builder");
    return;
  }

  // Configure multi-threading properties
  parquet::ArrowReaderProperties arrow_props = parquet::default_arrow_reader_properties();
  arrow_props.set_use_threads(use_threads);

  // Some older versions of Arrow might require builder.properties() instead,
  // but applying it directly to the builder is standard in modern Arrow API.
  builder.properties(arrow_props);

  auto reader_result = builder.Build();
  if (!reader_result.ok())
  {
    state.SkipWithError("Failed to build Arrow reader");
    return;
  }
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader = std::move(*reader_result);

  // 2. BENCHMARK PHASE
  for (auto _ : state)
  {
    std::shared_ptr<arrow::Table> table;

    arrow::Status st = arrow_reader->ReadTable(&table);

    if (!st.ok())
    {
      state.SkipWithError(st.ToString().c_str());
      break;
    }

    benchmark::DoNotOptimize(table);
    table.reset();
  }
}

// =========================================================
// 1. Flat Parquet - Single-Threaded
// =========================================================
static void BM_Flat_SingleThread(benchmark::State &state)
{
  BM_ParquetWorker(state, "flat.parquet", false);
}
BENCHMARK(BM_Flat_SingleThread)
    ->Unit(benchmark::kMillisecond)
    ->MeasureProcessCPUTime();

// =========================================================
// 2. Flat Parquet - Multi-Threaded
// =========================================================
static void BM_Flat_MultiThread(benchmark::State &state)
{
  BM_ParquetWorker(state, "flat.parquet", true);
}
BENCHMARK(BM_Flat_MultiThread)
    ->Unit(benchmark::kMillisecond)
    ->MeasureProcessCPUTime();

// =========================================================
// 3. Nested Parquet - Single-Threaded
// =========================================================
static void BM_Nested_SingleThread(benchmark::State &state)
{
  BM_ParquetWorker(state, "nested.parquet", false);
}
BENCHMARK(BM_Nested_SingleThread)
    ->Unit(benchmark::kMillisecond)
    ->MeasureProcessCPUTime();

// =========================================================
// 4. Nested Parquet - Multi-Threaded
// =========================================================
static void BM_Nested_MultiThread(benchmark::State &state)
{
  BM_ParquetWorker(state, "nested.parquet", true);
}
BENCHMARK(BM_Nested_MultiThread)
    ->Unit(benchmark::kMillisecond)
    ->MeasureProcessCPUTime();

// =========================================================
// Main Entry Point
// =========================================================
BENCHMARK_MAIN();