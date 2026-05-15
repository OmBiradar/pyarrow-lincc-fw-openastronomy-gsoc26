#include <benchmark/benchmark.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <iostream>
#include <vector>
#include <string>
#include <cmath>

// =========================================================
// Adjustable Generation Parameters
// =========================================================
constexpr int64_t GENERATE_NUM_ROWS = 100;
constexpr int64_t GENERATE_LIST_LENGTH = 200000;

// =========================================================
// Data Generation Phase
// =========================================================
arrow::Status GenerateMassiveParquetFiles(int64_t num_rows, int64_t list_length)
{
  arrow::MemoryPool *pool = arrow::default_memory_pool();

  // 1. Define Schemas
  auto t_type = arrow::list(arrow::float64());
  auto flux_type = arrow::list(arrow::float64());
  auto band_type = arrow::list(arrow::utf8());

  auto struct_type = arrow::struct_({arrow::field("t", t_type),
                                     arrow::field("flux", flux_type),
                                     arrow::field("band", band_type)});

  auto nested_schema = arrow::schema({arrow::field("nested", struct_type)});
  auto list_schema = arrow::schema({arrow::field("t", t_type),
                                    arrow::field("flux", flux_type),
                                    arrow::field("band", band_type)});

  // 2. Setup Arrow Builders
  arrow::ListBuilder t_builder(pool, std::make_shared<arrow::DoubleBuilder>(pool));
  arrow::ListBuilder flux_builder(pool, std::make_shared<arrow::DoubleBuilder>(pool));
  arrow::ListBuilder band_builder(pool, std::make_shared<arrow::StringBuilder>(pool));

  auto t_value_builder = static_cast<arrow::DoubleBuilder *>(t_builder.value_builder());
  auto flux_value_builder = static_cast<arrow::DoubleBuilder *>(flux_builder.value_builder());
  auto band_value_builder = static_cast<arrow::StringBuilder *>(band_builder.value_builder());

  // 3. Populate Data Arrays
  for (int64_t i = 0; i < num_rows; ++i)
  {
    ARROW_RETURN_NOT_OK(t_builder.Append());
    ARROW_RETURN_NOT_OK(flux_builder.Append());
    ARROW_RETURN_NOT_OK(band_builder.Append());

    for (int64_t j = 0; j < list_length; ++j)
    {
      double time_val = i * 1000.0 + j * 0.5;
      ARROW_RETURN_NOT_OK(t_value_builder->Append(time_val));
      ARROW_RETURN_NOT_OK(flux_value_builder->Append(std::sin(time_val * 0.01) * 15.0));

      const char *band_str = (j % 2 == 0) ? "g" : "r";
      ARROW_RETURN_NOT_OK(band_value_builder->Append(band_str));
    }
  }

  std::shared_ptr<arrow::Array> t_array, flux_array, band_array;
  ARROW_RETURN_NOT_OK(t_builder.Finish(&t_array));
  ARROW_RETURN_NOT_OK(flux_builder.Finish(&flux_array));
  ARROW_RETURN_NOT_OK(band_builder.Finish(&band_array));

  // 4. Construct Tables
  auto list_table = arrow::Table::Make(list_schema, {t_array, flux_array, band_array});

  std::vector<std::shared_ptr<arrow::Array>> children = {t_array, flux_array, band_array};
  std::vector<std::string> field_names = {"t", "flux", "band"};
  ARROW_ASSIGN_OR_RAISE(auto struct_array, arrow::StructArray::Make(children, field_names));
  auto nested_table = arrow::Table::Make(nested_schema, {struct_array});

  // 5. Write to Disk
  ARROW_ASSIGN_OR_RAISE(auto outfile1, arrow::io::FileOutputStream::Open("list_parquet.parquet"));
  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*list_table, pool, outfile1, 1024 * 1024));

  ARROW_ASSIGN_OR_RAISE(auto outfile2, arrow::io::FileOutputStream::Open("nested_parquet.parquet"));
  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*nested_table, pool, outfile2, 1024 * 1024));

  return arrow::Status::OK();
}

// =========================================================
// Benchmark Worker Function
// =========================================================
static void BM_ParquetWorker(benchmark::State &state, const std::string &file_path, bool use_threads)
{
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

  parquet::ArrowReaderProperties arrow_props = parquet::default_arrow_reader_properties();
  arrow_props.set_use_threads(use_threads);
  builder.properties(arrow_props);

  auto reader_result = builder.Build();
  if (!reader_result.ok())
  {
    state.SkipWithError("Failed to build Arrow reader");
    return;
  }
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader = std::move(*reader_result);

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
// Benchmarks for List Format (replaces Flat)
// =========================================================
static void BM_ListStorage_SingleThread(benchmark::State &state)
{
  BM_ParquetWorker(state, "list_parquet.parquet", false);
}
BENCHMARK(BM_ListStorage_SingleThread)->Unit(benchmark::kMillisecond)->MeasureProcessCPUTime();

static void BM_ListStorage_MultiThread(benchmark::State &state)
{
  BM_ParquetWorker(state, "list_parquet.parquet", true);
}
BENCHMARK(BM_ListStorage_MultiThread)->Unit(benchmark::kMillisecond)->MeasureProcessCPUTime();

// =========================================================
// Benchmarks for Struct Format (replaces Nested)
// =========================================================
static void BM_StructStorage_SingleThread(benchmark::State &state)
{
  BM_ParquetWorker(state, "nested_parquet.parquet", false);
}
BENCHMARK(BM_StructStorage_SingleThread)->Unit(benchmark::kMillisecond)->MeasureProcessCPUTime();

static void BM_StructStorage_MultiThread(benchmark::State &state)
{
  BM_ParquetWorker(state, "nested_parquet.parquet", true);
}
BENCHMARK(BM_StructStorage_MultiThread)->Unit(benchmark::kMillisecond)->MeasureProcessCPUTime();

// =========================================================
// Custom Main Entry Point
// =========================================================
int main(int argc, char **argv)
{
  std::cout << "--- Pre-Benchmark Phase ---\n";
  std::cout << "Generating test data (" << GENERATE_NUM_ROWS << " rows, "
            << GENERATE_LIST_LENGTH << " items per list)...\n";

  arrow::Status st = GenerateMassiveParquetFiles(GENERATE_NUM_ROWS, GENERATE_LIST_LENGTH);
  if (!st.ok())
  {
    std::cerr << "Fatal Error during data generation: " << st.ToString() << "\n";
    return 1;
  }

  std::cout << "Data generation complete. Initiating Google Benchmark...\n";
  std::cout << "---------------------------\n\n";

  // Initialize and run Google Benchmark
  ::benchmark::Initialize(&argc, argv);
  if (::benchmark::ReportUnrecognizedArguments(argc, argv))
    return 1;
  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();

  return 0;
}