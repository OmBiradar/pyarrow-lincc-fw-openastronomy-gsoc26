#include <benchmark/benchmark.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/util/thread_pool.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include <cassert>
#include <cmath>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

// =========================================================
// Parameters
// =========================================================
constexpr int64_t GENERATE_NUM_ROWS    = 200'000;
constexpr int64_t GENERATE_LIST_LENGTH = 1'000;

// 4 row groups across 100 rows. Parquet skips at row-group granularity,
// so at least a handful of groups gives the format room to breathe.
constexpr int64_t ROW_GROUP_SIZE = 25;

// Per-column read-ahead buffer fed to parquet::ReaderProperties.
constexpr int64_t IO_BUFFER_BYTES = 8 << 20;  // 8 MiB

// =========================================================
// Data Generation
// =========================================================
arrow::Status GenerateParquetFiles(int64_t num_rows, int64_t list_length)
{
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Schema types
    auto t_type    = arrow::list(arrow::float64());
    auto flux_type = arrow::list(arrow::float64());
    auto band_type = arrow::list(arrow::utf8());

    auto list_schema = arrow::schema({
        arrow::field("t",    t_type),
        arrow::field("flux", flux_type),
        arrow::field("band", band_type),
    });

    auto struct_field_type = arrow::struct_({
        arrow::field("t",    t_type),
        arrow::field("flux", flux_type),
        arrow::field("band", band_type),
    });
    auto nested_schema = arrow::schema({ arrow::field("nested", struct_field_type) });

    // Builders
    arrow::ListBuilder t_builder   (pool, std::make_shared<arrow::DoubleBuilder>(pool));
    arrow::ListBuilder flux_builder(pool, std::make_shared<arrow::DoubleBuilder>(pool));
    arrow::ListBuilder band_builder(pool, std::make_shared<arrow::StringBuilder>(pool));

    auto* t_vb    = static_cast<arrow::DoubleBuilder*>(t_builder.value_builder());
    auto* flux_vb = static_cast<arrow::DoubleBuilder*>(flux_builder.value_builder());
    auto* band_vb = static_cast<arrow::StringBuilder*>(band_builder.value_builder());

    // Populate arrays
    for (int64_t i = 0; i < num_rows; ++i) {
        ARROW_RETURN_NOT_OK(t_builder.Append());
        ARROW_RETURN_NOT_OK(flux_builder.Append());
        ARROW_RETURN_NOT_OK(band_builder.Append());

        for (int64_t j = 0; j < list_length; ++j) {
            const double t = i * 1000.0 + j * 0.5;
            ARROW_RETURN_NOT_OK(t_vb->Append(t));
            ARROW_RETURN_NOT_OK(flux_vb->Append(std::sin(t * 0.01) * 15.0));
            ARROW_RETURN_NOT_OK(band_vb->Append((j % 2 == 0) ? "g" : "r"));
        }
    }

    std::shared_ptr<arrow::Array> t_arr, flux_arr, band_arr;
    ARROW_RETURN_NOT_OK(t_builder.Finish(&t_arr));
    ARROW_RETURN_NOT_OK(flux_builder.Finish(&flux_arr));
    ARROW_RETURN_NOT_OK(band_builder.Finish(&band_arr));

    // Guard against future drift in generation logic
    assert(t_arr->length() == flux_arr->length());
    assert(t_arr->length() == band_arr->length());

    // Build tables
    auto list_table = arrow::Table::Make(list_schema, {t_arr, flux_arr, band_arr});

    ARROW_ASSIGN_OR_RAISE(
        auto struct_arr,
        arrow::StructArray::Make({t_arr, flux_arr, band_arr}, {"t", "flux", "band"}));
    auto nested_table = arrow::Table::Make(nested_schema, {struct_arr});

    // Write — ROW_GROUP_SIZE is a row count, not bytes
    {
        ARROW_ASSIGN_OR_RAISE(auto out,
            arrow::io::FileOutputStream::Open("list_parquet.parquet"));
        ARROW_RETURN_NOT_OK(
            parquet::arrow::WriteTable(*list_table, pool, out, ROW_GROUP_SIZE));
    }
    {
        ARROW_ASSIGN_OR_RAISE(auto out,
            arrow::io::FileOutputStream::Open("nested_parquet.parquet"));
        ARROW_RETURN_NOT_OK(
            parquet::arrow::WriteTable(*nested_table, pool, out, ROW_GROUP_SIZE));
    }

    return arrow::Status::OK();
}

// =========================================================
// Benchmark Worker
// =========================================================
static void BM_ParquetRead(benchmark::State& state,
                           const std::string& file_path,
                           bool use_threads)
{
    // --- Open file -------------------------------------------------------
    auto file_result = arrow::io::ReadableFile::Open(file_path);
    if (!file_result.ok()) {
        state.SkipWithError(("Failed to open: " + file_path).c_str());
        return;
    }
    auto raw_file = *file_result;

    // Record file size for throughput reporting (bytes/s in benchmark output)
    auto size_result = raw_file->GetSize();
    if (!size_result.ok()) {
        state.SkipWithError("Failed to stat file size");
        return;
    }
    const int64_t file_size = *size_result;

    // --- Parquet reader properties (enables buffered column reads) --------
    parquet::ReaderProperties parquet_props = parquet::default_reader_properties();
    parquet_props.enable_buffered_stream();
    parquet_props.set_buffer_size(IO_BUFFER_BYTES);

    // --- Arrow reader properties -----------------------------------------
    parquet::ArrowReaderProperties arrow_props =
        parquet::default_arrow_reader_properties();
    arrow_props.set_use_threads(use_threads);

    // --- Build reader once (ReadTable re-seeks per call) -----------------
    parquet::arrow::FileReaderBuilder builder;
    if (!builder.Open(raw_file, parquet_props).ok()) {
        state.SkipWithError("FileReaderBuilder::Open failed");
        return;
    }
    builder.properties(arrow_props);

    auto reader_result = builder.Build();
    if (!reader_result.ok()) {
        state.SkipWithError("FileReaderBuilder::Build failed");
        return;
    }
    auto reader = std::move(*reader_result);

    // Report the actual thread count visible to Arrow so results are self-documenting
    const int num_threads = use_threads
        ? static_cast<int>(std::thread::hardware_concurrency())
        : 1;
    state.counters["threads"] = static_cast<double>(num_threads);

    // --- Benchmark loop --------------------------------------------------
    for (auto _ : state) {
        std::shared_ptr<arrow::Table> table;
        auto st = reader->ReadTable(&table);
        if (!st.ok()) {
            state.SkipWithError(st.ToString().c_str());
            break;
        }
        // Prevent the compiler from eliding the read; pin the data pointer,
        // not the shared_ptr wrapper, so the underlying buffer can't be DCE'd
        benchmark::DoNotOptimize(table.get());
        benchmark::ClobberMemory();
    }

    // SetBytesProcessed lets Google Benchmark report GB/s alongside ms,
    // making the list vs struct comparison size-normalised
    state.SetBytesProcessed(state.iterations() * file_size);
}

// =========================================================
// Benchmark Registrations
// =========================================================
static void BM_List_SingleThread(benchmark::State& state) {
    BM_ParquetRead(state, "list_parquet.parquet", /*use_threads=*/false);
}
BENCHMARK(BM_List_SingleThread)
    ->Unit(benchmark::kMillisecond)
    ->MeasureProcessCPUTime()
    ->Iterations(10);

static void BM_List_MultiThread(benchmark::State& state) {
    BM_ParquetRead(state, "list_parquet.parquet", /*use_threads=*/true);
}
BENCHMARK(BM_List_MultiThread)
    ->Unit(benchmark::kMillisecond)
    ->MeasureProcessCPUTime()
    ->Iterations(10);

static void BM_Struct_SingleThread(benchmark::State& state) {
    BM_ParquetRead(state, "nested_parquet.parquet", /*use_threads=*/false);
}
BENCHMARK(BM_Struct_SingleThread)
    ->Unit(benchmark::kMillisecond)
    ->MeasureProcessCPUTime()
    ->Iterations(10);

static void BM_Struct_MultiThread(benchmark::State& state) {
    BM_ParquetRead(state, "nested_parquet.parquet", /*use_threads=*/true);
}
BENCHMARK(BM_Struct_MultiThread)
    ->Unit(benchmark::kMillisecond)
    ->MeasureProcessCPUTime()
    ->Iterations(10);

// =========================================================
// Main
// =========================================================
int main(int argc, char** argv)
{
    // Only regenerate when files are absent — generation dominates wall time
    // (~1.6 billion values). Delete the files manually to force regeneration.
    const bool files_exist =
        std::filesystem::exists("list_parquet.parquet") &&
        std::filesystem::exists("nested_parquet.parquet");

    if (!files_exist) {
        std::cout << "Generating test data ("
                  << GENERATE_NUM_ROWS << " rows x "
                  << GENERATE_LIST_LENGTH << " items/list) ...\n";

        auto st = GenerateParquetFiles(GENERATE_NUM_ROWS, GENERATE_LIST_LENGTH);
        if (!st.ok()) {
            std::cerr << "Fatal during generation: " << st.ToString() << "\n";
            return 1;
        }
        std::cout << "Generation complete.\n\n";
    } else {
        std::cout << "Parquet files already exist — skipping generation.\n"
                  << "Delete list_parquet.parquet / nested_parquet.parquet to regenerate.\n\n";
    }

    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}