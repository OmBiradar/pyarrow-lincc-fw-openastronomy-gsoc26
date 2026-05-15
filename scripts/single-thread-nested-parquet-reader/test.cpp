#include <arrow/api.h>
#include <arrow/io/api.h>
#include <iostream>
#include <parquet/arrow/reader.h>

arrow::Status ReadParquetFile(const std::string &file_path)
{
  arrow::MemoryPool *pool = arrow::default_memory_pool();

  // 1. Open the input file
  std::shared_ptr<arrow::io::RandomAccessFile> input;
  ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(file_path));

  // // 2. Configure multi-threading properties
  // parquet::ArrowReaderProperties arrow_props =
  //     parquet::default_arrow_reader_properties();
  // arrow_props.set_use_threads(true);

  // 3. Use the FileReaderBuilder (Modern Arrow API)
  parquet::arrow::FileReaderBuilder builder;

  // Open the file with the builder
  ARROW_RETURN_NOT_OK(builder.Open(input));

  // Pass in your memory pool and properties
  // builder.memory_pool(pool);
  // builder.properties(arrow_props);

  // Build the reader
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  ARROW_ASSIGN_OR_RAISE(arrow_reader, builder.Build());

  // 4. Read the entire file as a single Arrow table
  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table));

  // Verify
  std::cout << "Successfully read Parquet file: " << file_path << "\n";
  std::cout << "Number of columns: " << table->num_columns() << "\n";
  std::cout << "Number of rows: " << table->num_rows() << "\n";

  return arrow::Status::OK();
}

int main()
{
  std::string file_path = "nested.parquet";
  arrow::Status st = ReadParquetFile(file_path);

  if (!st.ok())
  {
    std::cerr << "Arrow Error: " << st.ToString() << std::endl;
    return 1;
  }

  return 0;
}
