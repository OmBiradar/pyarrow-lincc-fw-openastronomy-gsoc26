#include <arrow/api.h>
#include <iostream>

// Standard Arrow practice: wrap logic in a function that returns arrow::Status
arrow::Status RunMain() {
  arrow::Int64Builder builder;

  // Use the core Arrow macro
  ARROW_RETURN_NOT_OK(builder.Append(10));
  ARROW_RETURN_NOT_OK(builder.Append(20));
  ARROW_RETURN_NOT_OK(builder.Append(30));

  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));

  std::cout << "Successfully built Arrow Array!" << std::endl;
  std::cout << "Array length: " << array->length() << std::endl;
  std::cout << "Array contents: " << array->ToString() << std::endl;

  return arrow::Status::OK();
}

int main() {
  arrow::Status st = RunMain();
  if (!st.ok()) {
    std::cerr << "Arrow Error: " << st.ToString() << std::endl;
    return 1;
  }
  return 0;
}
