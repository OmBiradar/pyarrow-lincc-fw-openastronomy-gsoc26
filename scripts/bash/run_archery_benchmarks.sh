git clone https://github.com/OmBiradar/arrow.git
cd arrow
git checkout parquet-reader-multithreaded-for-list-struct
python -m venv .venv
source ./.venv/bin/activate
pip install -e "dev/archery[all]"

archery benchmark run --suit-filter="^parquet-arrow-reader-writer" --benchmark-filter="Read.*Struct" 2>&1 | tee ../optimized.log

git checkout main
archery benchmark run --suit-filter="^parquet-arrow-reader-writer" --benchmark-filter="Read.*Struct" 2>&1 | tee ../main.log

cd ..
rm -rf arrow
