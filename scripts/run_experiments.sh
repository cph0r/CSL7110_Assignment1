#!/bin/bash
# Q9: Run WordCount with different split.maxsize values to measure performance
# Usage: ./run_experiments.sh <input_hdfs_path>

set -e

INPUT_PATH=${1:-"/user/iitj/200.txt"}
JAR_PATH="../hadoop/WordCount.jar"

echo "=== Q9: Split MaxSize Experiments ==="
echo "Input: $INPUT_PATH"
echo ""

# Different split sizes to test (in bytes)
# Default HDFS block size is 128MB = 134217728 bytes
SPLIT_SIZES=(
    "33554432"    # 32 MB
    "67108864"    # 64 MB
    "134217728"   # 128 MB (default block size)
    "268435456"   # 256 MB
)

for SIZE in "${SPLIT_SIZES[@]}"; do
    SIZE_MB=$((SIZE / 1048576))
    OUTPUT_DIR="output_split_${SIZE_MB}mb"

    echo "-------------------------------------------"
    echo "Running with split.maxsize = $SIZE bytes ($SIZE_MB MB)"
    echo "-------------------------------------------"

    # Remove previous output
    hadoop fs -rm -r -f "$OUTPUT_DIR" 2>/dev/null || true

    # Run WordCount with split size
    hadoop jar "$JAR_PATH" WordCount "$INPUT_PATH" "$OUTPUT_DIR" "$SIZE"

    echo ""
done

echo "=== All experiments completed ==="
echo ""
echo "Observation: Smaller split sizes create more map tasks, which increases"
echo "parallelism but also increases overhead. Larger split sizes create fewer"
echo "map tasks, reducing overhead but potentially limiting parallelism."
echo "The optimal split size depends on the cluster configuration and data size."
