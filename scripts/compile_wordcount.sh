#!/bin/bash
# Compile WordCount.java for Hadoop
# Usage: ./compile_wordcount.sh

set -e

echo "=== Compiling WordCount.java ==="

# Get Hadoop classpath
HADOOP_CP=$(hadoop classpath)

# Compile
javac -classpath "$HADOOP_CP" WordCount.java

# Create JAR
jar cf WordCount.jar WordCount*.class

echo "=== Compilation successful ==="
echo "JAR file created: WordCount.jar"
echo ""
echo "To run:"
echo "  hadoop jar WordCount.jar WordCount <input_path> <output_path> [split_maxsize]"
