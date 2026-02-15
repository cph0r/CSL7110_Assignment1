# CSL7110 Assignment 1: MapReduce and Apache Spark

**Course:** CSL7110 - Big Data Frameworks  
**Roll Number:** M25DE1021  
**Total Marks:** 120 (10 marks per question × 12 questions)

## Project Structure

```
CSL7110_Assignment1/
├── README.md                   # This file
├── hadoop/
│   └── WordCount.java          # Q1-Q9: Hadoop MapReduce WordCount
├── spark/
│   ├── q10_metadata.py         # Q10: Book Metadata Extraction & Analysis
│   ├── q11_tfidf.py            # Q11: TF-IDF & Book Similarity
│   └── q12_influence.py        # Q12: Author Influence Network
├── data/                       # Dataset (not in repo - see setup)
├── outputs/                    # Output files and screenshots
├── scripts/
│   ├── compile_wordcount.sh    # Compile WordCount.java
│   └── run_experiments.sh      # Run Q9 split.maxsize experiments
└── .gitignore
```

## Prerequisites

- **Java JDK 8+** (for Hadoop)
- **Apache Hadoop 3.3.6** - [Installation Guide](https://medium.com/@abhikdey06/apache-hadoop-3-3-6-installation-on-ubuntu-22-04-14516bceec85)
- **Apache Spark 3.x** - [Download](https://spark.apache.org/downloads.html)
- **Python 3.8+** with PySpark
- **Linux-based OS** (recommended: Ubuntu 22.04)

## Setup

### 1. Install Hadoop

Follow the [Hadoop installation blog](https://medium.com/@abhikdey06/apache-hadoop-3-3-6-installation-on-ubuntu-22-04-14516bceec85) for single-node cluster setup on Ubuntu 22.04.

### 2. Install Spark

```bash
# Download Spark
wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar xvf spark-3.5.0-bin-hadoop3.tgz
sudo mv spark-3.5.0-bin-hadoop3 /opt/spark

# Add to PATH (in ~/.bashrc)
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Install PySpark
pip install pyspark
```

### 3. Download Dataset

Download the Project Gutenberg dataset (D184MB) and extract it into the `data/` directory.

```bash
# Extract the dataset
unzip <dataset_file>.zip -d data/
```

## Running the Code

### Hadoop MapReduce (Q1-Q9)

```bash
# Compile WordCount.java
cd hadoop/
./scripts/compile_wordcount.sh

# Copy data to HDFS
hadoop fs -mkdir -p /user/iitj/
hadoop fs -copyFromLocal ../data/200.txt /user/iitj/200.txt

# Run WordCount
hadoop jar WordCount.jar WordCount /user/iitj/200.txt output/

# Get merged output
hadoop fs -getmerge output/ output.txt
cat output.txt

# Run with split.maxsize (Q9)
hadoop jar WordCount.jar WordCount /user/iitj/200.txt output_split/ 67108864
```

### PySpark (Q10-Q12)

```bash
# Q10: Metadata Extraction
spark-submit spark/q10_metadata.py data/

# Q11: TF-IDF and Book Similarity
spark-submit spark/q11_tfidf.py data/ 10.txt

# Q12: Author Influence Network (with 5-year window)
spark-submit spark/q12_influence.py data/ 5
```

## Questions Summary

| # | Topic | Type | File |
|---|-------|------|------|
| Q1 | Run WordCount example | Hadoop | `hadoop/WordCount.java` |
| Q2 | Map phase output pairs & types | Theory | Report PDF |
| Q3 | Reduce phase input pairs & types | Theory | Report PDF |
| Q4 | Hadoop data types in WordCount | Hadoop | `hadoop/WordCount.java` |
| Q5 | Map function implementation | Hadoop | `hadoop/WordCount.java` |
| Q6 | Reduce function implementation | Hadoop | `hadoop/WordCount.java` |
| Q7 | Run on 200.txt dataset | Hadoop | Report PDF |
| Q8 | HDFS replication factor | Theory | Report PDF |
| Q9 | Execution time & split.maxsize | Hadoop | `hadoop/WordCount.java` |
| Q10 | Book Metadata Extraction | PySpark | `spark/q10_metadata.py` |
| Q11 | TF-IDF & Book Similarity | PySpark | `spark/q11_tfidf.py` |
| Q12 | Author Influence Network | PySpark | `spark/q12_influence.py` |

## References

1. [Apache Hadoop Installation Guide](https://medium.com/@abhikdey06/apache-hadoop-3-3-6-installation-on-ubuntu-22-04-14516bceec85)
2. [PySpark Tutorial](https://github.com/coder2j/pyspark-tutorial/tree/main)
3. [Apache Hadoop MapReduce Tutorial](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
4. [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
5. [HDFS Commands Guide](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html)
