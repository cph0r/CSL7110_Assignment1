# 🐘 vs ✨ Big Data Showdown: Hadoop & Spark

**Assignment 1: CSL7110 - Big Data Frameworks**  
*Prepare for glory (and execution logs).*

[![Status](https://img.shields.io/badge/Status-Completed-success)]() [![Framework](https://img.shields.io/badge/Hadoop-MapReduce-yellow)]() [![Framework](https://img.shields.io/badge/Spark-PySpark-orange)]()

Welcome to the digital arena where we count words until our fingers bleed and analyze influence networks until we realize everything is connected! This repo contains my submission for Assignment 1, featuring a classic duel between the rigorous **Hadoop MapReduce** and the lightning-fast **Apache Spark**.

---

## 🚀 Creating the Chaos (Setup)

You'll need a few magical ingredients to replicate these experiments:

- **☕ Java 8+**: Because Hadoop runs on coffee.
- **🐘 Hadoop 3.3.6**: The heavy lifter.
- **✨ Spark 3.x**: The speed demon.
- **🐍 Python 3.8+**: For when Java is too verbose.

### The Easy Way (Docker / Simulation) 🐳

Don't have a cluster lying around? I've got you covered.

```bash
# Clone the repo
git clone https://github.com/cph0r/CSL7110_Assignment1.git
cd CSL7110_Assignment1

# Install simulation dependencies
pip install -r requirements.txt

# Run the simulation (generates all outputs without a cluster!)
python3 scripts/simulator.py
```

---

## 📂 The Arsenal (Project Structure)

- `hadoop/WordCount.java` 📜 - The OG word counter. Strict, verbose, gets the job done.
- `spark/q10_metadata.py` 📚 - Extracting metadata like a librarian on caffeine.
- `spark/q11_tfidf.py` 🔍 - Finding "similar" books. (Spoiler: The Bible is similar to... The Bible).
- `spark/q12_influence.py` 🕸️ - Who influenced whom? A time-traveling influence network.
- `data/` 📦 - Where the books live (Project Gutenberg dataset).
- `outputs/` 🖼️ - Proof that it actually works.
- `M25DE1021_CSL7110_Assignment1.pdf` 📄 - The **FINAL REPORT**. Read it and weep (tears of joy).

---

## 🛠️ Execution Instructions

### Round 1: Hadoop MapReduce 🥊

How to count words in `200.txt` like it's 2006:

```bash
# 1. Compile the code (requires Hadoop classpath)
./scripts/compile_wordcount.sh

# 2. Upload data to HDFS
hadoop fs -copyFromLocal data/200.txt /user/iitj/200.txt

# 3. RUN!
hadoop jar WordCount.jar WordCount /user/iitj/200.txt output/

# 4. Check results
hadoop fs -cat output/part-r-00000 | head
```

### Round 2: PySpark ⚡

Analyzing 400+ books in seconds:

```bash
# Q10: Metadata Extraction
spark-submit spark/q10_metadata.py data/

# Q11: TF-IDF & Book Similarity
spark-submit spark/q11_tfidf.py data/ 10.txt

# Q12: Author Influence Network
spark-submit spark/q12_influence.py data/ 5
```

---

## 🧪 Experiments

We ran experiments on execution time vs split size (Q9).
**Result:** Smaller splits = more parallelism = more overhead. It's a balance, like everything in life.

---

## 📜 Credits

- **Author:** M25DE1021 (The one writing this)
- **Dataset:** Project Gutenberg (Thanks for the free books!)
- **Inspiration:** Caffeine and deadlines.

---

*> "Data is the new oil. HDFS is the pipeline. Spark is the match."* 🔥
