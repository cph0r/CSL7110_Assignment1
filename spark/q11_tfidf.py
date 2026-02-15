"""
Q11: TF-IDF and Book Similarity
CSL7110 Assignment 1 - Apache Spark

This script:
1. Preprocesses book texts (clean headers/footers, lowercase, remove punctuation,
   tokenize, remove stop words)
2. Calculates TF, IDF, and TF-IDF scores
3. Computes cosine similarity between all pairs of books
4. Finds top 5 most similar books for a given book

Usage:
    spark-submit q11_tfidf.py <path_to_books_directory> [target_book]

Example:
    spark-submit q11_tfidf.py ../data/ 10.txt
"""

import os
import sys
import re
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf, col, explode, split, lower, regexp_replace,
    count, log, sum as spark_sum, sqrt, desc, collect_list,
    struct, size, lit, array, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, FloatType,
    DoubleType, IntegerType
)
from pyspark.sql.window import Window


# Common English stop words
STOP_WORDS = set([
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "as", "is", "was", "are", "were", "be",
    "been", "being", "have", "has", "had", "do", "does", "did", "will",
    "would", "could", "should", "may", "might", "shall", "can", "need",
    "dare", "ought", "used", "it", "its", "i", "me", "my", "myself", "we",
    "our", "ours", "ourselves", "you", "your", "yours", "yourself",
    "yourselves", "he", "him", "his", "himself", "she", "her", "hers",
    "herself", "it", "itself", "they", "them", "their", "theirs",
    "themselves", "what", "which", "who", "whom", "this", "that", "these",
    "those", "am", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "having", "do", "does", "did", "doing", "a",
    "an", "the", "and", "but", "if", "or", "because", "as", "until",
    "while", "of", "at", "by", "for", "with", "about", "against",
    "between", "through", "during", "before", "after", "above", "below",
    "to", "from", "up", "down", "in", "out", "on", "off", "over", "under",
    "again", "further", "then", "once", "here", "there", "when", "where",
    "why", "how", "all", "both", "each", "few", "more", "most", "other",
    "some", "such", "no", "nor", "not", "only", "own", "same", "so",
    "than", "too", "very", "s", "t", "just", "don", "now"
])

# Broadcast variable will be set in main
stop_words_broadcast = None


def create_books_df(spark, data_dir):
    """Load all text files into a DataFrame with (file_name, text) schema."""
    books_data = []
    for filename in os.listdir(data_dir):
        if filename.endswith(".txt"):
            filepath = os.path.join(data_dir, filename)
            try:
                with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                    text = f.read()
                books_data.append((filename, text))
            except Exception as e:
                print(f"Warning: Could not read {filename}: {e}")

    schema = StructType([
        StructField("file_name", StringType(), True),
        StructField("text", StringType(), True)
    ])

    return spark.createDataFrame(books_data, schema=schema)


def preprocess_text(text):
    """
    Clean text for TF-IDF processing:
    1. Remove Project Gutenberg header (before '*** START OF') and
       footer (after '*** END OF')
    2. Convert to lowercase
    3. Remove punctuation
    4. Tokenize into words
    5. Remove stop words and short words
    """
    if text is None:
        return []

    # Remove Gutenberg header
    start_markers = [
        "*** START OF THIS PROJECT GUTENBERG",
        "*** START OF THE PROJECT GUTENBERG",
        "***START OF THIS PROJECT GUTENBERG",
        "***START OF THE PROJECT GUTENBERG"
    ]
    for marker in start_markers:
        idx = text.upper().find(marker.upper())
        if idx != -1:
            # Skip past the marker line
            newline_idx = text.find("\n", idx)
            if newline_idx != -1:
                text = text[newline_idx + 1:]
            break

    # Remove Gutenberg footer
    end_markers = [
        "*** END OF THIS PROJECT GUTENBERG",
        "*** END OF THE PROJECT GUTENBERG",
        "***END OF THIS PROJECT GUTENBERG",
        "***END OF THE PROJECT GUTENBERG",
        "End of the Project Gutenberg",
        "End of Project Gutenberg"
    ]
    for marker in end_markers:
        idx = text.upper().find(marker.upper())
        if idx != -1:
            text = text[:idx]
            break

    # Convert to lowercase
    text = text.lower()

    # Remove punctuation and numbers, keep only letters and spaces
    text = re.sub(r'[^a-z\s]', ' ', text)

    # Tokenize and filter
    words = text.split()
    words = [w for w in words if len(w) > 2 and w not in STOP_WORDS]

    return words


def main():
    if len(sys.argv) < 2:
        print("Usage: spark-submit q11_tfidf.py <path_to_books_directory> [target_book]")
        sys.exit(1)

    data_dir = sys.argv[1]
    target_book = sys.argv[2] if len(sys.argv) >= 3 else "10.txt"

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Q11_TF_IDF_Book_Similarity") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Broadcast stop words
    global stop_words_broadcast
    stop_words_broadcast = spark.sparkContext.broadcast(STOP_WORDS)

    # Load books
    books_df = create_books_df(spark, data_dir)
    total_books = books_df.count()
    print(f"\nLoaded {total_books} books")

    # ==========================================
    # 1. Preprocessing
    # ==========================================
    print("\n" + "=" * 60)
    print("1. PREPROCESSING")
    print("=" * 60)

    preprocess_udf = udf(preprocess_text, ArrayType(StringType()))

    # Create DataFrame with preprocessed words
    words_df = books_df.select(
        col("file_name"),
        explode(preprocess_udf(col("text"))).alias("word")
    )

    print(f"Total word tokens after preprocessing: {words_df.count()}")
    print("\nSample words per book:")
    words_df.groupBy("file_name").count().orderBy("file_name").show(10)

    # ==========================================
    # 2. TF-IDF Calculation
    # ==========================================
    print("\n" + "=" * 60)
    print("2. TF-IDF CALCULATION")
    print("=" * 60)

    # --- Term Frequency (TF) ---
    # TF(t, d) = (number of times term t appears in document d) / (total terms in d)
    print("\n--- Calculating Term Frequency (TF) ---")

    # Count occurrences of each word in each book
    word_counts = words_df.groupBy("file_name", "word") \
        .agg(count("*").alias("word_count"))

    # Count total words in each book
    total_words_per_book = words_df.groupBy("file_name") \
        .agg(count("*").alias("total_words"))

    # Calculate TF
    tf_df = word_counts.join(total_words_per_book, "file_name") \
        .withColumn("tf", col("word_count") / col("total_words"))

    print("Sample TF values:")
    tf_df.orderBy(desc("tf")).show(10)

    # --- Inverse Document Frequency (IDF) ---
    # IDF(t) = log(N / df(t)) where N = total docs, df(t) = docs containing term t
    print("\n--- Calculating Inverse Document Frequency (IDF) ---")

    # Document frequency: number of documents containing each word
    df_counts = word_counts.groupBy("word") \
        .agg(count("file_name").alias("doc_freq"))

    # Calculate IDF
    idf_df = df_counts.withColumn(
        "idf", log(lit(total_books) / col("doc_freq"))
    )

    print("Sample IDF values (highest IDF = most unique words):")
    idf_df.orderBy(desc("idf")).show(10)

    print("Sample IDF values (lowest IDF = most common words):")
    idf_df.orderBy("idf").show(10)

    # --- TF-IDF ---
    # TF-IDF(t, d) = TF(t, d) * IDF(t)
    print("\n--- Calculating TF-IDF ---")

    tfidf_df = tf_df.join(idf_df, "word") \
        .withColumn("tfidf", col("tf") * col("idf")) \
        .select("file_name", "word", "tf", "idf", "tfidf")

    print("Sample TF-IDF values:")
    tfidf_df.orderBy(desc("tfidf")).show(20)

    # ==========================================
    # 3. Book Similarity (Cosine Similarity)
    # ==========================================
    print("\n" + "=" * 60)
    print("3. BOOK SIMILARITY (COSINE SIMILARITY)")
    print("=" * 60)

    # For cosine similarity, we need to compute:
    # cos(A, B) = (A · B) / (||A|| * ||B||)
    # where A and B are TF-IDF vectors of two books

    # Compute dot product for all book pairs sharing common words
    tfidf_a = tfidf_df.select(
        col("file_name").alias("book_a"),
        col("word"),
        col("tfidf").alias("tfidf_a")
    )

    tfidf_b = tfidf_df.select(
        col("file_name").alias("book_b"),
        col("word"),
        col("tfidf").alias("tfidf_b")
    )

    # Self-join on common words
    dot_products = tfidf_a.join(tfidf_b, "word") \
        .filter(col("book_a") < col("book_b")) \
        .groupBy("book_a", "book_b") \
        .agg(spark_sum(col("tfidf_a") * col("tfidf_b")).alias("dot_product"))

    # Compute magnitudes (L2 norms) of each book's TF-IDF vector
    magnitudes = tfidf_df.groupBy("file_name") \
        .agg(sqrt(spark_sum(col("tfidf") * col("tfidf"))).alias("magnitude"))

    mag_a = magnitudes.select(
        col("file_name").alias("book_a"),
        col("magnitude").alias("magnitude_a")
    )

    mag_b = magnitudes.select(
        col("file_name").alias("book_b"),
        col("magnitude").alias("magnitude_b")
    )

    # Compute cosine similarity
    cosine_sim = dot_products \
        .join(mag_a, "book_a") \
        .join(mag_b, "book_b") \
        .withColumn(
            "cosine_similarity",
            when(
                (col("magnitude_a") > 0) & (col("magnitude_b") > 0),
                col("dot_product") / (col("magnitude_a") * col("magnitude_b"))
            ).otherwise(0.0)
        ) \
        .select("book_a", "book_b", "cosine_similarity")

    print(f"\nComputed cosine similarity for {cosine_sim.count()} book pairs")
    print("\nTop 20 most similar book pairs:")
    cosine_sim.orderBy(desc("cosine_similarity")).show(20)

    # ==========================================
    # 4. Top 5 similar books for target book
    # ==========================================
    print("\n" + "=" * 60)
    print(f"4. TOP 5 MOST SIMILAR BOOKS TO '{target_book}'")
    print("=" * 60)

    # Get similarities where target book is involved (either as book_a or book_b)
    target_sim = cosine_sim.filter(
        (col("book_a") == target_book) | (col("book_b") == target_book)
    ).select(
        when(col("book_a") == target_book, col("book_b"))
        .otherwise(col("book_a")).alias("similar_book"),
        col("cosine_similarity")
    ).orderBy(desc("cosine_similarity"))

    print(f"\nTop 5 books most similar to '{target_book}':")
    target_sim.show(5)

    # ==========================================
    # Discussion
    # ==========================================
    print("\n" + "=" * 60)
    print("DISCUSSION")
    print("=" * 60)
    print("""
TF (Term Frequency):
  TF(t, d) = (count of term t in document d) / (total terms in document d)
  Measures how frequently a term appears in a document. Higher TF means the
  term is more important to that specific document.

IDF (Inverse Document Frequency):
  IDF(t) = log(N / df(t))
  where N = total number of documents, df(t) = number of documents containing term t.
  Measures how unique/rare a term is across the entire corpus. Common words
  like "the" get low IDF, while rare/distinctive words get high IDF.

Why TF-IDF is Useful:
  TF-IDF = TF * IDF. This weighting scheme balances term frequency with
  uniqueness. Words that are frequent in a document BUT rare across the corpus
  get high scores — these are the words that best characterize a document.
  Common words that appear everywhere get downweighted by IDF.

Cosine Similarity:
  cos(A, B) = (A · B) / (||A|| * ||B||)
  where A and B are TF-IDF vectors. The result ranges from 0 (no similarity)
  to 1 (identical documents). It measures the angle between two vectors —
  documents pointing in similar directions in word-space are considered similar.

Why Cosine Similarity for TF-IDF:
  Cosine similarity is ideal for TF-IDF vectors because:
  1. It is length-independent — a long and short book about the same topic
     will still show high similarity.
  2. It handles high-dimensional sparse vectors efficiently.
  3. It focuses on the direction (topic composition) rather than magnitude
     (document length).

Scalability Challenges:
  Pairwise cosine similarity requires O(n²) comparisons for n documents,
  which becomes infeasible for large corpora. Challenges include:
  1. Memory: Storing all pairwise similarities for millions of docs
  2. Computation: The number of pairs grows quadratically
  3. Shuffling: The self-join on words causes massive data shuffling

How Spark Helps:
  1. Distributed computation across multiple nodes
  2. Lazy evaluation and query optimization via Catalyst
  3. In-memory caching of intermediate results
  4. Partitioning strategies to minimize shuffling
  5. Approximate methods (e.g., LSH — Locality-Sensitive Hashing) in MLlib
     for near-neighbor search without computing all pairs
    """)

    spark.stop()


if __name__ == "__main__":
    main()
