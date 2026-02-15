"""
Q12: Author Influence Network
CSL7110 Assignment 1 - Apache Spark

This script:
1. Extracts author and release_date from Project Gutenberg book texts
2. Constructs an influence network: author_a -> author_b if author_a published
   a book within X years before author_b
3. Calculates in-degree and out-degree for each author
4. Identifies top 5 authors by in-degree and out-degree

Usage:
    spark-submit q12_influence.py <path_to_books_directory> [time_window_years]

Example:
    spark-submit q12_influence.py ../data/ 5
"""

import os
import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf, col, count, desc, asc, abs as spark_abs, when, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)


def extract_author(text):
    """
    Extract the author name from Project Gutenberg header.
    Tries multiple patterns to handle different header formats.
    """
    if text is None:
        return None

    # Try "Author:" line first
    match = re.search(r'Author:\s*(.+)', text, re.IGNORECASE)
    if match:
        author = match.group(1).strip()
        # Clean up: remove dates/birth-death years in parentheses
        author = re.sub(r'\s*\(.*?\)\s*', '', author).strip()
        if author and author.lower() != "various" and len(author) > 1:
            return author

    # Try "by <author>" pattern from title line
    match = re.search(r'(?:Project Gutenberg[^\n]*\n.*?)?by\s+([A-Z][a-zA-Z\s\.\,]+)',
                      text[:2000])
    if match:
        author = match.group(1).strip().rstrip(',').strip()
        if author and len(author) > 3 and len(author) < 100:
            return author

    return None


def extract_release_year(text):
    """
    Extract the release year from Project Gutenberg header.
    Returns year as integer.
    """
    if text is None:
        return None

    # Try "Release date" pattern
    match = re.search(r'Release [Dd]ate:\s*.*?(\d{4})', text)
    if match:
        return int(match.group(1))

    # Try "Posting Date" pattern
    match = re.search(r'Posting Date:\s*.*?(\d{4})', text)
    if match:
        return int(match.group(1))

    return None


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


def main():
    if len(sys.argv) < 2:
        print("Usage: spark-submit q12_influence.py <path_to_books_directory> "
              "[time_window_years]")
        sys.exit(1)

    data_dir = sys.argv[1]
    time_window = int(sys.argv[2]) if len(sys.argv) >= 3 else 5

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Q12_Author_Influence_Network") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Load books
    books_df = create_books_df(spark, data_dir)
    total_books = books_df.count()
    print(f"\nLoaded {total_books} books")

    # ==========================================
    # 1. Preprocessing - Extract Author & Year
    # ==========================================
    print("\n" + "=" * 60)
    print("1. PREPROCESSING - EXTRACT AUTHOR AND RELEASE YEAR")
    print("=" * 60)

    extract_author_udf = udf(extract_author, StringType())
    extract_year_udf = udf(extract_release_year, IntegerType())

    # Extract metadata
    author_df = books_df.select(
        col("file_name"),
        extract_author_udf(col("text")).alias("author"),
        extract_year_udf(col("text")).alias("release_year")
    ).filter(
        col("author").isNotNull() & col("release_year").isNotNull()
    )

    # Use earliest release year per author (in case of multiple books)
    # Keep all books for the network analysis
    print(f"\nBooks with valid author and year: {author_df.count()}")
    print("\nSample extracted authors and release years:")
    author_df.orderBy("release_year").show(20, truncate=40)

    # Unique authors
    unique_authors = author_df.select("author").distinct().count()
    print(f"Unique authors extracted: {unique_authors}")

    # ==========================================
    # 2. Influence Network Construction
    # ==========================================
    print("\n" + "=" * 60)
    print(f"2. INFLUENCE NETWORK (Time Window: {time_window} years)")
    print("=" * 60)

    # We define influence as:
    # author_a "influences" author_b if:
    #   - author_a published a book BEFORE author_b (or same year)
    #   - The time difference is within X years
    #   - author_a != author_b

    # Self-join to create potential influence pairs
    authors_a = author_df.select(
        col("author").alias("author_a"),
        col("release_year").alias("year_a")
    ).distinct()

    authors_b = author_df.select(
        col("author").alias("author_b"),
        col("release_year").alias("year_b")
    ).distinct()

    # Create edges: author_a influences author_b
    # Condition: 0 < (year_b - year_a) <= time_window
    # (author_a published first, within the time window)
    edges_df = authors_a.crossJoin(authors_b).filter(
        (col("author_a") != col("author_b")) &
        (col("year_b") - col("year_a") > 0) &
        (col("year_b") - col("year_a") <= time_window)
    ).select(
        col("author_a"),
        col("author_b")
    ).distinct()

    edge_count = edges_df.count()
    print(f"\nTotal influence edges: {edge_count}")
    print("\nSample edges (author_a -> author_b means a potentially influenced b):")
    edges_df.show(20, truncate=30)

    # ==========================================
    # 3. Analysis - In-Degree and Out-Degree
    # ==========================================
    print("\n" + "=" * 60)
    print("3. DEGREE ANALYSIS")
    print("=" * 60)

    # Out-degree: number of authors that author_a potentially influenced
    out_degree = edges_df.groupBy("author_a") \
        .agg(count("*").alias("out_degree")) \
        .withColumnRenamed("author_a", "author")

    # In-degree: number of authors that potentially influenced author_b
    in_degree = edges_df.groupBy("author_b") \
        .agg(count("*").alias("in_degree")) \
        .withColumnRenamed("author_b", "author")

    # Print Top 5 by In-Degree
    print("\n--- Top 5 Authors by IN-DEGREE (most influenced by others) ---")
    in_degree.orderBy(desc("in_degree")).show(5, truncate=40)

    # Print Top 5 by Out-Degree
    print("\n--- Top 5 Authors by OUT-DEGREE (influenced most others) ---")
    out_degree.orderBy(desc("out_degree")).show(5, truncate=40)

    # Combined view
    print("\n--- Combined Degree Table (Top 20) ---")
    combined = out_degree.join(in_degree, "author", "outer") \
        .fillna(0) \
        .withColumn("total_degree", col("in_degree") + col("out_degree")) \
        .orderBy(desc("total_degree"))

    combined.show(20, truncate=40)

    # ==========================================
    # Network Statistics
    # ==========================================
    print("\n" + "=" * 60)
    print("NETWORK STATISTICS")
    print("=" * 60)

    print(f"Time window (X): {time_window} years")
    print(f"Total unique authors in network: {unique_authors}")
    print(f"Total edges (influence relationships): {edge_count}")
    if unique_authors > 0:
        density = edge_count / (unique_authors * (unique_authors - 1)) \
            if unique_authors > 1 else 0
        print(f"Network density: {density:.4f}")

    avg_in = in_degree.select("in_degree").agg({"in_degree": "avg"}).first()[0]
    avg_out = out_degree.select("out_degree").agg({"out_degree": "avg"}).first()[0]
    print(f"Average in-degree: {avg_in:.2f}" if avg_in else "Average in-degree: 0")
    print(f"Average out-degree: {avg_out:.2f}" if avg_out else "Average out-degree: 0")

    # ==========================================
    # Discussion
    # ==========================================
    print("\n" + "=" * 60)
    print("DISCUSSION")
    print("=" * 60)
    print(f"""
Network Representation Choice:
  We used a DataFrame representation for the influence network, where each row
  represents an edge (author_a, author_b). This was chosen because:

  Advantages:
  1. DataFrames support SQL-like aggregations (groupBy, count) which make
     degree calculations straightforward and efficient.
  2. Spark's Catalyst optimizer can optimize DataFrame operations automatically.
  3. Easy to filter, join, and transform the network data.
  4. Integrates well with other Spark DataFrames (e.g., joining with metadata).

  Disadvantages:
  1. No native graph operations (e.g., shortest path, PageRank) — would need
     GraphX or GraphFrames library for those.
  2. The cross-join for network construction is expensive for large datasets.
  3. Self-joins can cause significant shuffling overhead.

Effect of Time Window (X):
  - Smaller X (e.g., 2 years): Creates a sparser network with fewer edges,
    capturing only very close temporal relationships. May miss legitimate
    influences.
  - Larger X (e.g., 10-20 years): Creates a denser network with many more
    edges, potentially capturing broader cultural influences but also
    introducing many spurious connections.
  - The current X={time_window} years provides a moderate balance.

Limitations of Simplified Influence:
  1. Temporal proximity ≠ actual influence. Just because two books were
     published near each other doesn't mean one influenced the other.
  2. No content analysis — we don't check if books share themes or ideas.
  3. Ignores genre, geography, and language barriers.
  4. Self-publication dates on Project Gutenberg are _digitization_ dates,
     not original publication dates, which skews the analysis.

Scalability for Millions of Books/Authors:
  1. The cross-join is O(n²) which is prohibitive; we could partition by
     time window and only compare within relevant partitions.
  2. Use approximate methods or sampling strategies.
  3. Leverage GraphFrames for distributed graph algorithms.
  4. Use broadcast joins for smaller dimension tables.
  5. Implement incremental network updates instead of full recomputation.
  6. Consider using adjacency list representation instead of edge list for
     better memory efficiency.
    """)

    spark.stop()


if __name__ == "__main__":
    main()
