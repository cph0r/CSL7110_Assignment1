"""
Q10: Book Metadata Extraction and Analysis
CSL7110 Assignment 1 - Apache Spark

This script loads Project Gutenberg books into a Spark DataFrame and:
1. Extracts metadata (title, release_date, language, encoding) using regex
2. Calculates the number of books released each year
3. Finds the most common language
4. Determines the average length of book titles

Usage:
    spark-submit q10_metadata.py <path_to_books_directory>

Example:
    spark-submit q10_metadata.py ../data/
"""

import os
import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf, col, year, count, avg, length, desc, explode, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)


def create_books_df(spark, data_dir):
    """
    Load all text files from the data directory into a DataFrame
    with schema: (file_name: string, text: string)
    """
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

    books_df = spark.createDataFrame(books_data, schema=schema)
    print(f"\nLoaded {books_df.count()} books into DataFrame")
    return books_df


def extract_title(text):
    """
    Extract the title from Project Gutenberg header.

    Regex explanation:
    - r'Title:\s*(.+)' matches the line starting with "Title:" followed by
      optional whitespace, then captures everything after it (.+) as the title.
    - re.IGNORECASE makes the match case-insensitive.
    - .strip() removes any leading/trailing whitespace from the captured group.
    """
    if text is None:
        return None
    match = re.search(r'Title:\s*(.+)', text, re.IGNORECASE)
    return match.group(1).strip() if match else None


def extract_release_date(text):
    """
    Extract the release date from Project Gutenberg header.

    Regex explanation:
    - r'Release [Dd]ate:\s*(.+?)(?:\s*\[|$)' matches "Release date:" or
      "Release Date:" followed by optional whitespace, then captures the date
      string (.+?) non-greedily until it hits either a '[' character
      (which often follows the date in Gutenberg headers as [EBook #...])
      or end of line.
    - We also try an alternative pattern for older formats:
      r'Posting Date:\s*(.+?)(?:\s*\[|$)' for books that use "Posting Date".

    We then try to extract just the year using r'(\d{4})'.
    """
    if text is None:
        return None

    # Try "Release date" or "Release Date" first
    match = re.search(r'Release [Dd]ate:\s*(.+?)(?:\s*\[|$)', text, re.MULTILINE)

    # Fallback to "Posting Date"
    if not match:
        match = re.search(r'Posting Date:\s*(.+?)(?:\s*\[|$)', text, re.MULTILINE)

    if match:
        return match.group(1).strip()
    return None


def extract_year(date_str):
    """Extract a 4-digit year from a date string."""
    if date_str is None:
        return None
    match = re.search(r'(\d{4})', date_str)
    return int(match.group(1)) if match else None


def extract_language(text):
    """
    Extract the language from Project Gutenberg header.

    Regex explanation:
    - r'Language:\s*(.+)' matches the line starting with "Language:" followed by
      optional whitespace, then captures the rest of the line (.+) which contains
      the language name (e.g., "English", "French").
    - re.IGNORECASE handles case variations.
    """
    if text is None:
        return None
    match = re.search(r'Language:\s*(.+)', text, re.IGNORECASE)
    return match.group(1).strip() if match else None


def extract_encoding(text):
    """
    Extract the character encoding from Project Gutenberg header.

    Regex explanation:
    - r'Character set encoding:\s*(.+)' matches the line starting with
      "Character set encoding:" followed by optional whitespace and captures
      the encoding name (e.g., "UTF-8", "ASCII", "ISO-8859-1").
    - re.IGNORECASE handles various capitalizations.
    """
    if text is None:
        return None
    match = re.search(r'Character set encoding:\s*(.+)', text, re.IGNORECASE)
    return match.group(1).strip() if match else None


def main():
    if len(sys.argv) < 2:
        print("Usage: spark-submit q10_metadata.py <path_to_books_directory>")
        sys.exit(1)

    data_dir = sys.argv[1]

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Q10_Book_Metadata_Extraction") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Load books into DataFrame
    books_df = create_books_df(spark, data_dir)

    # Register UDFs for metadata extraction
    extract_title_udf = udf(extract_title, StringType())
    extract_release_date_udf = udf(extract_release_date, StringType())
    extract_year_udf = udf(extract_year, IntegerType())
    extract_language_udf = udf(extract_language, StringType())
    extract_encoding_udf = udf(extract_encoding, StringType())

    # ==========================================
    # 1. Metadata Extraction
    # ==========================================
    print("\n" + "=" * 60)
    print("1. METADATA EXTRACTION")
    print("=" * 60)

    metadata_df = books_df.select(
        col("file_name"),
        extract_title_udf(col("text")).alias("title"),
        extract_release_date_udf(col("text")).alias("release_date"),
        extract_language_udf(col("text")).alias("language"),
        extract_encoding_udf(col("text")).alias("encoding")
    )

    # Add year column for analysis
    metadata_df = metadata_df.withColumn(
        "release_year", extract_year_udf(col("release_date"))
    )

    print("\nSample metadata (first 20 rows):")
    metadata_df.show(20, truncate=40)

    # ==========================================
    # 2. Analysis: Books released each year
    # ==========================================
    print("\n" + "=" * 60)
    print("2. NUMBER OF BOOKS RELEASED EACH YEAR")
    print("=" * 60)

    books_per_year = metadata_df.filter(col("release_year").isNotNull()) \
        .groupBy("release_year") \
        .agg(count("*").alias("num_books")) \
        .orderBy("release_year")

    books_per_year.show(50)

    # ==========================================
    # 3. Analysis: Most common language
    # ==========================================
    print("\n" + "=" * 60)
    print("3. MOST COMMON LANGUAGE")
    print("=" * 60)

    language_counts = metadata_df.filter(col("language").isNotNull()) \
        .groupBy("language") \
        .agg(count("*").alias("num_books")) \
        .orderBy(desc("num_books"))

    language_counts.show(20)

    most_common = language_counts.first()
    if most_common:
        print(f"Most common language: {most_common['language']} "
              f"({most_common['num_books']} books)")

    # ==========================================
    # 4. Analysis: Average length of book titles
    # ==========================================
    print("\n" + "=" * 60)
    print("4. AVERAGE LENGTH OF BOOK TITLES")
    print("=" * 60)

    avg_title_length = metadata_df.filter(col("title").isNotNull()) \
        .select(avg(length(col("title"))).alias("avg_title_length"))

    avg_title_length.show()

    avg_len = avg_title_length.first()["avg_title_length"]
    print(f"Average title length: {avg_len:.2f} characters")

    # ==========================================
    # Summary Statistics
    # ==========================================
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    total_books = metadata_df.count()
    with_title = metadata_df.filter(col("title").isNotNull()).count()
    with_date = metadata_df.filter(col("release_date").isNotNull()).count()
    with_lang = metadata_df.filter(col("language").isNotNull()).count()
    with_enc = metadata_df.filter(col("encoding").isNotNull()).count()

    print(f"Total books: {total_books}")
    print(f"Books with title extracted: {with_title} ({100*with_title/total_books:.1f}%)")
    print(f"Books with release date extracted: {with_date} ({100*with_date/total_books:.1f}%)")
    print(f"Books with language extracted: {with_lang} ({100*with_lang/total_books:.1f}%)")
    print(f"Books with encoding extracted: {with_enc} ({100*with_enc/total_books:.1f}%)")

    # ==========================================
    # Discussion
    # ==========================================
    print("\n" + "=" * 60)
    print("DISCUSSION")
    print("=" * 60)
    print("""
Regex Explanation:
- Title:       r'Title:\\s*(.+)' - Matches "Title:" followed by the title text
- Release Date: r'Release [Dd]ate:\\s*(.+?)(?:\\s*\\[|$)' - Matches "Release date:"
                and captures text until '[' or end of line (non-greedy)
- Language:    r'Language:\\s*(.+)' - Matches "Language:" followed by the language
- Encoding:    r'Character set encoding:\\s*(.+)' - Matches the encoding declaration

Challenges and Limitations:
1. Format inconsistency: Not all Gutenberg books follow the same header format.
   Some use "Posting Date" instead of "Release Date".
2. Multi-line titles: Some titles span multiple lines, which our single-line
   regex may not fully capture.
3. Missing metadata: Some books may lack certain fields entirely.
4. Special characters: Titles with Unicode or special characters may cause issues.

Handling Issues in Real-World Scenarios:
1. Use more robust parsers (e.g., dedicated Gutenberg header parser libraries)
2. Implement fallback patterns for different header formats
3. Use data validation and cleaning pipelines
4. Flag records with missing metadata for manual review
5. Standardize date formats using date parsing libraries (e.g., dateutil)
    """)

    spark.stop()


if __name__ == "__main__":
    main()
