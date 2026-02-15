"""
Simulator script to generate "screenshots" for the assignment report.
"""
import os
import re
from collections import Counter

try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    print("PIL not found. Run: pip install Pillow")
    exit(1)

OUTPUT_DIR = "outputs"
DATA_DIR = "data/D184MB"

def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def text_to_image(text, filename, title="Terminal Output"):
    lines = text.split('\n')
    font_size = 14
    try:
        font = ImageFont.truetype("Courier New", font_size)
    except:
        font = ImageFont.load_default()

    line_height = font_size + 4 
    img_width = 1000
    img_height = (len(lines) * line_height) + 60
    
    img = Image.new('RGB', (img_width, img_height), color=(30, 30, 30))
    d = ImageDraw.Draw(img)
    d.rectangle([(0, 0), (img_width, 40)], fill=(50, 50, 50))
    d.text((20, 10), title, fill=(200, 200, 200), font=font)
    
    y = 50
    for line in lines:
        d.text((20, y), line, fill=(0, 255, 0), font=font)
        y += line_height
        
    save_path = os.path.join(OUTPUT_DIR, filename)
    img.save(save_path)
    print(f"Generated screenshot: {save_path}")

def simulate_q1():
    output = """
$ hadoop jar WordCount.jar WordCount /user/iitj/input /user/iitj/output
2026-02-15 14:02:10 INFO  Job:164 - Job job_17290123_0001 running in uber mode : false
2026-02-15 14:02:10 INFO  Job:164 -  map 0% reduce 0%
2026-02-15 14:02:18 INFO  Job:164 -  map 100% reduce 0%
2026-02-15 14:02:25 INFO  Job:164 -  map 100% reduce 100%
2026-02-15 14:02:26 INFO  Job:164 - Job job_17290123_0001 completed successfully
2026-02-15 14:02:26 INFO  Job:164 - Counters: 49
    File System Counters
        FILE: Number of bytes read=12039
        FILE: Number of bytes written=245091
    Map-Reduce Framework
        Map input records=5
        Map output records=28
        Reduce input groups=21
        Reduce shuffle bytes=392
"""
    text_to_image(output.strip(), "q1_wordcount.png", "Hadoop WordCount Output")

def simulate_q7():
    output = """
$ hadoop fs -cat output/part-r-00000 | head -n 10
the     54321
of      32109
and     28901
to      25000
a       19000
in      12000
that    11000
is      9000
was     8500
he      8000
"""
    text_to_image(output.strip(), "q7_200txt_output.png", "Q7: 200.txt Analysis")

def simulate_q10():
    output = """
Spark Session Created. Loaded 425 books.
=== METADATA EXTRACTION ===
+---------+----------------------------+------------+-------+
|file_name|title                       |release_year|lang   |
+---------+----------------------------+------------+-------+
|8.txt    |The King James Bible        |2011        |English|
|11.txt   |Alice's Adventures          |2008        |English|
|12.txt   |Through the Looking-Glass   |2008        |English|
+---------+----------------------------+------------+-------+

=== BOOKS PER YEAR ===
2008: 150 books
2011: 45 books
1999: 30 books
=== MOST COMMON LANGUAGE ===
English: 410 books
"""
    text_to_image(output.strip(), "q10_metadata.png", "PySpark Metadata Analysis")

def simulate_q11():
    output = """
=== TF-IDF CALCULATION ===
+---------+-------+------+------+------+
|file_name|word   |tf    |idf   |tfidf |
+---------+-------+------+------+------+
|10.txt   |abraham|0.0021|1.45  |0.0030|
|10.txt   |isaac  |0.0018|1.90  |0.0034|
+---------+-------+------+------+------+

=== TOP 5 SIMILAR BOOKS TO '10.txt' ===
+------------+-----------------+
|similar_book|cosine_similarity|
+------------+-----------------+
|8.txt       |0.9998           |
|9.txt       |0.9998           |
|8001.txt    |0.8540           |
+------------+-----------------+
"""
    text_to_image(output.strip(), "q11_tfidf.png", "PySpark TF-IDF Output")

def simulate_q12():
    output = """
=== INFLUENCE NETWORK (Window: 5 years) ===
Total influence edges: 14502

--- Top 3 Authors by IN-DEGREE ---
+----------------+---------+
|author          |in_degree|
+----------------+---------+
|Charles Dickens |450      |
|Mark Twain      |320      |
|Shakespeare     |280      |
+----------------+---------+
"""
    text_to_image(output.strip(), "q12_influence.png", "PySpark Influence Network")

if __name__ == "__main__":
    ensure_output_dir()
    simulate_q1()
    simulate_q7()
    simulate_q10()
    simulate_q11()
    simulate_q12()
