
import os

def install_package():
    try:
        import fpdf
    except ImportError:
        print("Installing fpdf...")
        os.system("/usr/bin/python3 -m pip install fpdf")

try:
    from fpdf import FPDF
except ImportError:
    # Fallback if pip install inside script fails, though we will try running this in a venv
    pass

class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 15)
        self.cell(0, 10, 'CSL7110 Assignment 1: MapReduce & Spark', 0, 1, 'C')
        self.set_font('Arial', 'I', 10)
        self.cell(0, 10, 'Roll No: M25DE1021', 0, 1, 'C')
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, 'Page ' + str(self.page_no()) + '/{nb}', 0, 0, 'C')

    def chapter_title(self, num, label):
        self.set_font('Arial', 'B', 12)
        self.set_fill_color(200, 220, 255)
        self.cell(0, 6, 'Q%d: %s' % (num, label), 0, 1, 'L', 1)
        self.ln(4)

    def chapter_body(self, body):
        self.set_font('Arial', '', 11)
        self.multi_cell(0, 5, body)
        self.ln()

    def code_block(self, code):
        self.set_font('Courier', '', 9)
        self.set_fill_color(240, 240, 240)
        self.multi_cell(0, 4, code, 0, 'L', True)
        self.ln()

    def add_screenshot(self, title, image_path):
        self.chapter_title(0, title)  # Use 0 or appropriate numbering logic if needed, or just title
        if os.path.exists(image_path):
            try:
                # Adjust width to fit page (A4 width is 210mm, margins are usuall 10mm each side)
                self.image(image_path, x=10, w=190)
                self.ln(5)
            except Exception as e:
                self.set_text_color(255, 0, 0)
                self.cell(0, 10, f"Error loading image: {e}", 0, 1)
                self.set_text_color(0, 0, 0)
        else:
            self.set_text_color(100, 100, 100)
            self.set_font('Arial', 'I', 10)
            self.cell(0, 10, f"[Placeholder: Save screenshot as '{os.path.basename(image_path)}' in outputs/ directory]", 0, 1)
            self.set_text_color(0, 0, 0)
        self.ln()

pdf = PDF()
pdf.alias_nb_pages()
pdf.add_page()

# GitHub Link
pdf.set_font('Arial', 'B', 12)
pdf.cell(0, 10, 'GitHub Repository:', 0, 1)
pdf.set_font('Arial', '', 11)
pdf.write(5, 'https://github.com/chiragphor/CSL7110_Assignment1\n(Please verify this link points to your actual repo)')
pdf.ln(10)

# Section: Apache Hadoop & MapReduce

pdf.chapter_title(1, 'WordCount Execution')
pdf.chapter_body('The WordCount example was executed successfully.')
pdf.add_screenshot("Output Screenshot", "outputs/q1_wordcount.png")

pdf.chapter_title(2, 'Map Phase Input/Output')
pdf.chapter_body('For the input lyrics "We\'re up all night...", the Map phase processes lines as values with byte offsets as keys.')
pdf.chapter_body('Input Pairs (Key: LongWritable, Value: Text):')
pdf.code_block('(0, "We\'re up all night till the sun")\n(31, "We\'re up all night to get some")\n(63, "We\'re up all night for good fun")\n(95, "We\'re up all night to get lucky")')
pdf.chapter_body('Output Pairs (Key: Text, Value: IntWritable):')
pdf.code_block('("We\'re", 1), ("up", 1), ("all", 1), ("night", 1), ("till", 1), ("the", 1), ("sun", 1)\n("to", 1), ("get", 1), ("some", 1), ...')
pdf.chapter_body('Types:\nInput Key: LongWritable (Byte Offset)\nInput Value: Text (Line context)\nOutput Key: Text (Word)\nOutput Value: IntWritable (Count 1)')

pdf.chapter_title(3, 'Reduce Phase Input/Output')
pdf.chapter_body('The Shuffle/Sort phase groups values by key.')
pdf.chapter_body('Input Pairs (Key: Text, Value: Iterable<IntWritable>):')
pdf.code_block('("up", [1, 1, 1, 1])\n("to", [1, 1, 1])\n("get", [1, 1])\n("lucky", [1])')
pdf.chapter_body('Types:\nInput Key: Text\nInput Value: Iterable<IntWritable>\nOutput Key: Text\nOutput Value: IntWritable (Sum)')

pdf.chapter_title(4, 'WordCount.java Modifications')
pdf.chapter_body('The code was modified to use Hadoop IO types.')
pdf.code_block('public void map(LongWritable key, Text value, Context context)\npublic void reduce(Text key, Iterable<IntWritable> values, Context context)\n\njob.setOutputKeyClass(Text.class);\njob.setOutputValueClass(IntWritable.class);')

pdf.chapter_title(5, 'Map Function Implementation')
pdf.chapter_body('Implemented using String.replaceAll("[^a-zA-Z\'\\\\s]", "") to handle punctuation and StringTokenizer for splitting.')

pdf.chapter_title(6, 'Reduce Function Implementation')
pdf.chapter_body('Implemented to iterate through IntWritable values and sum them up.')

pdf.chapter_title(7, 'Execution on 200.txt')
pdf.chapter_body('Executed on the cluster. The output file contains word counts for the dataset.')
pdf.add_screenshot("Q7 Output Screenshot", "outputs/q7_200txt_output.png")

pdf.chapter_title(8, 'HDFS Replication')
pdf.chapter_body('Q: Why don\'t we have a replication factor for directories?')
pdf.chapter_body('A: Directories in HDFS are metadata constructs stored in the NameNode\'s memory (namespace). They do not have physical data blocks distributed across DataNodes, so "replication" in the block storage sense does not apply.')
pdf.chapter_body('Q: How does changing replication factor impact performance?')
pdf.chapter_body('A: \n1. Higher replication: Increases fault tolerance and data availability. It can improve read performance by allowing more tasks to read data locally (data locality). However, it increases write overhead (more network traffic to replicate) and storage usage.\n2. Lower replication: Saves space and faster writes, but reduces fault tolerance and read reliability/locality.')

pdf.chapter_title(9, 'Execution Time & Split Size')
pdf.chapter_body('Added timing code using System.currentTimeMillis().\nExperimenting with split.maxsize:\n- Smaller split size -> More splits -> More Map tasks -> Higher parallelism but more container startup overhead.\n- Larger split size -> Fewer Map tasks -> Less overhead but potentially lower parallelism.\nOptimum depends on cluster capacity and file size.')

# Section: Apache Spark

pdf.chapter_title(10, 'Book Metadata Extraction')
pdf.chapter_body('Implemented in spark/q10_metadata.py using Python Regex.')
pdf.add_screenshot("Q10 Output Screenshot", "outputs/q10_metadata.png")
pdf.chapter_body('Regex used:\n- Title: "Title:\\s*(.+)"\n- Release Date: "Release [Dd]ate:\\s*(.+?)(?:\\s*\\[|$)"\n- Language: "Language:\\s*(.+)"\n- Encoding: "Character set encoding:\\s*(.+)"')
pdf.chapter_body('Challenges:\n- Inconsistent headers (e.g., "Posting Date" vs "Release Date").\n- Multi-line values.\n- Missing fields.')
pdf.chapter_body('Handling in Real-world:\n- Use robust parsers/libraries.\n- Data validation pipelines.\n- Fallback patterns.')

pdf.chapter_title(11, 'TF-IDF & Book Similarity')
pdf.chapter_body('Implemented using PySpark native functions (q11_tfidf.py).')
pdf.add_screenshot("Q11 Output Screenshot", "outputs/q11_tfidf.png")
pdf.chapter_body('Concepts:\n- TF (Term Frequency): How often a word appears in a doc.\n- IDF (Inverse Doc Frequency): log(N/df), weights down common words.\n- TF-IDF: Highlights words important to a specific doc but rare globally.')
pdf.chapter_body('Cosine Similarity:\n- Measures angle between vectors.\n- Range [0, 1].\n- Good for text because it ignores document length (magnitude).')
pdf.chapter_body('Scalability:\n- Pairwise comparison is O(N^2). Spark helps via distributed computing, but for massive datasets, approximate nearest neighbor (LSH) is preferred to avoid full shuffling.')

pdf.chapter_title(12, 'Author Influence Network')
pdf.chapter_body('Implemented in spark/q12_influence.py.')
pdf.add_screenshot("Q12 Output Screenshot", "outputs/q12_influence.png")
pdf.chapter_body('Influence Definition: Author A -> Author B if A released a book within X=5 years before B.')
pdf.chapter_body('Representation: DataFrame of edges (author_a, author_b).')
pdf.chapter_body('Pros: Easy SQL aggregations. Cons: Expensive cross-joins.')
pdf.chapter_body('Scalability: Cross-join is expensive. Optimization: Partition by time window or use GraphFrames.')

output_path = "M25DE1021_CSL7110_Assignment1.pdf"
pdf.output(output_path, 'F')
print(f"PDF generated: {output_path}")
