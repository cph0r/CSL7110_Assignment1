import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    /**
     * Map Class
     * 
     * Q2 Answer:
     * Input pairs:  (LongWritable key, Text value) - key is byte offset, value is the line
     * Output pairs: (Text key, IntWritable value)  - key is a word, value is 1
     * 
     * Example input:  (0, "We're up all night to the sun")
     * Example output: ("We're", 1), ("up", 1), ("all", 1), ("night", 1), ("to", 1), ("the", 1), ("sun", 1)
     *
     * Q5: Uses String.replaceAll() to remove punctuation and StringTokenizer to split words
     */
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Remove punctuation but keep apostrophes within words
            String line = value.toString().replaceAll("[^a-zA-Z'\\s]", "");

            // Split line into words using StringTokenizer
            StringTokenizer tokenizer = new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {
                // Trim leading/trailing apostrophes and convert to lowercase
                String token = tokenizer.nextToken().replaceAll("^'+|'+$", "").toLowerCase();
                if (!token.isEmpty()) {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }
    }

    /**
     * Reduce Class
     * 
     * Q3 Answer:
     * Input pairs:  (Text key, Iterable<IntWritable> values) - key is a word, values is list of counts
     * Output pairs: (Text key, IntWritable value) - key is a word, value is total count
     * 
     * Example input:  ("up", [1, 1, 1, 1])
     * Example output: ("up", 4)
     *
     * Q6: Sums all values for each key to produce the total word count
     */
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: WordCount <input path> <output path> [split.maxsize]");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCount");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        // Q4: Set the output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Q9: Experiment with split.maxsize parameter
        if (args.length >= 3) {
            long splitMaxSize = Long.parseLong(args[2]);
            job.getConfiguration().setLong(
                "mapreduce.input.fileinputformat.split.maxsize", splitMaxSize);
            System.out.println("Split max size set to: " + splitMaxSize + " bytes");
        }

        // Q9: Measure and display total execution time
        long startTime = System.currentTimeMillis();

        boolean success = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;

        System.out.println("===========================================");
        System.out.println("Job completed: " + (success ? "SUCCESS" : "FAILURE"));
        System.out.println("Total execution time: " + executionTime + " ms");
        System.out.println("Total execution time: " + (executionTime / 1000.0) + " seconds");
        System.out.println("===========================================");

        System.exit(success ? 0 : 1);
    }
}
