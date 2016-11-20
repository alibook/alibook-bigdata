package alibook.wordmean;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.base.Charsets;

/**
 * This example is used to caculate the mean length of all words 
 *
 */
public class WordMean {
	private final static Text LENGTH = new Text("length");
	private final static Text COUNT = new Text("count");
	private final static LongWritable ONE = new LongWritable(1);
	
    /**
	 * Maps words from line of text into 2 key-value pairs; one key-value pair for
	 * counting the word, another for counting its length.
	 */
	public static class WordMeanMapper extends Mapper<Object, Text, Text, LongWritable> {
		private LongWritable wordlen = new LongWritable();
		
	    /**
	     * Emits 2 key-value pairs for counting the word and its length. Outputs are
	     * (Text, LongWritable).
	     * 
	     * @param value
	     *          This will be a line of text coming in from our input file.
	     */
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			StringTokenizer iter = new StringTokenizer(value.toString());
			while (iter.hasMoreTokens()) {
				wordlen.set(iter.nextToken().length());
				context.write(LENGTH, wordlen);
				context.write(COUNT, ONE);
			}
		}
	}
	
    /**
	 * Performs integer summation of all the values for each key.
	 */
	public static class WordMeanReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable sum = new LongWritable(0);
		
	    /**
	     * Sums all the individual values within the iterator and writes them to the
	     * same key.
	     * 
	     * @param key
	     *          This will be one of 2 constants: LENGTH_STR or COUNT_STR.
	     * @param values
	     *          This will be an iterator of all the values associated with that
	     *          key.
	     */
		public void reduce(Text key, Iterable<LongWritable> values, Context context) 
				throws IOException, InterruptedException {
			int theSum = 0;
			for (LongWritable value : values) {
				theSum += value.get();
			}
			sum.set(theSum);
			context.write(key, sum);
		}
	}

	/**
	 * Reads the output file and parses the summation of lengths, and the word
	 * count, to perform a quick calculation of the mean.
	 * 
	 * @param path
	 *          The path to find the output file in. Set in main to the output
	 *          directory.
	 * @throws IOException
	 *           If it cannot access the output directory, we throw an exception.
	 */
	private static void readAndCalcMean(Path path, Configuration conf) 
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path file = new Path(path, "part-r-00000");
		
		if (!fs.exists(file)) {
			throw new IOException("output not found");
		}
		
		BufferedReader br = null;

		// average = total sum / number of elements;
		try {
			br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));
			
			long count = 0;
			long length = 0;
			
			String line ;
			while ((line = br.readLine()) != null) {
				StringTokenizer tokenizer = new StringTokenizer(line);
				
				//grab type
				String type = tokenizer.nextToken();
				
				if (type.equals(COUNT.toString())) {
					String countlit = tokenizer.nextToken();
					count = Long.parseLong(countlit);
				} else if (type.equals(LENGTH.toString())) {
					String lengthlit = tokenizer.nextToken();
					length = Long.parseLong(lengthlit);
				}
			}
			double theMean = (double)length / (double)count;
			System.out.println("The mean length is: " + theMean);
		} finally {
			if (br != null) {
				br.close();
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: <inputdir> <outputdir>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word mean");
		
		job.setJarByClass(WordMean.class);
	
		job.setMapperClass(WordMeanMapper.class);
		job.setReducerClass(WordMeanReducer.class);
		job.setCombinerClass(WordMeanReducer.class);
		
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    Path outputpath = new Path(args[1]);
	    FileOutputFormat.setOutputPath(job, outputpath);
	    
	    boolean result = job.waitForCompletion(true);
	    readAndCalcMean(outputpath, conf);
	}
}
