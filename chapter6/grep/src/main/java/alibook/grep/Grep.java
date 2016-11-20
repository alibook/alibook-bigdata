package alibook.grep;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Grep: It's used to Extracts matching string from input files and counts them.
 *
 */
public class Grep {
	public static class GrepMapper extends Mapper<Object, Text, Text, IntWritable> {
		public void map(Object obj, Text text, Context context) 
				throws IOException, InterruptedException {
			String pattern = context.getConfiguration().get("grep");
			
			String str = text.toString();
			Pattern r = Pattern.compile(pattern);
			Matcher matcher = r.matcher(str);
			
			while (matcher.find()) {
				FileSplit split = (FileSplit)context.getInputSplit();
				String filename = split.getPath().getName();
				
				context.write(new Text(filename), new IntWritable(1));
			}
		}
 	}
	
	public static class GrepReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text text, Iterable<IntWritable> values,  Context context) 
				throws IOException, InterruptedException{
			int sum = 0;
			Iterator<IntWritable> iterator = values.iterator();
			while (iterator.hasNext()) {
				sum += iterator.next().get();
			}
			
			context.write(text, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: <pattern> <inputdir> <outputdir>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		conf.set("grep", args[0]);

		Job job = Job.getInstance(conf, "distributed grep");
		
		job.setJarByClass(Grep.class);
		job.setMapperClass(GrepMapper.class);
		job.setReducerClass(GrepReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
	
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
