package Practice;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;

public class AvgWordLength {
	
	public static class WordMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		private static Text word = new Text();
		// Map
		public static void wordMapper(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreElements()) {
				word.set(itr.nextToken());
				IntWritable length = new IntWritable(word.getLength());
				context.write(word, length);
			}
		}
	}
	
	public static class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		private static IntWritable result = new IntWritable();
		
		public static void wordReducer(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			int count = 0;
			for(IntWritable val: values) {
				sum += val.get();
				count++;
			}
			
			result.set(sum / count);
			
			context.write(key, result);
		}
	}
	
	public void main(String args[]) throws IllegalArgumentException, IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(AvgWordLength.class);
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReducer.class);
		job.setCombinerClass(WordReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	}

}
