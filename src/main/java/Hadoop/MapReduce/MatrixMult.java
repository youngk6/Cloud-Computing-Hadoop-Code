package Hadoop.MapReduce;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMult {
	public static class MatMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		public static void matMapper(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			int m = Integer.parseInt(conf.get("m"));
			int n = Integer.parseInt(conf.get("n"));
			int p = Integer.parseInt(conf.get("p"));
			
			String line = value.toString();
			// (M, i, j, Mij)
			String[] indicesAndValue = line.split(",");
			
			Text outputKey = new Text();
			Text outputValue = new Text();
			if(indicesAndValue[0].equals("M")) {
				for(int k = 0; k < p; k++) {
					outputKey.set(indicesAndValue[1] + "," + k);
					//output key set to (i, k)
					outputValue.set(indicesAndValue[0] + "," + indicesAndValue[2] + "," + indicesAndValue[3]);
					// output value set to (M, j, Mij)
					context.write(outputKey, outputValue);
				}
			}else {
				//(N, j, k, Njk)
				for(int i = 0; i < m; i++) {
					outputKey.set(i + "," + indicesAndValue[2]);
					outputValue.set("N," + indicesAndValue[1] + "," + indicesAndValue[3]);
					context.write(outputKey, outputValue);
				}
			}	
		}
	}
	
	public static class MatReducer extends Reducer<Text, Text, Text, Text>{
		public static void matReducer(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] value;
			//key, values
			// (i,k) , [(M/N, j, V/W), ... ]
			HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
			HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
			
			for(Text val: values) {
				value = val.toString().split(",");
				if(value[0].contentEquals("M")) {
					hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
				}else {
					hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
				}
			}
			
			int n = Integer.parseInt(context.getConfiguration().get("n"));
			float result = 0.0f;
			float m_ij;
			float n_jk;
			
			for(int j = 0; j < n; j++) {
				m_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
				n_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
				result += m_ij * n_jk;
			}
			
			context.write(null, new Text(key.toString() + "," + Float.toString(result)));
		}
	}
	
	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		conf.set("m", "5");
		conf.set("n", "5");
		conf.set("p", "5");
		
		Job job = new Job(conf, "MatrixMultiply");
		job.setJarByClass(MatrixMult.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MatMapper.class);
		job.setReducerClass(MatReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
	
	
}

