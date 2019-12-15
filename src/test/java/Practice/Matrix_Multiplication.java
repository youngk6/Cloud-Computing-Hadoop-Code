package Practice;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Matrix_Multiplication {
	public static class MatrixMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		public static void matrixMapper(Object key, IntWritable value, Context context) throws IOException, InterruptedException{
			
		}
		
	}
	
	public static class MatrixReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		public static void matrixReducer(Text key, IntWritable value, Context context) throws IOException, InterruptedException{
			
		}
	}
	
	public static void main(String args[]) {
		
	}

}
