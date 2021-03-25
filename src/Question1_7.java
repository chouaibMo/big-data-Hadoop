
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question1_7 {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		public HashMap<String,Integer> dataMap;
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			for(String stringLoop : value.toString().split(" ")) {
				stringLoop = stringLoop.replaceAll("\\s*,\\s*$", "");
				stringLoop = stringLoop.trim();
				
				if( this.dataMap.get(stringLoop) == null) 
					this.dataMap.put(stringLoop, 1);
				else {
					int nb = this.dataMap.get(stringLoop) + 1;
					this.dataMap.put(stringLoop, nb);
				}
			}
		}
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException{
			this.dataMap = new HashMap<>();
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (Map.Entry<String,Integer> entry : dataMap.entrySet()){
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			}
		}
	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable value : values) {
			sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println(args[0]);
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		
		Job job = Job.getInstance(conf, "Question0_0");
		job.setJarByClass(Question1_7.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}