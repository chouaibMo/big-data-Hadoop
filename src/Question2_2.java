import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.curator.shaded.com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class Question2_2 {
	
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, StringAndInt> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//splitting the line
			String line[] = value.toString().split("\t");
			
			//checking if latitude, longitude and tags are empty
			if( line[11].isEmpty() || line[10].isEmpty() || line[8].isEmpty())
				return;
						
			Double lat = Double.parseDouble(line[11]);
			Double lon = Double.parseDouble(line[10]);
			String allTags = line[8];
			
			//Getting the country by latitude and longitude
			Country country = Country.getCountryAt(lat, lon);
				
			
			if (country != null){
				String[] tags = URLDecoder.decode(allTags.toString(), "UTF-8").split(",");
				if(tags.length > 0) 
					for (String tag : tags) 
						context.write(new Text(country.toString()) , new StringAndInt(tag.toString(), 1) );
				
			}	
		}
	}
	
	public static class MyCombiner extends Reducer<Text, StringAndInt, Text, StringAndInt> {
		
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
			
			Map<String, Integer> combinerMap = new HashMap<String, Integer>();
			
			for (StringAndInt value : values) {
				if (combinerMap.containsKey(value.getText().toString())) 
					combinerMap.put(value.getText().toString(), combinerMap.get( value.getText().toString() ) + 1);
				
				else
					combinerMap.put(value.getText().toString(), 1);
			}
			

			for (Entry<String, Integer> entry : combinerMap.entrySet()) {	
				context.write(key, new StringAndInt(entry.getKey(), entry.getValue()));
		    }
		}
	}

	
	public static class MyReducer extends Reducer<Text, StringAndInt, Text, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context) throws IOException, InterruptedException {
			
			HashMap<String, Integer> tagsMap = new HashMap<>();
			int k = Integer.parseInt(context.getConfiguration().get("K"));
			
			for (StringAndInt value : values) {
					if (tagsMap.containsKey(value.getText().toString())) 
						tagsMap.put(value.getText().toString(), tagsMap.get(value.getText().toString())+value.getOccurrence());
					else 
						tagsMap.put(value.getText().toString(), value.getOccurrence());
				
			}
			
			MinMaxPriorityQueue<StringAndInt> queue = MinMaxPriorityQueue.maximumSize(k).create();
			for (String tag : tagsMap.keySet()) {
				queue.add(new StringAndInt(tag, tagsMap.get(tag)));
			}
			
			StringAndInt val = null;
			while(!queue.isEmpty()) {
			    val = queue.pollFirst();
				context.write(key, new Text(val.toString()));
			}
		}	
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		conf.setInt("K", Integer.parseInt(otherArgs[2]));

		Job job = Job.getInstance(conf, "Question2_2");
		job.setJarByClass(Question2_2.class);

		//MAPPER
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringAndInt.class);
		
		
		//REDUCER
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//COMBINER
		job.setCombinerClass(MyCombiner.class);
				
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);

		job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}