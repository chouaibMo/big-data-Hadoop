import java.io.IOException;
import java.net.URLDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question3_1 {
	
	// +--------------------------------------------------------------------------------------------------------------+
	// |                                          JOB 1                                                               |
	// +--------------------------------------------------------------------------------------------------------------+
	public static class MyMapper1 extends Mapper<LongWritable, Text, CountryAndTag, IntWritable> {
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
						context.write(new CountryAndTag(country.toString(),tag.toString()), new IntWritable(1));
				
			}	
		}
	}
	
	public static class MyCombiner1 extends Reducer<CountryAndTag, IntWritable, CountryAndTag, IntWritable> {	
		@Override
		protected void reduce(CountryAndTag key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable value : values) 
				sum += value.get();				
			
			context.write(key, new IntWritable(sum));
		}
	}

	
	public static class MyReducer1 extends Reducer<CountryAndTag, IntWritable, CountryAndTag, IntWritable> {
		@Override
		protected void reduce(CountryAndTag key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {		
			
			int count = 0;
			for (IntWritable value : values) 
				count += value.get();				
			
			context.write(key, new IntWritable(count));
		}	
	}
	
	
	// +--------------------------------------------------------------------------------------------------------------+
	// |                                          JOB 2                                                               |
	// +--------------------------------------------------------------------------------------------------------------+
	
	public static class MyMapper2 extends Mapper<CountryAndTag, IntWritable, StringAndInt, Text> {

		@Override
		protected void map(CountryAndTag key, IntWritable value, Context context) throws IOException, InterruptedException {			
			context.write(new StringAndInt(key.getCountry(), value.get()), new Text(key.getTag()));						
		}
	}

	
	public static class MyReducer2 extends Reducer<StringAndInt, Text, Text, StringAndInt> {
		@Override
		protected void reduce(StringAndInt key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			int k = Integer.parseInt(context.getConfiguration().get("k"));
			int cpt = 0;
			for (Text value : values) {
				context.write(new Text(key.getText()), new StringAndInt(value.toString(), key.getOccurrence()));
				cpt++;
				
				if (cpt == k) 
					break;
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		Integer k = new Integer(otherArgs[2]);

		conf.setInt("k", k);
		
		/* -------------- JOB 1 -------------- */
		Job job = Job.getInstance(conf, "Question3_1");
		job.setNumReduceTasks(1);
		job.setJarByClass(Question3_1.class);

		//MAPPER 1:
		job.setMapperClass(MyMapper1.class);
		job.setMapOutputKeyClass(CountryAndTag.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//COMBINER 1:
		job.setCombinerClass(MyCombiner1.class);

		//REDUCER 1:
		job.setReducerClass(MyReducer1.class);
		job.setOutputKeyClass(CountryAndTag.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output+".passe1"));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.waitForCompletion(true);
		
		/* -------------- JOB 2 -------------- */
		Job job2 = Job.getInstance(conf, "Question3_1");
		job2.setNumReduceTasks(1);
		job2.setJarByClass(Question3_1.class);
		
		//MAPPER 2 :	
		job2.setMapperClass(MyMapper2.class);
		job2.setMapOutputKeyClass(StringAndInt.class);
		job2.setMapOutputValueClass(Text.class);

		//REDUCER 2 :
		job2.setReducerClass(MyReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(StringAndInt.class);
		
		// FONCTIONS DE TRI :
		job2.setGroupingComparatorClass(CountryComparator.class);
		job2.setSortComparatorClass(SortComparator.class);

		FileInputFormat.addInputPath(job2, new Path(output+".passe1"));
		job2.setInputFormatClass(SequenceFileInputFormat.class);

		FileOutputFormat.setOutputPath(job2, new Path(output+".passe2"));
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		job2.waitForCompletion(true);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
	
}