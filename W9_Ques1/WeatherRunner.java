package W9_Ques1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WeatherRunner {

	  
  
	  public static void main(String[] args) throws Exception {
		  // Program by Jawahar G-170004

		  Configuration conf = new Configuration();

		  Job job = new Job(conf, "weather Data Question 1");

		  job.setJarByClass(WeatherRunner.class);

		  job.setMapOutputKeyClass(Text.class);

		  job.setMapOutputValueClass(IntWritable.class);

		  job.setOutputKeyClass(Text.class);

		  job.setOutputValueClass(IntWritable.class);

		  job.setMapperClass(WeatherMapper.class);

		  job.setReducerClass(WeatherReducer.class);

		  job.setInputFormatClass(TextInputFormat.class);

		  job.setOutputFormatClass(TextOutputFormat.class);

		  FileInputFormat.addInputPath(job, new Path(args[0]));

		  FileOutputFormat.setOutputPath(job,new Path(args[1]));

		  job.waitForCompletion(true);
		  }
}
