package W9_Ques1;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WeatherMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	// Program by Jawahar G-170004
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {

String record = value.toString();
String[] parts = record.split(",,");
context.write(new Text(parts[0]), new IntWritable(Integer.parseInt(parts[1])));
}
}