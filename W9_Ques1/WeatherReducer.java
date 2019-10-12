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

public class WeatherReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	// Program by Jawahar G-170004

public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

int maximum_Value = 0;
for (IntWritable val : values) {
maximum_Value = Math.max(maximum_Value, val.get());
}

context.write(key, new IntWritable(maximum_Value));
}
}