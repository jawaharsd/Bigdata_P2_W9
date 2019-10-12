package W9_Ques4;

import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.io.IOException;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.conf.Configuration;

public class WeatherReducer extends Reducer<Text, Text, Text, Text> {
	// Program by Jawahar G-170004

		public void reduce(Text Key, Iterator<Text> Values, Context context)
				throws IOException, InterruptedException {

			String temperature_values = Values.next().toString();
			context.write(Key, new Text(temperature_values));
		}

	}
