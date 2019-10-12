package W9_Ques3;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class WeatherReducer extends
			Reducer<Text, Text, Text, Text> {
	// Program by Jawahar G-170004
 
		public void reduce(Text Key, Iterator<Text> Values, Context context)
				throws IOException, InterruptedException {
 
			
			
			String temperature = Values.next().toString();
			context.write(Key, new Text(temperature));
		}
 
	}
 