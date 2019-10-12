package W9_Ques2;
import java.io.IOException;
import java.util.Iterator;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
public class WeatherMapper {
		
		public static class MaxTemperatureMapper extends
				Mapper<LongWritable, Text, Text, Text> {
			// Program by Jawahar G-170004
			@Override
			public void map(LongWritable arg0, Text Value, Context context)
					throws IOException, InterruptedException {
				String line_String = Value.toString();
				if (!(line_String.length() == 0)) {
					String date = line_String.substring(6, 14);
					float max_Temperature = Float
							.parseFloat(line_String.substring(39, 45).trim());
					float min_Temperature = Float
							.parseFloat(line_String.substring(47, 53).trim());
					if (max_Temperature > 35.0) {
						context.write(new Text("Hot Day " + date),
								new Text(String.valueOf(max_Temperature)));
					}
					if (min_Temperature < 10) {
						context.write(new Text("Cold Day " + date),
								new Text(String.valueOf(min_Temperature)));
					}
				}
			}
	 
		}
}
	 