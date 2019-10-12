package W9_Ques4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {
	// Program by Jawahar G-170004
public static final int MISSING = 9999;
public void map(LongWritable arg0, Text Value, Context context)
				throws IOException, InterruptedException {
			String line = Value.toString();
		
			if (!(line.length() == 0)) {
				
				String date = line.substring(6, 14);
	
				float temp_Max = Float
						.parseFloat(line.substring(39, 45).trim());
				
				float temp_Min = Float
						.parseFloat(line.substring(47, 53).trim());
	
				if ( temp_Max != MISSING && temp_Min != MISSING) {
				
					context.write(new Text("|Time/Date: " + date+ " | "),
							new Text(String.valueOf(" Maximum Temperature: "+ temp_Max+ "\t|  Minimum Temperature: "+ temp_Min+ "\t|")));
				}

				
				
               
			}
		}
}
	