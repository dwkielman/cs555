package cs555.hadoop.q04;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs555.hadoop.Util.DataUtilities;

public class Q04Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().isEmpty()){
			String[] record = value.toString().split(",");
			
			if (record.length >= 26) {
				
				String origin = record[16];
				String dest = record[17];
				String weatherDelay = record[25];
				
				if (!weatherDelay.isEmpty()) {
					if (!weatherDelay.equals("NA")) {
						double delay = Double.parseDouble(weatherDelay);
						if (delay > 0) {
							if (!origin.isEmpty()) {
								context.write(new Text("CITY:" + origin), new Text("WEATHERDELAY:" + delay));
							}
							
							if (!dest.isEmpty()) {
								context.write(new Text("CITY:" + dest), new Text("WEATHERDELAY:" + delay));
							}
						}
					}
				}
			}
		}
	}
}
