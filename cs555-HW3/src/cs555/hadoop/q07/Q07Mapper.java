package cs555.hadoop.q07;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q07Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().isEmpty()){
			String[] record = value.toString().split(",");
			
			if (record.length >= 28) {
				String year = record[0];
				String origin = record[16];
				String dest = record[17];
				String lateAircraftDelay = record[28];
				
				if (!lateAircraftDelay.isEmpty()) {
					if (!lateAircraftDelay.equals("NA")) {
						double delay = Double.parseDouble(lateAircraftDelay);
						if (!origin.isEmpty()) {
							context.write(new Text("YEAR:" + year), new Text("CITY:" + origin + ",LATEAIRCRAFTDELAY:" + delay));
						}
						
						if (!dest.isEmpty()) {
							context.write(new Text("YEAR:" + year), new Text("CITY:" + origin + ",LATEAIRCRAFTDELAY:" + delay));
						}
					}
				}
			}
		}
	}
}
