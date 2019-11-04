package cs555.hadoop.q05;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs555.hadoop.Util.DataUtilities;

public class Q05Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().isEmpty()){
			//ArrayList<String> record = DataUtilities.dataReader(value.toString());
			String[] record = value.toString().split(",");
			
			//if (record.size() >= 25) {
				//String uniqueCarrier = record.get(9);
				//String carrierDelay = record.get(25);
				
			if (record.length >= 25) {
				String uniqueCarrier = record[8];
				String carrierDelay = record[24];
				
				if (!carrierDelay.isEmpty()) {
					if (!carrierDelay.equals("NA")) {
						double delay = Double.parseDouble(carrierDelay);
						if (delay > 0) {
							context.write(new Text("CARRIER:" + uniqueCarrier), new Text("CARRIERDELAY:" + delay));
						}
					}
				}
			}
		}
	}
}
