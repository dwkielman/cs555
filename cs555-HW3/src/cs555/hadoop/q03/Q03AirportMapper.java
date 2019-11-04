package cs555.hadoop.q03;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs555.hadoop.Util.DataUtilities;

public class Q03AirportMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().isEmpty()){
			ArrayList<String> record = DataUtilities.dataReader(value.toString());
			
			String iata = record.get(0);
			String airport = record.get(1);
			String city = record.get(2);
			String state = record.get(3);
			
			if (state.length() == 2 && !state.equals("HI") && !state.equals("AK")) {
				if (!airport.isEmpty() && !city.isEmpty()) {
					context.write(new Text("AIRPORTCODE:" + iata), new Text(airport + ", located in " + city + ", " + state));
				}
			}
		}
	}

}
