package cs555.hadoop.q04;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import cs555.hadoop.Util.DataUtilities;

public class Q04AirportMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().isEmpty()){
			ArrayList<String> record = DataUtilities.dataReader(value.toString());
			
			String iata = record.get(0);
			String airport = record.get(1);
			String city = record.get(2);
			String state = record.get(3);
			
			if (!iata.isEmpty() && !state.isEmpty() && !airport.isEmpty() && !city.isEmpty()) {
				context.write(new Text("AIRPORTCODE:" + iata), new Text(airport + ", located in " + city + ", " + state));
			}
		}
	}

}
