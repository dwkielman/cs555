package cs555.hadoop.q03;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Q03Combiner extends Reducer<Text, Text, Text, Text> {

	private HashMap<String, Integer> airportCountMap;
	private HashMap<String, String> airportCodeMap;
	private HashMap<Integer, HashMap<String, Integer>> yearCountMap;
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		airportCountMap = new HashMap<String, Integer>();
		airportCodeMap = new HashMap<String, String>();
		yearCountMap = new HashMap<Integer, HashMap<String, Integer>>();
	}
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		HashMap<String, Integer> tempAirportCountMap = new HashMap<String, Integer>();
		
		String[] keyStringArray = key.toString().split(":");
		if (keyStringArray.length == 2) {
			String keyString = keyStringArray[0];
			String keyStringAttribute = keyStringArray[1];
			
			if (keyString.equals("YEAR")) {
				for (Text val : values) {
					String iata = val.toString();
					if (airportCountMap.containsKey(iata)) {
						int currentCount = airportCountMap.get(iata);
						currentCount++;
						airportCountMap.replace(iata, currentCount);
					} else {
						airportCountMap.put(iata, 1);
					}
					
					if (tempAirportCountMap.containsKey(iata)) {
						int currentCount = tempAirportCountMap.get(iata);
						currentCount++;
						tempAirportCountMap.replace(iata, currentCount);
					} else {
						tempAirportCountMap.put(iata, 1);
					}
				}
			} else if (keyString.equals("AIRPORTCODE")) {
				//if (keyStringArray.length > 0) {
					for (Text val : values) {
						airportCodeMap.put(keyStringAttribute, val.toString());
					}
				//}
			}
			
			if (keyString.equals("YEAR")) {
				yearCountMap.put(Integer.parseInt(keyStringAttribute), tempAirportCountMap);
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		for (int year : yearCountMap.keySet()) {
			HashMap<String, Integer> yearsAirportCount = yearCountMap.get(year);
			for (String iata : yearsAirportCount.keySet()) {
				if (airportCodeMap.containsKey(iata)) {
					int airportFlightCount = yearsAirportCount.get(iata);
					String airportInfo = airportCodeMap.get(iata);
					context.write(new Text("YEAR:" + year), new Text("AIRPORTCODE:" + iata + ",FLIGHTCOUNT:" + airportFlightCount + "," + airportInfo));
				}
			}
		}
	}

}
