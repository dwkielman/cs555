package cs555.hadoop.q04;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q04Reducer extends Reducer<Text, Text, Text, Text> {
	
	private HashMap<String, Integer> airportCountMap;
	private HashMap<String, Double> airportDelayTotalMap;
	private HashMap<String, String> airportCodeMap;
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		airportCountMap = new HashMap<String, Integer>();
		airportCodeMap = new HashMap<String, String>();
		airportDelayTotalMap = new HashMap<String, Double>();
	}

	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String[] keyStringArray = key.toString().split(":");
		String keyString = keyStringArray[0];
		String keyStringAttribute = keyStringArray[1];
		
		if (keyString.equals("CITY")) {
			for (Text val : values) {
				String[] valueStringArray = val.toString().split(":");
				double delay = Double.parseDouble(valueStringArray[1]);
				
				if (airportDelayTotalMap.containsKey(keyStringAttribute)) {
					double currentDelay = airportDelayTotalMap.get(keyStringAttribute);
					currentDelay += delay;
					airportDelayTotalMap.replace(keyStringAttribute, currentDelay);
				} else {
					airportDelayTotalMap.put(keyStringAttribute, delay);
				}
				
				if (airportCountMap.containsKey(keyStringAttribute)) {
					int currentCount = airportCountMap.get(keyStringAttribute);
					currentCount += 1;
					airportCountMap.replace(keyStringAttribute, currentCount);
				} else {
					airportCountMap.put(keyStringAttribute, 1);
				}
			}
		} else if (keyString.equals("AIRPORTCODE")) {
			for (Text val : values) {
				airportCodeMap.put(keyStringAttribute, val.toString());
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		context.write(new Text("QUESTION 4: "), new Text("Which cities experience the most weather related delays? Please list the top 10."));
		
		Map<String, Integer> sortedTopAirportsMap =
				airportCountMap.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));
		
		int airportSize = 0;
		
		if (sortedTopAirportsMap.size() > 10) {
			 airportSize = 10;
		} else {
			airportSize = sortedTopAirportsMap.size();
		}
		
		int airportCount = 0;
		for (String airport : sortedTopAirportsMap.keySet()) {
			if (airportCount >= airportSize) {
				break;
			}
			String airportName = airportCodeMap.get(airport);
			double totalDelay = airportDelayTotalMap.get(airport);
			
			airportCount++;
			context.write(new Text(airportCount + ": (" + airport + ") " + airportName), new Text("Total Number of Weather Delays:\t" + airportCountMap.get(airport) + "\tTotal Number of Minutes of Lost to Weather Delays:\t" + totalDelay));
			
		}
		
		/**
		if (airportCountMap.size() > 10) {
			 airportSize = 10;
		} else {
			airportSize = airportCountMap.size();
		}
		
		int airportCount = 0;
		for (String airport : airportCountMap.keySet()) {
			if (airportCount >= airportSize) {
				break;
			}
			String airportName = airportCodeMap.get(airport);
			context.write(new Text(airportName), new Text("Number of Weather Delays: " + airportCountMap.get(airport)));
			airportCount++;
		}
		**/
		
	}
	
}
