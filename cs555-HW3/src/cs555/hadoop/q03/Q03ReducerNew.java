package cs555.hadoop.q03;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q03ReducerNew extends Reducer<Text, Text, Text, Text> {

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
					if (tempAirportCountMap.containsKey(iata)) {
						int currentCount = tempAirportCountMap.get(iata);
						currentCount++;
						tempAirportCountMap.replace(iata, currentCount);
					} else {
						tempAirportCountMap.put(iata, 1);
					}
				}
			} else if (keyString.equals("AIRPORTCODE")) {
				for (Text val : values) {
					airportCodeMap.put(keyStringAttribute, val.toString());
				}
			}
			
			if (keyString.equals("YEAR")) {
				yearCountMap.put(Integer.parseInt(keyStringAttribute), tempAirportCountMap);
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		context.write(new Text("QUESTION 3: "), new Text("What are the major hubs (busiest airports) in continental U.S.? Has there been a change over the 21-year period covered by this dataset?"));
		
		for (int year : yearCountMap.keySet()) {
			HashMap<String, Integer> yearsAirportCount = yearCountMap.get(year);
			for (String iata : yearsAirportCount.keySet()) {
				if (airportCodeMap.containsKey(iata)) {
					int airportFlightCount = yearsAirportCount.get(iata);
					//String airportInfo = airportCodeMap.get(iata);
					
					if (airportCountMap.containsKey(iata)) {
						int currentCount = airportCountMap.get(iata);
						currentCount += airportFlightCount;
						airportCountMap.replace(iata, currentCount);
					} else {
						airportCountMap.put(iata, airportFlightCount);
					}
				}
			}
					
			Map<String, Integer> sortedTopAirportsByYearMap =
					yearsAirportCount.entrySet().stream()
                    .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                            (e1, e2) -> e1, LinkedHashMap::new));
			
			context.write(new Text(""), new Text(""));
			context.write(new Text("Year: "), new Text(Integer.toString(year)));
			
			int airportSize = 0;
			
			if (yearsAirportCount.size() > 10) {
				 airportSize = 10;
			} else {
				airportSize = sortedTopAirportsByYearMap.size();
			}
			
			int airportCount = 0;
			for (String airport : sortedTopAirportsByYearMap.keySet()) {
				if (airportCount >= airportSize) {
					break;
				}
				int flightCount = sortedTopAirportsByYearMap.get(airport);
				String airportName = airportCodeMap.get(airport);
				
				airportCount++;
				context.write(new Text(airportCount + ": (" + airport + ") " + airportName), new Text("Number of Flights:\t" + flightCount));
				
			}
		}
		
		Map<String, Integer> sortedTopAirportsMap =
				airportCountMap.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));
		
		context.write(new Text(""), new Text(""));
		context.write(new Text("QUESTION 3 (continued): "), new Text("What are the major hubs (busiest airports) in continental U.S.? Please list the top 10."));
		
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
			int flightCount = sortedTopAirportsMap.get(airport);
			String airportName = airportCodeMap.get(airport);
			
			airportCount++;
			context.write(new Text(airportCount + ": (" + airport + ") " + airportName), new Text("Number of Flights:\t" + flightCount));
			
		}
	}

}
