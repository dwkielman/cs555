package cs555.hadoop.q03;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q03Reducer extends Reducer<Text, Text, Text, Text> {

	private HashMap<String, Integer> airportCountMap;
	private HashMap<String, String> airportInfoMap;
	private HashMap<Integer, HashMap<String, Integer>> yearCountMap;
	private HashMap<String, Integer> debugMMap;
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		airportCountMap = new HashMap<String, Integer>();
		airportInfoMap = new HashMap<String, String>();
		yearCountMap = new HashMap<Integer, HashMap<String, Integer>>();
		debugMMap = new HashMap<String, Integer>();
	}
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String[] keyStringArray = key.toString().split(":");
		String keyString = keyStringArray[0];
		String keyStringAttribute = keyStringArray[1];
		
		HashMap<String, Integer> tempAirportCountMap = new HashMap<String, Integer>();
		
		int count = 0;
		
		for (Text value : values) {
			count++;
		}
		
		debugMMap.put(key.toString(), count);
		
		for (Text value : values) {
			String[] parts = value.toString().split(",");
			
			String iata = "";
			int flightCount = 0;
			String city = "";
			String state = "";
			String name = "";
			
			for (int i = 0; i < parts.length; i++) {
				
				String[] info = parts[i].split(":");
				if (info.length == 2) {
					if (info[0].equals("AIRPORTCODE")) {
						iata = info[1];
					} else if (info[0].equals("FLIGHTCOUNT")) {
						flightCount = Integer.parseInt(info[1]);
						/**
					} else if (info[0].equals("AIRPORT")) {
						name = info[1];
					} else if (info[0].equals("CITY")) {
						city = info[1];
						**/
					} else if (info[0].equals("STATE")) {
						state = info[1];
					}
				}
			}
			
			if (airportCountMap.containsKey(iata)) {
				int currentCount = airportCountMap.get(iata);
				currentCount += flightCount;
				airportCountMap.replace(iata, currentCount);
			} else {
				airportCountMap.put(iata, flightCount);
			}
			
			tempAirportCountMap.put(iata, flightCount);
			
			if (!airportInfoMap.containsKey(iata)) {
				airportInfoMap.put(iata, name + " , " + city + ", " + state);
			}

		}
		
		yearCountMap.put(Integer.parseInt(keyStringAttribute), tempAirportCountMap);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		context.write(new Text("QUESTION 3: "), new Text("What are the major hubs (busiest airports) in continental U.S.? Please list the top 10."));
		
		context.write(new Text("DEBUG: "), new Text("Total number of entries in airportCountMap: " + airportCountMap.size()));
		
		context.write(new Text("DEBUG: "), new Text("Total number of entries in debugMMap: " + debugMMap.size()));
		
		if (!debugMMap.isEmpty()) {
			int count = 0;
			for (String s : debugMMap.keySet()) {
				if (count > 1) {
					break;
				} else {
					context.write(new Text("DEBUG: "), new Text("debugMMap Key: " + s));
					context.write(new Text("DEBUG: "), new Text("debugMMap Entry: " + debugMMap.get(s)));
				}
				count++;
			}
		}
		
		Map<String, Integer> sortedTopAirportsMap =
				airportCountMap.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                (e1, e2) -> e1, LinkedHashMap::new));
		
		context.write(new Text(""), new Text(""));
		
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
			String airportName = airportInfoMap.get(airport);
			
			context.write(new Text(airportName), new Text("Flights: " + flightCount));
			airportCount++;
		}
		
		context.write(new Text("QUESTION 3 (continued): "), new Text("Has there been a change over the 21-year period covered by this dataset?"));
		
		for (Integer year : yearCountMap.keySet()) {
			HashMap<String, Integer> yearCounts = yearCountMap.get(year);
			
			Map<String, Integer> sortedTopAirportsByYearMap =
					yearCounts.entrySet().stream()
	                        .sorted(Map.Entry.comparingByValue())
	                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
	                                (e1, e2) -> e1, LinkedHashMap::new));
			
			context.write(new Text(""), new Text(""));
			context.write(new Text("Year: "), new Text(year.toString()));
			
			airportSize = 0;
			
			if (yearCounts.size() > 10) {
				 airportSize = 10;
			} else {
				airportSize = sortedTopAirportsByYearMap.size();
			}
			
			airportCount = 0;
			for (String airport : sortedTopAirportsByYearMap.keySet()) {
				if (airportCount >= airportSize) {
					break;
				}
				int flightCount = sortedTopAirportsByYearMap.get(airport);
				String airportName = airportInfoMap.get(airport);
				
				context.write(new Text(airportName), new Text("Flights: " + flightCount));
				airportCount++;
			}
		}
	}
	
}
