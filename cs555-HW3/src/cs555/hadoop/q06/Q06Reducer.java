package cs555.hadoop.q06;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q06Reducer extends Reducer<Text, Text, Text, Text> {

	private ArrayList<Double> eastCoastTotalList;
	private ArrayList<Double> westCoastTotalList;
	private HashMap<String, Double> westAirportTotalMap;
	private HashMap<String, Double> eastAirportTotalMap;
	private HashMap<String, Integer> westAirportCountMap;
	private HashMap<String, Integer> eastAirportCountMap;
	private HashMap<String, String> airportCodeMap;

	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		eastCoastTotalList = new ArrayList<Double>();
		westCoastTotalList = new ArrayList<Double>();
		westAirportTotalMap = new HashMap<String, Double>();
		eastAirportTotalMap = new HashMap<String, Double>();
		westAirportCountMap = new HashMap<String, Integer>();
		eastAirportCountMap = new HashMap<String, Integer>();
		airportCodeMap = new HashMap<String, String>();
	}
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String[] keyStringArray = key.toString().split(":");
		if (keyStringArray.length == 2) {
			String keyString = keyStringArray[0];
			String keyStringAttribute = keyStringArray[1];
			
			if (keyString.equals("EAST")) {
				for (Text val : values) {
					double delay = Double.parseDouble(val.toString());
					if (eastAirportTotalMap.containsKey(keyStringAttribute)) {
						double currentDelay = eastAirportTotalMap.get(keyStringAttribute);
						currentDelay += delay;
						eastAirportTotalMap.replace(keyStringAttribute, currentDelay);
					} else {
						eastAirportTotalMap.put(keyStringAttribute, delay);
					}
					
					if (eastAirportCountMap.containsKey(keyStringAttribute)) {
						int currentCount = eastAirportCountMap.get(keyStringAttribute);
						currentCount++;
						eastAirportCountMap.replace(keyStringAttribute, currentCount);
					} else {
						eastAirportCountMap.put(keyStringAttribute, 1);
					}
					
					eastCoastTotalList.add(delay);
				}
				
			} else if (keyString.equals("WEST")) {
				for (Text val : values) {
					double delay = Double.parseDouble(val.toString());
					if (westAirportTotalMap.containsKey(keyStringAttribute)) {
						double currentDelay = westAirportTotalMap.get(keyStringAttribute);
						currentDelay += delay;
						westAirportTotalMap.replace(keyStringAttribute, currentDelay);
					} else {
						westAirportTotalMap.put(keyStringAttribute, delay);
					}
					
					if (westAirportCountMap.containsKey(keyStringAttribute)) {
						int currentCount = westAirportCountMap.get(keyStringAttribute);
						currentCount++;
						westAirportCountMap.replace(keyStringAttribute, currentCount);
					} else {
						westAirportCountMap.put(keyStringAttribute, 1);
					}
					
					westCoastTotalList.add(delay);
				}
				
			} else if (keyString.equals("AIRPORTCODE")) {
				for (Text val : values) {
					airportCodeMap.put(keyStringAttribute, val.toString());
				}
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		context.write(new Text("QUESTION 6: "), new Text("Do East or West coast airports have more delays?"));
		context.write(new Text(""), new Text(""));
		String winner = "";
		int totalDelayed = 0;
				
		if (eastCoastTotalList.size() > westCoastTotalList.size()) {
			winner = "East";
			totalDelayed = eastCoastTotalList.size();
		} else {
			winner = "West";
			totalDelayed = westCoastTotalList.size();
		}
		
		double eastSum = eastCoastTotalList.stream()
			    .mapToDouble(a -> a)
			    .sum();
		
		double westSum = westCoastTotalList.stream()
			    .mapToDouble(a -> a)
			    .sum();
		
		// get the airport name and use it to display who has the most
		Map<String, Integer> eastSortedTopAirportsByYearMap =
				eastAirportCountMap.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));
		
		Map<String, Integer> westSortedTopAirportsByYearMap =
				westAirportCountMap.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));
		
		context.write(new Text("East Coast Airports:"), new Text("Total Number of Minutes Lost to Delays: " + eastSum));
		context.write(new Text("East Coast Airports:"), new Text("Total Number of Delays: " + eastCoastTotalList.size()));
		
		context.write(new Text(""), new Text(""));
		
		int airportCount = 1;
		for (String iata : eastSortedTopAirportsByYearMap.keySet()) {
			double totalMinutes = eastAirportTotalMap.get(iata);
			int totalCount = eastAirportCountMap.get(iata);
			
			if (airportCodeMap.containsKey(iata)) {
				String airportName = airportCodeMap.get(iata);
				
				context.write(new Text(airportCount + ": (" + iata + ") " + airportName), new Text("Total Number of Delays:\t" + totalCount + "\tTotal Number of Minutes that were Lost to Delays:\t" + totalMinutes));
				airportCount++;
			}
		}
		
		context.write(new Text(""), new Text(""));
		
		context.write(new Text("West Coast Airports:"), new Text("Total Number of Minutes Lost to Delays: " + westSum));
		context.write(new Text("West Coast Airports:"), new Text("Total Number of Delays: " + westCoastTotalList.size()));
		
		context.write(new Text(""), new Text(""));
		
		airportCount = 1;
		for (String iata : westSortedTopAirportsByYearMap.keySet()) {
			double totalMinutes = westAirportTotalMap.get(iata);
			int totalCount = westAirportCountMap.get(iata);
			
			if (airportCodeMap.containsKey(iata)) {
				String airportName = airportCodeMap.get(iata);
				
				context.write(new Text(airportCount + ": (" + iata + ") " + airportName), new Text("Total Number of Delays:\t" + totalCount + "\tTotal Number of Minutes that were Lost to Delays:\t" + totalMinutes));
				airportCount++;
			}
		}
		
		context.write(new Text(""), new Text(""));
		context.write(new Text(winner + " Coast airports have more Delays."), new Text("Total number of delayed flights: " + totalDelayed));
	}
}
