package cs555.hadoop.q07;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q07Reducer extends Reducer<Text, Text, Text, Text> {
	
	private HashMap<String, String> airportCodeMap;
	private HashMap<Integer, HashMap<String, ArrayList<Double>>> yearCountMap;
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		airportCodeMap = new HashMap<String, String>();
		yearCountMap = new HashMap<Integer, HashMap<String, ArrayList<Double>>>();
	}
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		HashMap<String, ArrayList<Double>> tempAirportCountMap = new HashMap<String, ArrayList<Double>>();
		
		String[] keyStringArray = key.toString().split(":");
		if (keyStringArray.length == 2) {
			String keyString = keyStringArray[0];
			String keyStringAttribute = keyStringArray[1];
			
			if (keyString.equals("YEAR")) {
				for (Text val : values) {
					String[] parts = val.toString().split(",");
					String iata = "";
					double delay = 0.0;
					
					for (int i = 0; i < parts.length; i++) {
						String[] delayInfo = parts[i].split(":");
						if (delayInfo.length == 2) {
							if (delayInfo[0].equals("CITY")) {
								iata = delayInfo[1];
							} else if (delayInfo[0].equals("LATEAIRCRAFTDELAY")) {
								delay = Double.parseDouble(delayInfo[1]);
							}
						}
					}
					
					if (iata != "") {
						if (tempAirportCountMap.containsKey(iata)) {
							ArrayList<Double> currentDelays = tempAirportCountMap.get(iata);
							currentDelays.add(delay);
							tempAirportCountMap.replace(iata, currentDelays);
						} else {
							ArrayList<Double> delaysList = new ArrayList<Double>();
							delaysList.add(delay);
							tempAirportCountMap.put(iata, delaysList);
						}
						
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
		
		context.write(new Text("QUESTION 7: "), new Text("What are the aspects that you can infer from the LateAircraftDelay field?"));
		
		context.write(new Text("QUESTION 7: "), new Text("What are the airports with the most delays due to Late Aircrafts? Has there been a change over the 21-year period covered by this dataset?"));
		
		HashMap<String, Integer> airportTotals = new HashMap<String, Integer>();
		HashMap<String, Double> airportTotalLateAircraftDelay = new HashMap<String, Double>();
		HashMap<String, Double> airportAverageTotals = new HashMap<String, Double>();
		
		for (int year : yearCountMap.keySet()) {
			HashMap<String, ArrayList<Double>> yearsAirportCount = yearCountMap.get(year);
			
			Map<String, Double> cityWithTotalMap = new HashMap<String, Double>();
			Map<String, Integer> cityWithCountMap = new HashMap<String, Integer>();
			
			for (String iata : yearsAirportCount.keySet()) {
				if (airportCodeMap.containsKey(iata)) {
					
					double currentSum = yearsAirportCount.get(iata).stream().mapToDouble(Double::doubleValue).sum();
					int currentCount = yearsAirportCount.get(iata).size();

					if (airportTotalLateAircraftDelay.containsKey(iata)) {
						double sum = airportTotalLateAircraftDelay.get(iata);
						sum += currentSum;
						airportTotalLateAircraftDelay.replace(iata, sum);
					} else {
						airportTotalLateAircraftDelay.put(iata, currentSum);
					}
					
					if (airportTotals.containsKey(iata)) {
						int count = airportTotals.get(iata);
						count += currentCount;
						airportTotals.replace(iata, count);
					} else {
						airportTotals.put(iata, currentCount);
					}
					
					cityWithTotalMap.put(iata, currentSum);
					cityWithCountMap.put(iata, currentCount);
				}
			}
					
			Map<String, Integer> sortedTopAirportsByYearMap =
					cityWithCountMap.entrySet().stream()
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
				int delayCount = cityWithCountMap.get(airport);
				String airportName = airportCodeMap.get(airport);
				double totalDelay = cityWithTotalMap.get(airport);
				double averageDelay = totalDelay / delayCount;
				
				airportCount++;
				context.write(new Text(airportCount + ": (" + airport + ") " + airportName), new Text("Total Number of Late Aircrafts:\t" + delayCount + "\tTotal Number of Minutes that were Lost to Late Aircrafts:\t" + totalDelay + "\tAverage Number of Minutes that were Lost to Late Aircrafts:\t" + averageDelay));
			}
		}
		
		Map<String, Integer> sortedTopAirportsrMap =
				airportTotals.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));
		
		int airportSize = 0;
		
		if (sortedTopAirportsrMap.size() > 10) {
			 airportSize = 10;
		} else {
			airportSize = sortedTopAirportsrMap.size();
		}
		
		int airportCount = 0;
		
		context.write(new Text(""), new Text(""));
		context.write(new Text("Which airlines experience the most late aircraft related delays? "), new Text("Please list the top 10."));
		
		for (String airport : sortedTopAirportsrMap.keySet()) {
			if (airportCount >= airportSize) {
				break;
			}
			int delayCount = airportTotals.get(airport);
			String airportName = airportCodeMap.get(airport);
			double totalDelay = airportTotalLateAircraftDelay.get(airport);
			double averageDelay = totalDelay / delayCount;
			
			airportCount++;
			context.write(new Text(airportCount + ": (" + airport + ") " + airportName), new Text("Total Number of Late Aircrafts:\t" + delayCount + "\tTotal Number of Minutes that were Lost to Late Aircrafts:\t" + totalDelay + "\tAverage Number of Minutes that were Lost to Late Aircrafts:\t" + averageDelay));
		}
		
		if (!airportTotals.isEmpty() && !airportTotalLateAircraftDelay.isEmpty()) {

			for (String s : airportTotals.keySet()) {
				double mean = airportTotalLateAircraftDelay.get(s) / airportTotals.get(s);
				airportAverageTotals.put(s, mean);
			}
			
			if (!airportAverageTotals.isEmpty()) {
				context.write(new Text(""), new Text(""));
				context.write(new Text("Which city has the highest average late aircraft delay across all data?"), new Text());
				String mostDelayAirline = airportAverageTotals.entrySet().stream().max((entry1, entry2) -> entry1.getValue() > entry2.getValue() ? 1 : -1).get().getKey();
				int delayCount = airportTotals.get(mostDelayAirline);
				double totalDelay = airportTotalLateAircraftDelay.get(mostDelayAirline);
				double averageDelay = airportAverageTotals.get(mostDelayAirline);
				
				String airportName = airportCodeMap.get(mostDelayAirline);
				
				context.write(new Text("(" + mostDelayAirline + ") " + airportName), new Text("Total Number of Late Aircrafts:\t" + delayCount  + "\tTotal Number of Minutes that were Lost to Late Aircrafts:\t" + totalDelay + "\tAverage Number of Minutes that were Lost to Late Aircrafts:\t" + averageDelay));
			}
		}
		
	}

}
