package cs555.hadoop.q05;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q05Reducer extends Reducer<Text, Text, Text, Text> {
	
	private HashMap<String, ArrayList<Double>> carrierTotalCountMap;
	private HashMap<String, String> carrierCodeMap;
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		carrierTotalCountMap = new HashMap<String, ArrayList<Double>>();
		carrierCodeMap = new HashMap<String, String>();
	}
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String[] keyStringArray = key.toString().split(":");
		String keyString = keyStringArray[0];
		String keyStringAttribute = keyStringArray[1];
		
		if (keyString.equals("CARRIER")) {
			for (Text val : values) {
				String[] valueStringArray = val.toString().split(":");
				if (valueStringArray.length == 2) {
					double delay = Double.parseDouble(valueStringArray[1]);
					if (carrierTotalCountMap.containsKey(keyStringAttribute)) {
						ArrayList<Double> currentDelays = carrierTotalCountMap.get(keyStringAttribute);
						currentDelays.add(delay);
						carrierTotalCountMap.replace(keyStringAttribute, currentDelays);
					} else {
						ArrayList<Double> delaysList = new ArrayList<Double>();
						delaysList.add(delay);
						carrierTotalCountMap.put(keyStringAttribute, delaysList);
					}
				}
			}
		} else if (keyString.equals("CARRIERCODE")) {
			for (Text val : values) {
				carrierCodeMap.put(keyStringAttribute, val.toString());
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		context.write(new Text("QUESTION 5: "), new Text("Which carriers have the most delays?"));

		HashMap<String, Integer> carrierTotals = new HashMap<String, Integer>();
		HashMap<String, Double> carrierAverageTotals = new HashMap<String, Double>();
		
		for (String carrier : carrierTotalCountMap.keySet()) {
			double sum = carrierTotalCountMap.get(carrier).stream().mapToDouble(Double::doubleValue).sum();
			int delayCount = carrierTotalCountMap.get(carrier).size();
			
			double mean = sum / delayCount;
			
			carrierTotals.put(carrier, delayCount);
			carrierAverageTotals.put(carrier, mean);
		}
		
		carrierTotals.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue().reversed());

		Map<String, Integer> sortedTopAirportsMap =
				carrierTotals.entrySet().stream()
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
			String airportName = carrierCodeMap.get(airport);
			double sum = carrierTotalCountMap.get(airport).stream().mapToDouble(Double::doubleValue).sum();
			double average = sum / carrierTotals.get(airport);
			airportCount++;
			
			context.write(new Text(airportCount + ": (" + airport + ") " + airportName), new Text("Total Number of Carrier Delays:\t" + carrierTotals.get(airport) + "\tTotal Number of Minutes that were Lost to Delays:\t" + sum + "\tAverage Number of Minutes that were Lost to Delays:\t" + average));
			
			//context.write(new Text(airportCount + ": (" + airport + ") " + airportName), new Text("Total Number of Minutes that were Lost to Delays: " + Double.toString(sum)));
			
			//context.write(new Text(airportCount + ": (" + airport + ") " + airportName), new Text("Average Number of Minutes that were Lost to Delays: " + Double.toString(average)));
		}
		
		/**
		int carrierSize = 0;
		
		if (carrierTotals.size() > 10) {
			carrierSize = 10;
		} else {
			carrierSize = carrierTotals.size();
		}
		
		int carrierCount = 0;
		for (String carrier : carrierTotals.keySet()) {
			if (carrierCount >= carrierSize) {
				break;
			}
			String carrierName = carrierCodeMap.get(carrier);
			context.write(new Text(carrierName), new Text("Number of Carrier Delays: " + carrierTotals.get(carrier)));
			int sum = carrierTotalCountMap.get(carrierName).stream().mapToInt(Integer::intValue).sum();
			context.write(new Text(carrierName), new Text("Total Number of Minutes taht were Lost to Delays: " + sum));
			carrierCount++;
		}
		 **/
		if (!carrierAverageTotals.isEmpty()) {
			String mostDelayCarrier = carrierAverageTotals.entrySet().stream().max((entry1, entry2) -> entry1.getValue() > entry2.getValue() ? 1 : -1).get().getKey();
			
			String airportName = carrierCodeMap.get(mostDelayCarrier);
			double sum = carrierTotalCountMap.get(mostDelayCarrier).stream().mapToDouble(Double::doubleValue).sum();
			double average = carrierAverageTotals.get(mostDelayCarrier);
			
			context.write(new Text("Which carrier has the highest average delay?"), new Text());
			context.write(new Text("(" + mostDelayCarrier + ") " + airportName), new Text("Total Number of Carrier Delays:\t" + carrierTotals.get(mostDelayCarrier) + "\tTotal Number of Minutes that were Lost to Delays:\t" + sum + "\tAverage Number of Minutes that were Lost to Delays:\t" + average));
			
			//context.write(new Text("(" + mostDelayCarrier + ") " + airportName), new Text("Total Number of Minutes that were Lost to Delays: " + Double.toString(sum)));
			
			//context.write(new Text("(" + mostDelayCarrier + ") " + airportName), new Text("Average Number of Minutes that were Lost to Delays: " + Double.toString(average)));
		}
	}

}
