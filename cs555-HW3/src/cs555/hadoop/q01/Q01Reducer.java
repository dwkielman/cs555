package cs555.hadoop.q01;

import java.io.IOException;
import java.time.DayOfWeek;
import java.time.Month;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q01Reducer extends Reducer<Text, Text, Text, Text> {

	private HashMap<String, Double> hourDelayMap;
	private HashMap<String, Double> dayDelayMap;
	private HashMap<String, Double> monthDelayMap;
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		hourDelayMap = new HashMap<String, Double>();
		dayDelayMap = new HashMap<String, Double>();
		monthDelayMap = new HashMap<String, Double>();
	}
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		long numberOfFlights = 0;
		double delayTotal = 0.0;
		
		for (Text value : values) {
			String[] parts = value.toString().split(",");
			for (int i = 0; i < parts.length; i++) {
				String[] delayInfo = parts[i].split(":");
				if (delayInfo.length == 2) {
					if (delayInfo[0].equals("DELAYTOTAL")) {
						double d = Double.parseDouble(delayInfo[1]);
						delayTotal += d;
					} else if (delayInfo[0].equals("NUMFLIGHTS")) {
						long l = Long.parseLong(delayInfo[1]);
						numberOfFlights += l;
					}
				}
			}
		}
		
		double meanDelayTotal = delayTotal / numberOfFlights;
		
		String[] keyStringArray = key.toString().split(":");
		
		if (keyStringArray.length == 2) {
			String keyString = keyStringArray[0];
			String keyStringAttribute = keyStringArray[1];
			
			if (!keyString.isEmpty() && !keyStringAttribute.isEmpty()) {
				if (keyString.equals("HOUR")) {
					hourDelayMap.put(keyStringAttribute, meanDelayTotal);
				} else if (keyString.equals("DAY")) {
					int day = Integer.parseInt(keyStringAttribute);
					if (day <= 7 && day >= 1) {
						String dayOfWeek = DayOfWeek.of(day).toString();
						dayDelayMap.put(dayOfWeek, meanDelayTotal);
					}
				} else if (keyString.equals("MONTH")) {
					int m = Integer.parseInt(keyStringAttribute);
					if (m <= 12 && m >= 1) {
						String month = Month.of(m).name();
						monthDelayMap.put(month, meanDelayTotal);
					}
				}
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		context.write(new Text("QUESTION 1: "), new Text("What is the best time-of-the-day/day-of-week/time-of-year to fly to minimize delays?"));

		Map<String, Double> sortedHourDelayMap =
        		hourDelayMap.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                (e1, e2) -> e1, LinkedHashMap::new));
		
		context.write(new Text(""), new Text(""));
		context.write(new Text("Time-of-the-day"), new Text("(ordered from best to worst):"));
		for (Entry<String, Double> entry : sortedHourDelayMap.entrySet()) {
			context.write(new Text("HOUR " + entry.getKey()), new Text("DELAY:\t" + Double.toString(entry.getValue())));
		}
		
		Map<String, Double> sortedDayDelayMap =
				dayDelayMap.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                (e1, e2) -> e1, LinkedHashMap::new));
		
		context.write(new Text(""), new Text(""));
		context.write(new Text("Day-of-week"), new Text("(ordered from best to worst):"));
		for (Entry<String, Double> entry : sortedDayDelayMap.entrySet()) {
			context.write(new Text(entry.getKey()), new Text("\tDELAY:\t" + Double.toString(entry.getValue())));
		}
		
		Map<String, Double> sortedMonthDelayMap =
				monthDelayMap.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                (e1, e2) -> e1, LinkedHashMap::new));
		
		context.write(new Text(""), new Text(""));
		context.write(new Text("Time-of-year"), new Text("(ordered from best to worst):"));
		for (Entry<String, Double> entry : sortedMonthDelayMap.entrySet()) {
			context.write(new Text(entry.getKey()), new Text("\tDELAY:\t" + Double.toString(entry.getValue())));
		}
		
	}
}
