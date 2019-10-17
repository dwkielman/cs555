package cs555.hadoop.hw3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import cs555.hadoop.Util.DataUtilities;

public class HW3Reducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
	}
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		
		/**
		 * Q1: What is the best time-of-the-day/day-of-week/time-of-year to fly to minimize delays?
		 */
		
		
		
		/**
		 * Q2: What is the worst time-of-the-day / day-of-week/time-of-year to fly to minimize delays?
		 */
		
		
		
		/**
		 * Q3: What are the major hubs (busiest airports) in continental U.S.? Please list the top 10. Has there
		 * been a change over the 21-year period covered by this dataset?
		 */
		
		
		
		/**
		 * Q4: Which cities experience the most weather related delays? Please list the top 10.
		 */
		
		
		
		/**
		 * Q5: Which carriers have the most delays? You should report on the total number of delayed flights
		 * and also the total number of minutes that were lost to delays. Which carrier has the highest average
		 * delay?
		 */
		
		
		
		/**
		 * Q6: Do older planes cause more delays? Include details to substantiate your analysis.
		 */
		
		
		
		
		/**
		 * Q7: What are the aspects that you can infer from the LateAircraftDelay field? Develop a program that
		 * harnesses this field.
		 */
		

		
		
	}
	
}
