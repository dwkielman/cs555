package cs555.hadoop.Util;


import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;

/**
 * Centralized class to help with parsing and handling data-specific functions when going through the various datasets 
 */

public class DataUtilities {

	public static double doubleReader(String s) {
		try {
			return Double.parseDouble(s);
	    } catch ( NumberFormatException e ) {
	    	return 0;
	    }
	}
	
	public static ArrayList<String> dataReader(String s) {
	    ArrayList<String> entries = new ArrayList<String>();
	    
	    int charCounter = 0;
	    boolean isInCell = true;
	    char cellDelimiter = ',';
	    char quoteDelimiter = '"';
	    
	    for (int i=0; i < s.length(); i++) {
	    	if ((s.charAt(i) == cellDelimiter) && isInCell){
	    		entries.add(s.substring(charCounter, i));
	    		charCounter = i + 1;
	    	} else if (s.charAt(i) == quoteDelimiter) {
	    		isInCell = !isInCell;
	    	}
	    }
	    
	    entries.add(s.substring(charCounter));
	    return entries;
	}
	
	public static double roundDouble(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    BigDecimal bd = new BigDecimal(value);
	    bd = bd.setScale(places, RoundingMode.HALF_UP);
	    return bd.doubleValue();
	}
	
	public static ArrayList<Double> segmentsMaker(String s) {
		ArrayList<Double> segments = new ArrayList<Double>();
		
		String[] parts = s.trim().split("\\s+");
		for (int i = 0; i < parts.length; i++) {
			double d = doubleReader(parts[i]);
			segments.add(d);
		}
		
		return segments;
	}
	
	public static double getAverageValue(ArrayList<Double> doubles) {
		double average = 0.0;
		
		if (doubles.size() > 0) {
			double total = 0.0;
			for (int i = 0; i < doubles.size(); i++) {
				total += doubles.get(i);
			}
			
			average = total / doubles.size();
		}
		
		return average;
	}
}