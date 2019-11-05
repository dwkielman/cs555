package cs555.hadoop.Util;

import java.util.ArrayList;

/**
 * Centralized class to help with parsing and handling data-specific functions when going through the various datasets 
 */

public class DataUtilities {
	
	public static ArrayList<String> dataReader(String s) {
	    ArrayList<String> entries = new ArrayList<String>();
	    
	    int charCounter = 0;
	    boolean isInCell = true;
	    char cellDelimiter = ',';
	    char quoteDelimiter = '"';
	    
	    for (int i=0; i < s.length(); i++) {
	    	if ((s.charAt(i) == cellDelimiter) && isInCell){
	    		String entry = s.substring(charCounter, i);
	    		entry = entry.replaceAll("^\"|\"$", "");
	    		//entries.add(s.substring(charCounter, i));
	    		entries.add(entry);
	    		charCounter = i + 1;
	    	} else if (s.charAt(i) == quoteDelimiter) {
	    		isInCell = !isInCell;
	    	}
	    }
	    
	    entries.add(s.substring(charCounter));
	    return entries;
	}

}