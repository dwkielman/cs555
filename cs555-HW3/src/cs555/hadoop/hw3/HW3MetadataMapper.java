package cs555.hadoop.hw3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

import cs555.hadoop.Util.Airport;
import cs555.hadoop.Util.Carrier;
import cs555.hadoop.Util.DataUtilities;

public class HW3MetadataMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	//Map<String, String> airportsData = new HashMap<String, String>();
    //Map<String, String> carriersData = new HashMap<String, String>();
	/**
    ArrayList<String> airports = new ArrayList<String>();
    ArrayList<String> carriers = new ArrayList<String>();
    
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		
		FileSplit fsFileSplit = (FileSplit) context.getInputSplit();
		String filename = context.getConfiguration().get(fsFileSplit.getPath().getParent().getName());
		
		if (filename.equals("airports.csv")) {
			Path path = fsFileSplit.getPath();
			BufferedReader airportsFile = new BufferedReader(new FileReader(path.toString()));
			// airports.csv
			String airportRecord = airportsFile.readLine();
			while((airportRecord = airportsFile.readLine()) != null) {
				ArrayList<String> record = DataUtilities.dataReader(airportRecord);
				String code = record.get(1);
				String name = record.get(2);
				String city = record.get(3);
				airports.add(code + "," + name + "," + city);
				//airportsData.put(code, name + "," + city);
			}
		} else if (filename.equals("carriers.csv")) {
			Path path = fsFileSplit.getPath();
			BufferedReader carriersFile = new BufferedReader(new FileReader(path.toString()));
			// carriers.csv
			String carriersRecord = carriersFile.readLine();
			while((carriersRecord = carriersFile.readLine()) != null) {
				ArrayList<String> record = DataUtilities.dataReader(carriersRecord);
				String code = record.get(1);
				String description = record.get(2);
				carriers.add(code + "," + description);
				//carriersData.put(code, description);
			}
		}
		/**
		if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
			
			URI[] cacheFiles = context.getCacheFiles();
			
			if (cacheFiles != null && cacheFiles.length > 0) {
				Path airportsFilePath = new Path(cacheFiles[0].getPath());
				Path carriersFilePath = new Path(cacheFiles[1].getPath());
				//Path planesDataFilePath = new Path(cacheFiles[2].getPath());
				BufferedReader airportsFile = new BufferedReader(new FileReader(airportsFilePath.toString()));
				BufferedReader carriersFile = new BufferedReader(new FileReader(carriersFilePath.toString()));
				//BufferedReader planesDataFile = new BufferedReader(new FileReader(planesDataFilePath.toString()));
				
				// airports.csv
				String airportRecord = airportsFile.readLine();
				while((airportRecord = airportsFile.readLine()) != null) {
					ArrayList<String> record = DataUtilities.dataReader(airportRecord);
					String code = record.get(1);
					String name = record.get(2);
					String city = record.get(3);
					airportsData.put(code, name + "," + city);
				}
				
				// carriers.csv
				String carriersRecord = carriersFile.readLine();
				while((carriersRecord = carriersFile.readLine()) != null) {
					ArrayList<String> record = DataUtilities.dataReader(carriersRecord);
					String code = record.get(1);
					String description = record.get(2);
					carriersData.put(code, description);
				}
				
				// planes-data.csv
				/**
				String planesRecord = planesDataFile.readLine();
				while((planesRecord = planesDataFile.readLine()) != null) {
					ArrayList<String> record = DataUtilities.dataReader(planesRecord);
					String tailNumber = record.get(1);
					// year = (int) DataUtilities.doubleReader(parts[1]);
					String year = "HW3NA";
					if (record.size() >= 9) {
						year = record.get(9);
					}
					planeData.put(tailNumber, year);
				}
			}
			@SuppressWarnings("deprecation")
			Path[] localPaths = context.getLocalCacheFiles();
		    BufferedReader airportsFile = new BufferedReader(new FileReader(localPaths[0].toString()));
		    BufferedReader carriersFile = new BufferedReader(new FileReader(localPaths[1].toString()));
		    BufferedReader planesFile = new BufferedReader(new FileReader(localPaths[2].toString()));
		}
		
		super.setup(context);
	}
	 **/
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String keyTag = "HW3NA";
		String data = "HW3NA";
		
		if(!value.toString().isEmpty()) {
			ArrayList<String> record = DataUtilities.dataReader(value.toString());
			keyTag = record.get(0);
			data = record.get(1);
		}
		
		context.write(new Text(keyTag), new Text(data));
		
		/**
		
		if (!airports.isEmpty()) {
			for (String s : airports) {
				String[] keyRecords = s.toString().split(",");
				keyTag = keyRecords[0];
				data = keyRecords[1];
			}
		} else if (!carriers.isEmpty()) {
			for (String s : carriers) {
				String[] keyRecords = s.toString().split(",");
				keyTag = keyRecords[0];
				data = keyRecords[1];
			}
		}
		**/
		
	}
}