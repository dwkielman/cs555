package cs555.hadoop.hw3;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs555.hadoop.Util.DataUtilities;

public class HW3AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().isEmpty()){
			ArrayList<String> record = DataUtilities.dataReader(value.toString());
			
			String year = record.get(1);
			String month = record.get(2);
			String dayOfMonth = record.get(3);
			String dayOfWeek = record.get(4);
			String carrierCode = record.get(9);
			String flightNumber = record.get(10);
			String tailNumber = record.get(11);
			String originAirportCode = record.get(17);
			
			if (!year.isEmpty() && !month.isEmpty() && !dayOfMonth.isEmpty() && !dayOfWeek.isEmpty() && !carrierCode.isEmpty() && !flightNumber.isEmpty() && !tailNumber.isEmpty()) {
				String flightCode = year + "," + month + "," + dayOfMonth + "," + dayOfWeek + "," + flightNumber;
				
				String airportInfo = "HW3NA";
				String carrierInfo = "HW3NA";
				
				//context.write(new Text(flightCode), new Text("analysis\t" + carrierCode + "," + tailNumber + "," + airportInfo + "," + carrierInfo));
				context.write(new Text(flightCode), new Text(carrierCode + "," + tailNumber + "," + airportInfo + "," + carrierInfo));
			}
		}
	}

}
