package cs555.hadoop.q06;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import cs555.hadoop.Util.DataUtilities;

public class Q06Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	// east coast airports are determined based on this site: http://www.micefinder.com/pays/major-airports-east-coast-u-s-a-en.html
	private static final String[] EAST_COAST_AIRPORTS = { "IAD", "DCA", "ATL", "BWI", "BOS", "CLT", "CAE", "JAX", "MIA", "JFK", "LGA", "EWR", "PHL", "PIT", "RDU", "RIC", "TPA" };
	// west coast airports are determined based on this site: http://www.micefinder.com/pays/major-airports-west-coast-u-s-a-en.html
	private static final String[] WEST_COAST_AIRPORTS = { "ABQ", "LAS", "LAX", "OAK", "PHX", "PDX", "RNO", "MHR", "SLC", "SAN", "SFO", "SJC", "BFI", "SEA" };

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().isEmpty()){
			
			String[] record = value.toString().split(",");
			
			if (record.length >= 18) {
				String arrDelay = record[14];
				String depDelay = record[15];
				String origin = record[16];
				String dest = record[17];
				
				if (!arrDelay.isEmpty()) {
					if (!arrDelay.equals("NA")) {
						double delay = Double.parseDouble(arrDelay);
						if (delay > 0) {
							if (Arrays.stream(EAST_COAST_AIRPORTS).anyMatch(dest::equals)) {
								context.write(new Text("EAST:" + dest), new Text(Double.toString(delay)));
							} else if (Arrays.stream(WEST_COAST_AIRPORTS).anyMatch(dest::equals)) {
								context.write(new Text("WEST:" + dest), new Text(Double.toString(delay)));
							}
						}
					}
				}
				
				if (!depDelay.isEmpty()) {
					if (!depDelay.equals("NA")) {
						double delay = Double.parseDouble(depDelay);
						if (delay > 0) {
							if (Arrays.stream(EAST_COAST_AIRPORTS).anyMatch(origin::equals)) {
								context.write(new Text("EAST:" + origin), new Text(Double.toString(delay)));
							} else if (Arrays.stream(WEST_COAST_AIRPORTS).anyMatch(origin::equals)) {
								context.write(new Text("WEST:" + origin), new Text(Double.toString(delay)));
							}
						}
					}
				}
			}
		}
	}

}
