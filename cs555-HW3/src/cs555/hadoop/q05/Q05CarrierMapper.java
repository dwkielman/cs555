package cs555.hadoop.q05;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs555.hadoop.Util.DataUtilities;

public class Q05CarrierMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().isEmpty()){
			ArrayList<String> record = DataUtilities.dataReader(value.toString());
			
			String code = record.get(0);
			String description = record.get(1);
			
			if (!code.isEmpty() && !description.isEmpty()) {
				context.write(new Text("CARRIERCODE:" + code), new Text(description));
			}
		}
	}
}
