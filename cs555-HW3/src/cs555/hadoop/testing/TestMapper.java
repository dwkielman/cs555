package cs555.hadoop.testing;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TestMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().isEmpty()){
			//ArrayList<String> record = DataUtilities.dataReader(value.toString());
			String[] record = value.toString().split(",");
			
			/**
			String year = record.get(1);
			String origin = record.get(17);
			String dest = record.get(18);
			**/
			
			String year = record[0];
			String origin = record[16];
			String dest = record[17];
			
			if (!origin.isEmpty()) {
				context.write(new Text("YEAR:" + year), new Text(origin));
			}
			
			if (!dest.isEmpty()) {
				context.write(new Text("YEAR:" + year), new Text(dest));
			}
		}
	}

}
