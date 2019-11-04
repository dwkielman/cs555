package cs555.hadoop.q01;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q01Combiner extends Reducer<Text, Text, Text, Text> {

	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		long numberOfFlights = 0;
		double delayTotal = 0.0;
		
		for (Text delay : values) {
			delayTotal += Double.parseDouble(delay.toString());
			numberOfFlights++;
		}

		context.write(key, new Text("DELAYTOTAL:" + delayTotal + ",NUMFLIGHTS:" + numberOfFlights));

	}
}
