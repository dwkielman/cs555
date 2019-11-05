package cs555.hadoop.q01;

import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs555.hadoop.Util.DataUtilities;

public class Q01Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().isEmpty()){
			String[] record = value.toString().split(",");

			String month = record[1];
			String dayOfWeek = record[3];
			String cRSDepTime = record[5];
			String arrDelay = record[14];
			String depDelayDeparture = record[15];

			double arrivalDelayDouble = 0;
			double departureDelayDouble = 0;
			
			if (!arrDelay.isEmpty() && !arrDelay.equals("NA") && !arrDelay.equals("ArrDelay")) {
				arrivalDelayDouble = Double.parseDouble(arrDelay);
			}
			
			if (!depDelayDeparture.isEmpty() && !depDelayDeparture.equals("NA") && !depDelayDeparture.equals("DepDelay")) {
				departureDelayDouble = Double.parseDouble(depDelayDeparture);
			}

			double delayDouble = (arrivalDelayDouble + departureDelayDouble);
			
			if (!cRSDepTime.isEmpty() && !cRSDepTime.equals("NA") && !cRSDepTime.equals("CRSDepTime")) {
				
				// format the time into military time
				if (cRSDepTime.length() != 4) {
					while (cRSDepTime.length() != 4) {
						cRSDepTime = "0" + cRSDepTime;
					}
				}
				
				// write the time
				DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("HHmm");
				LocalTime lt = LocalTime.parse(cRSDepTime, inputFormatter);
				int hour = lt.getHour();
				String hourKey = "HOUR:" + hour;
				context.write(new Text(hourKey), new Text(Double.toString(delayDouble)));
			}
			
			if (!dayOfWeek.isEmpty() && !dayOfWeek.equals("NA") && !dayOfWeek.equals("DayOfWeek")) {
				// day
				String dayKey = "DAY:" + dayOfWeek;
				context.write(new Text(dayKey), new Text(Double.toString(delayDouble)));
			}
			
			if (!month.isEmpty() && !month.equals("NA") && !month.equals("Month")) {
				// month
				String monthKey = "MONTH:" + month;
				context.write(new Text(monthKey), new Text(Double.toString(delayDouble)));
			}
		}
	}

}
