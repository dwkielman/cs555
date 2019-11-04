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
			//ArrayList<String> record = DataUtilities.dataReader(value.toString());
			String[] record = value.toString().split(",");
			
			/**
			String month = record.get(2);
			String dayOfWeek = record.get(4);
			String cRSDepTime = record.get(6);
			String arrDelay = record.get(15);
			String depDelayDeparture = record.get(16);
			
			**/
			String month = record[1];
			String dayOfWeek = record[3];
			String cRSDepTime = record[5];
			String arrDelay = record[14];
			String depDelayDeparture = record[15];

			//int arrivalDelayInteger = 0;
			//int departureDelayInteger = 0;
			
			double arrivalDelayDouble = 0;
			double departureDelayDouble = 0;
			
			if (!arrDelay.isEmpty() && !arrDelay.equals("NA") && !arrDelay.equals("ArrDelay")) {
				//arrivalDelayInteger = Integer.parseInt(arrDelay);
				arrivalDelayDouble = Double.parseDouble(arrDelay);
			}
			
			if (!depDelayDeparture.isEmpty() && !depDelayDeparture.equals("NA") && !depDelayDeparture.equals("DepDelay")) {
				//departureDelayInteger = Integer.parseInt(depDelayDeparture);
				departureDelayDouble = Double.parseDouble(depDelayDeparture);
			}
			
			//double delayDouble = (arrivalDelayInteger + departureDelayInteger) / 2;
			//double delayDouble = (arrivalDelayDouble + departureDelayDouble) / 2;
			double delayDouble = (arrivalDelayDouble + departureDelayDouble);
			
			//if (!arrDelay.equals("HW3NA") && !depDelayDeparture.equals("HW3NA")) {
				if (!cRSDepTime.isEmpty() && !cRSDepTime.equals("NA") && !cRSDepTime.equals("CRSDepTime")) {
					// time
					if (cRSDepTime.length() != 4) {
						while (cRSDepTime.length() != 4) {
							cRSDepTime = "0" + cRSDepTime;
						}
					}
					
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
			//}
		}
	}

}
