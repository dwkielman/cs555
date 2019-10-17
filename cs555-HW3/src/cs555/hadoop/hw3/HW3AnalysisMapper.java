package cs555.hadoop.hw3;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs555.hadoop.Util.DataUtilities;

public class HW3AnalysisMapper  extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!value.toString().isEmpty()){
			ArrayList<String> record = DataUtilities.dataReader(value.toString());
			
			String songID = record.get(1);
			String loudness = record.get(10);
			String songHotttnesss = record.get(2);
			String duration = record.get(5);
			String endOfFadeIn = record.get(6);
			String startOfFadeOut = record.get(13);
			String danceability = record.get(4);
			String energy = record.get(7);
			String segmentsStart = record.get(18);
			String segementsPitches = record.get(20);
			String segementsTimbre = record.get(21);
			String segementsLoudnessMaxTime = record.get(23);
			String segementsLoudnessStart = record.get(24);
			String timeSignature = record.get(15);
			String songKey = record.get(8);
			String mode = record.get(11);
			String tempo = record.get(14);
			
			if (segmentsStart.isEmpty()) {
				segmentsStart = "-1.0";
			}
			
			if (segementsPitches.isEmpty()) {
				segementsPitches = "-1.0";
			}
			
			if (segementsTimbre.isEmpty()) {
				segementsTimbre = "-1.0";
			}
			
			if (segementsLoudnessMaxTime.isEmpty()) {
				segementsLoudnessMaxTime = "-1.0";
			}
			
			if (segementsLoudnessStart.isEmpty()) {
				segementsLoudnessStart = "-1.0";
			}
			
			if (timeSignature.isEmpty()) {
				timeSignature = "-1.0";
			}
			
			if (songKey.isEmpty()) {
				songKey = "-1.0";
			}
			
			if (mode.isEmpty()) {
				mode = "-1.0";
			}
			
			if (tempo.isEmpty()) {
				tempo = "-1.0";
			}
			
			if (!songID.isEmpty() && !loudness.isEmpty() && !songHotttnesss.isEmpty() && !duration.isEmpty() && !endOfFadeIn.isEmpty() && !startOfFadeOut.isEmpty()
					&& !danceability.isEmpty() && !energy.isEmpty()) {
				context.write(new Text(songID), new Text("analysis\t" + loudness + "," + songHotttnesss + "," + duration + "," + endOfFadeIn + "," + startOfFadeOut
						 + "," + danceability + "," + energy + "," + segmentsStart + "," + segementsPitches + "," + segementsTimbre + "," + segementsLoudnessMaxTime + "," + segementsLoudnessStart
						 + "," + timeSignature + "," + songKey + "," + mode + "," + tempo));
			}
			
			
		}
	}

}
