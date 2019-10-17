package cs555.hadoop.hw3;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import cs555.hadoop.Util.DataUtilities;

public class HW3Combiner extends Reducer<Text, Text, Text, Text> {
	
	private final StringBuilder sb = new StringBuilder();
	private final Text output = new Text();
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text val : values) {
        	String parts[] = val.toString().split("\t");
        	if (parts[0].equals("analysis")) {
        		String[] record = parts[1].split(",");
        		ArrayList<Double> data = new ArrayList<Double>();
        		double average = 0.0;
        		
        		// song loudness
        		sb.append("LOUDNESS_#_" + record[0]);
        		sb.append(",");
        		
        		// song Hotttnesss
        		sb.append("SONGHOTTTNESSS_#_" + record[1]);
        		sb.append(",");
        		
        		// song duration
        		sb.append("DURATION_#_" + record[2]);
        		sb.append(",");
        		
        		// song endOfFadeIn
        		sb.append("ENDOFFADEIN_#_" + record[3]);
        		sb.append(",");
        		
        		// song startOfFadeOut
        		sb.append("STARTOFFADEOUT_#_" + record[4]);
        		sb.append(",");
        		
        		// song danceability
        		sb.append("DANCEABILITY_#_" + record[5]);
        		sb.append(",");
        		
        		// song energy
        		sb.append("ENERGY_#_" + record[6]);
        		sb.append(",");

        		// song segments Start
        		data = DataUtilities.segmentsMaker(record[7]);
        		average = DataUtilities.getAverageValue(data);
        		sb.append("SEGMENTSSTARTAVERAGE_#_" + average);
        		sb.append(",");
        		
        		data = new ArrayList<Double>();
        		average = 0.0;
        		
        		// song segementsPitches
        		data = DataUtilities.segmentsMaker(record[8]);
        		average = DataUtilities.getAverageValue(data);
        		sb.append("SEGMENTSPITCHESAVERAGE_#_" + average);
        		sb.append(",");
        		
        		data = new ArrayList<Double>();
        		average = 0.0;
        	
        		// song segementsTimbre
        		data = DataUtilities.segmentsMaker(record[9]);
        		average = DataUtilities.getAverageValue(data);
        		sb.append("SEGMENTSTIMBREAVERAGE_#_" + average);
        		sb.append(",");
        		
        		data = new ArrayList<Double>();
        		average = 0.0;
        		
        		// song segementsLoudnessMaxTime
        		data = DataUtilities.segmentsMaker(record[10]);
        		average = DataUtilities.getAverageValue(data);
        		sb.append("SEGMENTSLOUDNESSMAXTIMEAVERAGE_#_" + average);
        		sb.append(",");
        		
        		data = new ArrayList<Double>();
        		average = 0.0;
        		
        		// song segementsLoudnessStart
        		data = DataUtilities.segmentsMaker(record[11]);
        		average = DataUtilities.getAverageValue(data);
        		sb.append("SEGMENTSLOUDNESSSTARTAVERAGE_#_" + average);
        		sb.append(",");
        		
        		// song time Signature
        		sb.append("TIMESIGNATURE_#_" + record[12]);
        		sb.append(",");
        		
        		// song Key
        		sb.append("SONGKEY_#_" + record[13]);
        		sb.append(",");
        		
        		// song Mode
        		sb.append("SONGMODE_#_" + record[14]);
        		sb.append(",");
        		
        		// song Tempo
        		sb.append("TEMPO_#_" + record[15]);
        		sb.append(",");
        		
        		output.set(sb.toString());
        		sb.setLength(0);
        		sb.trimToSize();
        		context.write(key, output);

        	} else if (parts[0].equals("metadata")) {
        		String[] record = parts[1].split(",");

        		// artist ID
        		sb.append("ARTISTID_#_" + record[0]);
        		sb.append(",");
        		
        		// artist Name
        		sb.append("ARTISTNAME_#_" + record[1]);
        		sb.append(",");
        		
        		// song Title
        		sb.append("SONGTITLE_#_" + record[2]);
        		sb.append(",");
        		
        		// artist Familiarity
        		sb.append("ARTISTFAMILIARITY_#_" + record[3]);
        		sb.append(",");
        		
        		// artist Hotttnesss
        		sb.append("ARTISTHOTTTNESSS_#_" + record[4]);
        		sb.append(",");
        		
        		// artist Similar Artists
        		int similarArtistsSize = 0;
        		if (!record[5].equals("HW3NA")) {
        			similarArtistsSize = DataUtilities.dataReader(record[5]).size();
        		}
        		sb.append("SIMILARARTISTS_#_" + similarArtistsSize);
        		sb.append(",");

        		// song terms
        		sb.append("SONGTERMS_#_" + record[6]);
        		sb.append(",");
        		
        		// location
        		sb.append("LOCATION_#_" + record[7]);
        		sb.append(",");
        		
        		// year
        		sb.append("ARTISTYEAR_#_" + record[8]);
        		sb.append(",");
        		
        		output.set(sb.toString());
        		sb.setLength(0);
        		sb.trimToSize();
        		context.write(key, output);

        	}
        }
	}
}
