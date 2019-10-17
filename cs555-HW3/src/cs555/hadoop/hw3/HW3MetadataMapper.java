package cs555.hadoop.hw3;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs555.hadoop.Util.DataUtilities;

public class HW3MetadataMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(!value.toString().isEmpty()){
			ArrayList<String> record = DataUtilities.dataReader(value.toString());
			
			String artistID = record.get(3);
			String artistName = record.get(7);
			String songID = record.get(8);
			String songTitle = record.get(9);
			String artistFamiliarity = record.get(1);
			String artistHotttnesss = record.get(2);
			String similarArtists = record.get(10);
			String artistSongTerms = record.get(11);
			String year = record.get(14);
			String location = record.get(6);
			
			if (artistFamiliarity.isEmpty()) {
				artistFamiliarity = "-1.0";
			}
			if (artistHotttnesss.isEmpty()) {
				artistHotttnesss = "-1.0";
			}
			if (similarArtists.isEmpty()) {
				similarArtists = "HW3NA";
			}
			if (artistSongTerms.isEmpty()) {
				artistSongTerms = "HW3NA";
			}
			if (year.isEmpty()) {
				year = "0";
			}
			if (location.isEmpty()) {
				location = "HW3NA";
			}
			
			if (!songID.isEmpty() && !artistID.isEmpty() && !artistName.isEmpty() && !songTitle.isEmpty()) {
				context.write(new Text(songID), new Text("metadata\t" + artistID + "," + artistName + "," + songTitle
						 + "," + artistFamiliarity + "," + artistHotttnesss + "," + similarArtists + "," + artistSongTerms
						 + "," + location + "," + year));
			}
		}
	}
}