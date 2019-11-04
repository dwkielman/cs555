package cs555.hadoop.testing;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestJob {
	
	public static void main(String[] args) {
	
		try {
	        Configuration conf = new Configuration();
	        // Give the MapRed job a name. You'll see this name in the Yarn webapp.
	        Job job = Job.getInstance(conf, "TestJob");
	        // Current class.
	        job.setJarByClass(TestJob.class);
	        
	        
	        // Combiner. We use the reducer as the combiner in this case.
	        job.setCombinerClass(TestCombiner.class);
		    // Mapper
		    //job.setMapperClass(HW3MetadataMapper.class);
		    
	        // Combiner
	        //job.setCombinerClass(HW3Combiner.class);
	        // Reducer
	        //job.setReducerClass(TestReducer.class);
	        
	        // TestReducer
	        job.setReducerClass(TestReducer.class);
	        // Outputs from the Mapper.
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(Text.class);
	        // Outputs from Reducer. It is sufficient to set only the following two properties
	        // if the Mapper and Reducer has same key and value types. It is set separately for
	        // elaboration.
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        // path to input in HDFS
	        
	        //FileInputFormat.addInputPath(job, new Path(args[0]));
	        Path p1 = new Path(args[0]);
	        Path p2 = new Path(args[1]);
	        
	        MultipleInputs.addInputPath(job, p1, TextInputFormat.class, TestMapper.class);
	        MultipleInputs.addInputPath(job, p2, TextInputFormat.class, TestAirportMapper.class);
	        // path to output in HDFS
	        FileOutputFormat.setOutputPath(job, new Path(args[2]));
	        // Block until the job is completed.
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	    } catch (IOException e) {
	        System.err.println(e.getMessage());
	    } catch (InterruptedException e) {
	        System.err.println(e.getMessage());
	    } catch (ClassNotFoundException e) {
	        System.err.println(e.getMessage());
	    }
	
	}

}
