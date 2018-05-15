package MapReduce1;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MainClass {
	
	
public static void main (String args[]) throws Exception {
		
		double inizio = System.currentTimeMillis();
	
		Job job = new Job(new Configuration(), "MainClass");
		
		job.setJarByClass(MainClass.class);
		job.setMapperClass(YearMapperClass.class);
			
		job.setCombinerClass(YearCombinerClass.class);
		job.setReducerClass(YearReducerClass.class);
		

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);	
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);	
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
		
		System.out.println("Tempo impiegato per eseguire il Job1 :" + (System.currentTimeMillis()-inizio)/1000);
			
	}

}
