package MapReduce3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MainClass {
	
	public static void main (String args[]) throws Exception {

		Job job = new Job(new Configuration(), "MainClass");

		job.setJarByClass(MainClass.class);

		job.setMapperClass(SameUserMapperClass.class);
		job.setReducerClass(SameUserReducerClass.class);


		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(BigramProductWritable.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);


	}

}
