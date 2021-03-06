package MapReduce3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MainClass {

	public static void main (String args[]) throws Exception {

		double inizio = System.currentTimeMillis();

		Job job = new Job(new Configuration(), "MainClass");

		Path file=new Path(args[1]);
		job.setJarByClass(MainClass.class);
		job.setMapperClass(SameUserMapperClass.class);
		job.setReducerClass(SameUserReducerClass.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, file);	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);

		Job job2 = new Job(new Configuration(), "MainClass2");

		job2.setJarByClass(MainClass.class);
		job2.setMapperClass(SameUserMapperClass2.class);
		job2.setReducerClass(SameUserReducerClass2.class);
		FileInputFormat.addInputPath(job2, file);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"2"));
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.waitForCompletion(true);

		System.out.println("Tempo impiegato per eseguire il Job3 :" + (System.currentTimeMillis()-inizio)/1000);

	}

}
