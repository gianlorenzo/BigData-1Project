package MapReduce;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;


public class MainClass {

	public static void main (String args[]) throws IOException {
		
		Job job = new Job(new Configuration(), "MainClass");
		
		job.setJarByClass(MainClass.class);
		

	}

}
