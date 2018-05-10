package MapReduce1;

import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Calendar;
import java.util.Date;

public class YearMapperClass extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	private static final IntWritable one = new IntWritable(1);
/*	private Text word = new Text();
*/	

	
	public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Context context)	throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] fields = line.split(",");
		
		Calendar date=Calendar.getInstance();
		date.setTimeInMillis(Integer.parseInt(fields[7])*1000);
		
		int anno=date.get(Calendar.YEAR);
		
		
		context.write(new IntWritable(anno),one);
		
		/*
		word=results[7];
		year=results[8]; // è dato come timestamp, va convertito in data e estratto l'anno
		output.collect(new Text(year), new Text(word));
		*/
			
		
		}
		 
	}
