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

public class YearMapperClass extends Mapper<LongWritable, Text, LongWritable, IntWritable> {

	private static final IntWritable one = new IntWritable(1);
	private static final LongWritable MISSING = new LongWritable(0);
/*	private Text word = new Text();
*/	

	
	public void map(LongWritable key, Text value,Context context)	throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] fields = line.split(",");
	
		try {
				Calendar date=Calendar.getInstance();
				date.setTimeInMillis(Integer.parseInt(fields[7])*1000);	
				long anno=date.get(Calendar.YEAR);
				context.write(new LongWritable(anno),one);
		}
		catch(NumberFormatException ex)
		{
			context.write(MISSING,one);
		}
		
		/*
		word=results[7];
		year=results[8]; // è dato come timestamp, va convertito in data e estratto l'anno
		output.collect(new Text(year), new Text(word));
		*/
			
		
		}
		 
	}
