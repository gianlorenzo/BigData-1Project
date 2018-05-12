package MapReduce1;

import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Calendar;
import java.util.StringTokenizer;

public class YearMapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);
	//private static final Text MISSING = new Text("0");
	private Text word = new Text();
	private Text chiave = new Text();
	

	
	public void map(LongWritable key, Text value,Context context)	throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] fields = line.split(",");
		try {
				Calendar date=Calendar.getInstance();
				date.setTimeInMillis(Long.parseLong((fields[7]))*1000);	
				long anno=date.get(Calendar.YEAR);
				if(anno>1970)
				{
					StringTokenizer tokenizer = new StringTokenizer(fields[8]);			
					while (tokenizer.hasMoreTokens()) 
					{
							word.set(tokenizer.nextToken());
							chiave.set(Long.toString(anno)+"\t"+word);
							context.write(chiave,one);
					}
				}
		}
		catch(NumberFormatException ex)
		{
			//context.write(MISSING,one);
		}
		
		/*
		word=results[7];
		year=results[8]; // è dato come timestamp, va convertito in data e estratto l'anno
		output.collect(new Text(year), new Text(word));
		*/
			
		
		}
		 
	}
