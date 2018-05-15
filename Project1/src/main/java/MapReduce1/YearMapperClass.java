package MapReduce1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Calendar;

import java.util.StringTokenizer;

public class YearMapperClass extends Mapper<LongWritable, Text, Text, Text> {

	//private static final IntWritable one = new IntWritable(1);
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
							chiave.set(Long.toString(anno)+"--"+word);
							context.write(chiave,new Text("1"));
					}
				}
				
		}
		catch(NumberFormatException ex)
		{
			context.write(new Text("0000--"),new Text("1"));
		}
			
		}
		 
	}
