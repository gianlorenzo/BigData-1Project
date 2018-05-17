package MapReduce3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SameUserMapperClass2 extends Mapper<LongWritable, Text, Text, Text>{
	

	public void map(LongWritable key, Text value,Context context)	throws IOException, InterruptedException {

		try {		
			// user 2 -->prodotto 1
			String line = value.toString();
			String[] fields = line.split("&&");
			context.write(new Text(fields[1]), new Text(fields[0]));			
		   }
		catch(Exception e) 
			{
			}
	
	}
	
	
	}