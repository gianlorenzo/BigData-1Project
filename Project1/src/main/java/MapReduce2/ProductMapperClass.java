package MapReduce2;

import java.io.IOException;
import java.io.StringReader;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.opencsv.CSVReader;




public class ProductMapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final IntWritable one = new IntWritable(40);
	private Text word = new Text();
	private Text word1 = new Text();
	int year;

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] fields = line.split(",");
		try {
			Calendar date=Calendar.getInstance();
			date.setTimeInMillis(Long.parseLong((fields[7]))*1000);	
			long anno=date.get(Calendar.YEAR);
			if(anno>=2003 && anno<=2012)
				context.write(new Text(fields[1]+"-"+Long.toString(anno)), new IntWritable(Integer.parseInt(fields[6])));
		}
		catch (NumberFormatException e) {
			context.write(new Text("perso"),one);
		}


	}
}



