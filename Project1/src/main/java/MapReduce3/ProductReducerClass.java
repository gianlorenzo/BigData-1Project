package MapReduce3;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ProductReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	public void reduce(Text key, Iterator<IntWritable> values,
			 OutputCollector<Text, IntWritable> output,
			 Reporter reporter)
			throws IOException {
		
		int i = 0;
		
		while(values.hasNext()) {
			i += values.next().get(); 
		}
		
		output.collect(key, new IntWritable(i));
		
		
	}

}
