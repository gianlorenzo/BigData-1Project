package MapReduce1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;

public class YearReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, OutputCollector<Text,IntWritable> output,
			Context context) throws IOException, InterruptedException {
		int len = 0;
		
		for(IntWritable int1 : values) {
			len = len + int1.get();
		}
		
		output.collect(key,new IntWritable(1));
	}
}