package MapReduce1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import sun.awt.SunHints.Value;

public class YearReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {  // i secondi due sono l'output

	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

		int freq=0;
	//	Map <Text,Integer> map=new HashMap<Text,Integer>();
		
		
		for (IntWritable value : values) {
			freq+= value.get();
			
		}	
			context.write(key,new IntWritable(freq));
		
	}
}
