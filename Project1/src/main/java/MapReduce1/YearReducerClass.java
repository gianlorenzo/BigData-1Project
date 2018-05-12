package MapReduce1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import sun.awt.SunHints.Value;

public class YearReducerClass extends Reducer<Text, IntWritable, Text, Text> {  // i secondi due sono l'output

	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

		int somma=0;
		
		for (IntWritable value : values) {
		
			somma+=value.get();
		}
			String[] word=key.toString().split("--");
			
			
			context.write(new Text(word[0]),new Text(word[1]+"--"+Integer.toString(somma)));
		
	}
}
