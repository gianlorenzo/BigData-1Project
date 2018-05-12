package MapReduce1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import sun.awt.SunHints.Value;

public class YearCombinerClass extends Reducer<Text, IntWritable, Text, Text> {  // i secondi due sono l'output

	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

		int somma=0;
		
		for (IntWritable value : values) {
		
			somma+=value.get();
		}
			String year=key.toString().substring(0, 4);
			String[] word=key.toString().split("--");
			
			
			context.write(new Text(year),new Text(word[1]+"--"+Integer.toString(somma)));
		
	}
}
