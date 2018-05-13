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

public class YearReducerClass extends Reducer<Text, Text, Text, Text> {  // i secondi due sono l'output

	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

		String resoconto="";
		
		for (Text value : values) {
		
			resoconto=resoconto+"  "+value.toString();
		}
		
		context.write(key,new Text(resoconto));
		
	}
}
