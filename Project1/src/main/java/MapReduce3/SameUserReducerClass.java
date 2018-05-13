package MapReduce3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SameUserReducerClass extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values,	Context context) throws IOException, InterruptedException {

		List<String> user=new ArrayList<String>();
		
		for (Text value : values) {
			user.add(value.toString());
		}
		
		context.write(key, new Text(user.toString()));
	}
}