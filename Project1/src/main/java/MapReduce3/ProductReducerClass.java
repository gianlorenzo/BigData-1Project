package MapReduce3;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;


public class ProductReducerClass extends Reducer<Text, MapWritable, Text, MapWritable> {

	MapWritable mw1 = new MapWritable();
	String[] out;
	public void reduce(Text key, Iterable<MapWritable> values, Context context) 
			throws IOException, InterruptedException {
		out = key.toString().split("-");
		for (MapWritable m : values) {
			context.write(new Text(out[1]),m);
		
		}



	}


}


/*String translations = "";
	for (Text val : values) {
	translations += "," + val.toString();
	}
	result.set(translations);
	context.write(key, result);*/

