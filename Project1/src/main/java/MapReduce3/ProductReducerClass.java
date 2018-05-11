package MapReduce3;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;


public class ProductReducerClass extends Reducer<Text, MapWritable, Text, MapWritable> {
	
	MapWritable mw = new MapWritable();
	
	public void reduce(Text key, MapWritable values,
			Context context) throws IOException, InterruptedException {
		int i =0; //score medio
		for(Writable m : values.values()) {
			i = (i+Integer.parseInt(m.toString()))/values.values().size();
			mw.put(m, new LongWritable(i));
			context.write(key, mw);
		}
		

		
		

		
	}


	/*String translations = "";
	for (Text val : values) {
	translations += "," + val.toString();
	}
	result.set(translations);
	context.write(key, result);*/
}
