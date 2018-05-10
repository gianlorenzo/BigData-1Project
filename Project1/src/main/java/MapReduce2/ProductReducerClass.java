package MapReduce2;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Reducer;


public class ProductReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, IntWritable value,
			Context context) throws IOException, InterruptedException {

		

		context.write(key, value);
	}


	/*String translations = "";
	for (Text val : values) {
	translations += "," + val.toString();
	}
	result.set(translations);
	context.write(key, result);*/
}
