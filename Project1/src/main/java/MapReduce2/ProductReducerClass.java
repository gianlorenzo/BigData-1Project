package MapReduce2;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


public class ProductReducerClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	private double score;
	private double lenght;
	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		for (DoubleWritable value : values) {
			score+= value.get();
			lenght++;
		}
		context.write(key, new DoubleWritable(score/lenght));
	}


}

