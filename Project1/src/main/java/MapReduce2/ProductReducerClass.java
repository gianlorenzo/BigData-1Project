package MapReduce2;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


public class ProductReducerClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {

		double score = 0;
		int lenght = 0;
		for (DoubleWritable value : values) {
			score+= value.get();
			lenght++;
		}
		context.write(key, new DoubleWritable(score/lenght));
	}


}

