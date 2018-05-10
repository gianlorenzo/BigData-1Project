package MapReduce1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class YearReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int sum = 0;

		for (IntWritable value : values) {
			sum += value.get();
		}

		context.write(key, new IntWritable(sum));
	}
}
