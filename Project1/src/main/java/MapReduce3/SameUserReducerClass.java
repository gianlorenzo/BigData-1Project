package MapReduce3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SameUserReducerClass extends Reducer<BigramProductWritable, Text, BigramProductWritable, LongWritable> {
	public void reduce(BigramProductWritable products, Iterable<Text> userIds, Context context) throws IOException, InterruptedException {
		
		context.write(products, new LongWritable(1));
	}

}
