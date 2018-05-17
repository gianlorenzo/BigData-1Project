package MapReduce3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SameUserReducerClass2 extends Reducer<Text, Text, Text, IntWritable> {
	
	public void reduce(Text key, Iterable<Text> values,	Context context) throws IOException, InterruptedException {

	//	List<String> product=new ArrayList<String>();
		int sum=0;
		for (Text value : values) {
			//product.add(value.toString());
			sum++;
		}
		context.write(key, new IntWritable(sum));
		
	}

}
