package MapReduce3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SameUserReducerClass2 extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values,	Context context) throws IOException, InterruptedException {

		List<String> product=new ArrayList<String>();
		
		for (Text value : values) {
			product.add(value.toString());
		}
		
		
		context.write(key, new Text("\t"+product.toString()));
		
	}

}
