package MapReduce2;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;




public class ProductReducerClass extends Reducer<Text, Text, Text, Text> {
	
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		
		List<String> medie=new ArrayList<String>();
		
		for (Text value : values) {
			medie.add(value.toString());
			
		}
		context.write(key, new Text(medie.toString()));
	}


}

