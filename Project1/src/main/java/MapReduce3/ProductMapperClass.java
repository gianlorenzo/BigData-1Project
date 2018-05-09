package MapReduce3;
import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

public class ProductMapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, OutputCollector<Text,
			 IntWritable> output, Reporter reporter)
			throws IOException {
		
		String[] results = value.toString().split(",");
		
		output.collect(new Text(results[1]), new IntWritable(Integer.parseInt(results[0])));
		
		
	}
	
}
