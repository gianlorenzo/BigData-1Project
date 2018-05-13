package MapReduce3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SameUserMapperClass extends Mapper<LongWritable, Text, Text, Text>{
	private static final BigramProductWritable BIGRAM = new BigramProductWritable();

	public void map(LongWritable key, Text value,Context context)	throws IOException, InterruptedException {
		MapWritable mw = new MapWritable();
		try {

			String line = value.toString();
			String[] fields = line.split(",");
			
			//for (int i=0;i<ids.length;i++)
		}
		catch(NumberFormatException e) {

		}
	}
}