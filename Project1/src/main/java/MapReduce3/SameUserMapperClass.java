package MapReduce3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SameUserMapperClass extends Mapper<LongWritable, Text, BigramProductWritable, Text>{
	private static final BigramProductWritable BIGRAM = new BigramProductWritable();

	public void map(LongWritable key, Text value,Context context)	throws IOException, InterruptedException {
		try {

			String line = value.toString();
			String[] fields = line.split(",");
			String prev = null;
			StringTokenizer itr = new StringTokenizer(fields[1]);
			while (itr.hasMoreTokens()) {
				String cur = itr.nextToken();
				if (prev != null) {
					BIGRAM.set(new Text(prev),new Text(cur));
					context.write(BIGRAM, new Text(fields[2]));
				}
				prev = cur;
			}
		}
		catch(NumberFormatException e) {

		}
	}
}