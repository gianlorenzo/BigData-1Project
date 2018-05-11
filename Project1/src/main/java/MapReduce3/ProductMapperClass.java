package MapReduce3;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;





public class ProductMapperClass extends Mapper<LongWritable, Text, Text, MapWritable> {
	private static final IntWritable one = new IntWritable(40);
	private static final MapWritable missing = new MapWritable();
	MapWritable mw = new MapWritable();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		missing.put(new LongWritable(0), new IntWritable(0));
		String line = value.toString();
		String[] fields = line.split(",");
		try {
			Calendar date=Calendar.getInstance();
			date.setTimeInMillis(Long.parseLong((fields[7]))*1000);	
			long anno=date.get(Calendar.YEAR);
			if(anno>=2003 && anno<=2012) {
				mw.put(new LongWritable(anno), new IntWritable(Integer.parseInt(fields[6])));
				context.write(new Text(fields[1]+"-"+Long.toString(anno)), mw);
			}
		}
		catch (NumberFormatException e) {
			context.write(new Text("perso"),missing);
		}


	}
}



