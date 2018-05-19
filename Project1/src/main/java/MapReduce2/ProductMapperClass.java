package MapReduce2;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProductMapperClass extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		try {
			String[] fields = line.split(",");
			String score = fields[6];
			Calendar date=Calendar.getInstance();
			date.setTimeInMillis(Long.parseLong((fields[7]))*1000);	
			long anno=date.get(Calendar.YEAR);
			if(anno>=2003 && anno<=2012) {
				context.write(new Text(fields[1]+"--"+Long.toString(anno)), new Text(score));
			}
		}
		catch (NumberFormatException e) {
		}

	}


}



