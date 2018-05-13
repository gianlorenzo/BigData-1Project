package MapReduce2;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProductMapperClass extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	private static final DoubleWritable missing = new DoubleWritable(0);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		try {
			String[] fields = line.split(",");
			DoubleWritable score = new DoubleWritable(Double.parseDouble(fields[6]));
			Calendar date=Calendar.getInstance();
			date.setTimeInMillis(Long.parseLong((fields[7]))*1000);	
			long anno=date.get(Calendar.YEAR);
			if(anno>=2003 && anno<=2012) {
				context.write(new Text(fields[1]+"\t"+Long.toString(anno)), score);
			}
		}
		catch (NumberFormatException e) {
			//context.write(new Text("Score iniziale"),missing);
		}

	}


}



