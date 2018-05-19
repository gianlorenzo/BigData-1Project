package MapReduce2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ProductCombinerClass extends Reducer<Text, Text, Text, Text> {
		private double score;
		private double lenght;
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				score=score + Double.parseDouble(value.toString());
				lenght++;
			}
			double media=score/lenght;
			String[] chiave=key.toString().split("--");
			if(chiave.length>1)
			context.write(new Text(chiave[0]), new Text(chiave[1]+" : "+String.valueOf(media)));
		}


	}
	
