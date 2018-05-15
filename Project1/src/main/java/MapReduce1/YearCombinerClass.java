package MapReduce1;

import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class YearCombinerClass extends Reducer<Text, Text, Text, Text> {  // i secondi due sono l'output

	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

		int somma=0;
		
		for (Text value : values) {
		
			somma+=Integer.parseInt(value.toString());
		}
			String[] word=key.toString().split("--");
			
			if(word.length>1)
				context.write(new Text(word[0]),new Text(word[1]+"--"+Integer.toString(somma)));
		
	}
}
