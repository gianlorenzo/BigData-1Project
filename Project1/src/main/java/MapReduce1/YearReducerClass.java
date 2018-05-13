package MapReduce1;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import sun.awt.SunHints.Value;

public class YearReducerClass extends Reducer<Text, Text, Text, Text> {  // i secondi due sono l'output

	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

		String resoconto="";
		
		for (Text value : values) {
		
			resoconto=resoconto+"\t"+value.toString();
		}
		
		String topTen="";
		topTen=getTopTen(resoconto);
		context.write(key,new Text(topTen));
		
	}
	
	public String getTopTen(String resoconto)
	{
		String[] tot=resoconto.split("\t");	
		Map<String,Integer> map= new HashMap<String,Integer>();
		
		for(int i=0;i<tot.length;i++)
		{
			if(tot[i].split("--").length>1)
				map.put(tot[i].split("--")[0],Integer.parseInt(tot[i].split("--")[1]));		
		}
		int k=0;
		String [] res= new String[10];
		while(k<10 && !map.isEmpty())
		{
			int maxx=0;	
			String chiave="";	
			for(Map.Entry<String,Integer> entry:map.entrySet())
				{
					if(entry.getValue()>=maxx)
						{
							maxx=entry.getValue();
							chiave=entry.getKey();
						}			
				}
			map.remove(chiave);
			res[k]=chiave+":"+Integer.toString(maxx);
			k++;		
		}
		return Arrays.toString(res);
	}
}
