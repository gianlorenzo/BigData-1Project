package MapReduce1;

import java.io.IOException;
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

public class YearReducerClass extends Reducer<LongWritable, MapWritable, LongWritable, MapWritable> {  // i secondi due sono l'output

	public void reduce(LongWritable key, Iterable<MapWritable> values,Context context) throws IOException, InterruptedException {

		MapWritable map=new MapWritable();
	//	Map <Text,Integer> map=new HashMap<Text,Integer>();
	//	MapWritable map=new MapWritable();
		
		//freq=values.toString();
		
		for (MapWritable value : values) {
			
			Set<Writable> keyset=value.keySet();
				for(Writable keys:keyset)
				{
					map.put(keys, value.get(keys));
				}
			
		}	
			context.write(key,map);
		
	}
}
