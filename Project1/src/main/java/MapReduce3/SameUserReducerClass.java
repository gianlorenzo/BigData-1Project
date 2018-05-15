package MapReduce3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SameUserReducerClass extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values,	Context context) throws IOException, InterruptedException {

		List<String> user=new ArrayList<String>();
		
		for (Text value : values) {
			user.add(value.toString());
		}
		
		List<DoppiaLista<String,String>> coppie=getAllUserForProduct(user);
		int i=0;
		while(i<coppie.size())
		{
		context.write(key, new Text("\t"+coppie.get(i).getPrimo()+"--"+coppie.get(i).getSecondo()));
		i++;
		}
	}
	

	private List<DoppiaLista<String, String>> getAllUserForProduct(List<String> user) 
	{
		int j=0;
		DoppiaLista <String,String> coppia;
		List<DoppiaLista<String,String>> coppie=new ArrayList<DoppiaLista<String, String>>();
		for(String utente : user)
		{
			j++;
			int i=0;
			while(i+j<user.size())
			{
				if(utente.compareTo(user.get(i+j))<0)
				{
					coppia=new DoppiaLista<String, String>(utente,user.get(i+j));
				}
				else
				{
					coppia=new DoppiaLista<String, String>(user.get(i+j),utente);
				}
			coppie.add(coppia);
			i++;
			}
			
		}
		return coppie;
	}
	
	
	
	
	
	
	
	
	
	
}