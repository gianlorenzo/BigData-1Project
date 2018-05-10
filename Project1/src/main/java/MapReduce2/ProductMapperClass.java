package MapReduce2;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.opencsv.CSVReader;




public class ProductMapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final IntWritable one = new IntWritable(40);
	private Text word = new Text();
	private Text word1 = new Text();
	int year;
	String[] words = {"a","b","c","d","e","f","g","h","i","l","m","n","o","p","q","r","s","t","u","v","z",
			"j","k","A","B","C","D","E","F","G","H","I","L","M","N","O","P","Q","R","S","T","U",
			"V","Z","J","K","w","W","\"","!","£","$","%","&","/","(",")","=","?","?","^","[",
			"]","{","}","è","é","*","ç","ò","à","ù","§","°","a","@","#","€","a","-","/","a",
			".",";","_"};
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] fields = line.split(",");

		String score = fields[6].replaceAll(" ", "0").replaceAll("0and0Kitten\"\"\"", "0").replaceAll(
				"0Dad\"\"", "0").replaceAll("0\"","0").replaceAll("book-blogger\"\"\"", "0").replaceAll(
						" and book lover\"\"\"", "0").replaceAll("0USA\"", "0").replaceAll("0friend\"\"\"","0").replaceAll(
								"0RN\"\"\"", "0").replaceAll("0swimme...\"", "0").replaceAll("0Music0Fan...\"", "0").replaceAll(
										"0and0book0lover\"", "0").replaceAll("0\"\"", "0").replaceAll(
												"&#4314", "0").replaceAll("\"0and0Reviewer0\"Todd0...\"", "0");


			word.set(fields[1]);

			context.write(word, new IntWritable(1));


		}
	}



