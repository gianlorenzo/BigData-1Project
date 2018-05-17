package Spark2;

import java.util.Calendar;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ProductAvg {
	
	public static void main (String[] args) {
		
		String inputFile = args[0];
		String outputFile = args[1];
		
		SparkConf conf = new SparkConf().setAppName("ProductAvg");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> input = sc.textFile(inputFile);
		
		JavaPairRDD<String,Tuple2<Long,Float>> mapper = input.mapToPair(
				line -> {
					String[] fields = line.split(",");
					Float score = Float.parseFloat(fields[6]);
					Calendar date = Calendar.getInstance();
					date.setTimeInMillis(Long.parseLong(fields[7])*1000);
					long anno = date.get(Calendar.YEAR);
					return new Tuple2<>(fields[1],new Tuple2<Long,Float>(anno,score));
					
				});
		
		//JavaPairRDD<String,Tuple2<Long,Float>>
	}

}
