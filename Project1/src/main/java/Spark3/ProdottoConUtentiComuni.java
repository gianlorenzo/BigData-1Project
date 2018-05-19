package Spark3;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import com.google.common.collect.Lists;


import Spark3.SerializableComparator;

public class ProdottoConUtentiComuni {

	
	String inputPath;
	String outputPath = "/home/gianlorenzo/out7";

	public ProdottoConUtentiComuni(String file){
		this.inputPath = file;
	}
	
	public JavaRDD<String[]> mapper() {
		SparkConf conf = new SparkConf().setAppName("ProdottiConUtentiComuni");

		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);


		return sc.textFile(this.inputPath).map(line -> {
			try {
				return line.split(",");
			} catch(NumberFormatException e) {
				return null;
			}
		}).cache();
	}
	
	private void ProdottiConUtentiComuni() {
		JavaRDD<String[]> reducer = mapper();
		
		JavaPairRDD<String, String> user2product = reducer.filter(x -> x != null)		
														  .mapToPair(x -> new Tuple2<String, String> (x[2], x[1]));
		
		user2product
		.join(user2product)
		.filter(x -> !x._2()._1().equals(x._2()._2()))
		.mapValues(x -> (x._1().compareTo(x._2()) == 1) ? new Tuple2<>(x._2(), x._1()) : x)
		.distinct()
		.mapToPair(x -> new Tuple2<Tuple2<String, String>, String>(x._2(), x._1()))
		.groupByKey()
		.mapToPair(x -> new Tuple2<Tuple2<String, String>, Long>(x._1(), (long) Lists.newArrayList(x._2()).size()))
		.sortByKey(SerializableComparator.serialize((a, b) -> a._1().compareTo(b._1())))
		.coalesce(1).saveAsTextFile(outputPath);	
	}
	
	public static void main(String[] args) {
		if(args.length < 1) { 
			System.err.println("error"); 
			System.exit(1);
		}

		new ProdottoConUtentiComuni(args[0]).ProdottiConUtentiComuni();
	}
	

}
