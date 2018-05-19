package Spark2;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Ex0WordCount {
	private static String pathToFile;

	 public Ex0WordCount(String file){
	 this.pathToFile = file;
	 }
	/**
	 * Load the data from the text file and return an RDD of words
	 */
	 public JavaRDD<String> loadData() {

	 SparkConf conf = new SparkConf()
	 .setAppName("Wordcount");
	 JavaSparkContext sc = new JavaSparkContext(conf);
	 sc.addJar("/home/gianlorenzo/BigData/example_jar/spark.jar");
	 JavaRDD<String> words = sc.textFile("/home/gianlorenzo/BigData/codiceBIGDATA/data/words.txt")
	 .flatMap(line -> Arrays.asList(line.split(" ")));
	 return words;
	 }
	 
	 public JavaPairRDD<String, Integer> wordcount() {
		 JavaRDD<String> words = loadData();
		 // Step 1: mapper step
		 JavaPairRDD<String, Integer> couples =
		 words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
		 // Step 2: reducer step
		 JavaPairRDD<String, Integer> result = couples.reduceByKey((a, b) -> a + b);
		 return result;
		 }
	 public JavaPairRDD<String, Integer> filterOnWordcount(int x) {
		 JavaPairRDD<String, Integer> wordcounts = wordcount();
		 JavaPairRDD<String, Integer> filtered =
		 wordcounts.filter(couple -> couple._2() > x);
		 return filtered;
		 }
	 public static void main(String[] args) {
		 // TODO Auto-generated method stub

		 if (args.length < 1) {
		 System.err.println("Usage: Ex0Wordcount <filetxt>");
		 System.exit(1);
		 }

		Ex0WordCount wc = new Ex0WordCount(args[0]);

		 System.out.println("wordcount: "+wc.wordcount().toString());

		 }
}
