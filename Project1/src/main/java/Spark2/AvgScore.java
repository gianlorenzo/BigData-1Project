package Spark2;


import java.util.Calendar;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class AvgScore {
	
	private String inputPath;

	public AvgScore(String inputPath) {
		this.inputPath = inputPath;
	}

	private JavaRDD<Product> loadData() {
		SparkConf conf = new SparkConf().setAppName("AvgScore");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		Product p = new Product();
		
		return sc.textFile(this.inputPath).map(line -> { 
			try {
			String[] fields = this.inputPath.split(",");
			Calendar date = Calendar.getInstance();
			date.setTimeInMillis(Long.parseLong(fields[7])*1000);
			long anno = date.get(Calendar.YEAR);
			p.setId(Long.parseLong(fields[0]));
			p.setProductId(fields[1]);
			p.setUserId(fields[2]);
			p.setProfileName(fields[3]);
			p.setHelpfullnessNumerator(Integer.parseInt(fields[4]));
			p.setHelpfullnessDenominator(Integer.parseInt(fields[5]));
			p.setScore(Float.parseFloat(fields[6]));
			p.setYear(anno);
			p.setSummary(fields[8]);
			p.setText(fields[9]);
			return p;
			}
			catch (NumberFormatException e) {
				return null;
			}
			
		}).cache();
		

	}

	/*private void getAvgsByProductId() {
		JavaRDD<Product> reviews = loadData();

		reviews
		.filter(x -> x != null)
		.mapToPair(x -> new Tuple2<String, Tuple2<Integer, Integer>>(
				x.getProductId(), 
				new Tuple2<Integer, Integer>(
						x.getYear(), 
						x.getScore()
				)
		))
		.filter(x -> x._1() != null)
		.groupByKey()
	    .mapToPair(x -> {
	    	return new Tuple2<String, List<Tuple2<Integer, Float>>>(x._1(), AvgScoreTask.getAvgByYear(x._2()));
	    })
	    .sortByKey()
	    .coalesce(1)
	  //.saveAsTextFile(this.outputPath)
	  	;
	}
	*/
	/*public static void main(String[] args) {
		if(args.length < 1) { 
			System.err.println("Usage: AvgScore <csvdataset>"); 
			System.exit(1);
		}

		long start = System.currentTimeMillis();
		new AvgScore(args[0]).getAvgsByProductId();
		log.info("Completed in: " + (System.currentTimeMillis() - start) + "ms");
	}*/
}