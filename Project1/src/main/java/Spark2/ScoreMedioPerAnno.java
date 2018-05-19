package Spark2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

public class ScoreMedioPerAnno implements Serializable {

	private static final long serialVersionUID = 1L;
	String inputPath;
	String outputPath = "/home/gianlorenzo/out5";

	public ScoreMedioPerAnno(String file){
		this.inputPath = file;
	}

	private JavaRDD<String[]> mapper() {
		SparkConf conf = new SparkConf().setAppName("AvgScore");

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

	public long unixYearConverter(String year)
	{
		try {
			Calendar date=Calendar.getInstance();
			date.setTimeInMillis(Long.parseLong((year))*1000);	
			long anno=date.get(Calendar.YEAR);
			return anno;
		}
		catch(NumberFormatException ex)
		{
			return 0;
		}
	}

	public double getDoubleScore(String score) {
		try {
			return Double.parseDouble(score);
		}
		catch(NumberFormatException ex)
		{
			return 0;
		}
	}

	public static List<Tuple2<Long, Double>> scoreMediPerAnni(Iterable<Tuple2<Long, Double>> tuple) {
		Map<Long, List<Double>> scorePerAnno = new HashMap<>();
		List<Tuple2<Long, Double>> scoreMedioPerAnno = new ArrayList<>();

		tuple.forEach(x -> {
			long year = x._1();

			if(year >= 2003 && year <= 2012) {


				List<Double> scores = null;

				if(scorePerAnno.containsKey(year))
					scores = scorePerAnno.get(year);
				else
					scores = new ArrayList<>();

				scores.add(x._2());
				scorePerAnno.put(year, scores);
			}
			else
				return;
		});

		scorePerAnno.keySet().forEach(a -> {
			List<Double> scores = scorePerAnno.get(a);
			double scoreMedio = scores.stream().reduce((b, c) -> b + c).get() / (double)scores.size();

			scoreMedioPerAnno.add(new Tuple2<Long, Double>(a,scoreMedio));
		});

		return scoreMedioPerAnno;
	}
	private void reducer() {
		JavaRDD<String[]> reviews = mapper();

		reviews
		.filter(a -> a != null)
		.mapToPair(a -> new Tuple2<String, Tuple2<Long, Double>>(
				a[1], 
				new Tuple2<Long, Double>(
						unixYearConverter(a[7]), 
						getDoubleScore(a[6])
						)
				))
		.filter(a -> a._1() != null)
		.groupByKey()
		.mapToPair(a -> {
			return new Tuple2<String, List<Tuple2<Long, Double>>>(a._1(),scoreMediPerAnni(a._2()));
		}).sortByKey().coalesce(1).saveAsTextFile(outputPath);



	}

	public static void main(String[] args) {
		if(args.length < 1) { 
			System.err.println("error"); 
			System.exit(1);
		}
		new ScoreMedioPerAnno(args[0]).reducer();

	}


}