package Spark1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.stream.Collectors;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import scala.Tuple2;


public class ParolePerAnni implements Serializable {
	
	private static final long serialVersionUID = 1L;

	private String inputPath;
	private String outputPath = "~/Spark1";

	public ParolePerAnni(String inputPath) {
		this.inputPath = inputPath;
	}

	public JavaRDD<String[]> mapper() {
		SparkConf conf = new SparkConf()
				.setAppName("ParolePerAnni");

		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);


		return sc.textFile(this.inputPath).map(line -> {
			try {
				return line.split(",");
			} catch(Exception e) {
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

	public List<String> getToken(String summary)
	{
		String tok="";
		List<String> words=new ArrayList<>();
		StringTokenizer tokenizer = new StringTokenizer(summary);			
		while (tokenizer.hasMoreTokens()) 
		{
			tok=(tokenizer.nextToken());
			words.add(tok);

		}
		return words;
	}

	public List<Tuple2<String, Long>> getWordOccurencies(Iterable<String> it) {
		Map<String, Long> occourrenceMap = new HashMap<>();
		List<Tuple2<String, Long>> sortedMap = new LinkedList<Tuple2<String, Long>>();

		it.forEach(x -> { 
			if (occourrenceMap.containsKey(x))
				occourrenceMap.put(x, occourrenceMap.get(x) + 1l);
			else
				occourrenceMap.put(x, 1l);
		});

		occourrenceMap.keySet().forEach(x -> {
			Long occurrencies = occourrenceMap.get(x);
			sortedMap.add(new Tuple2<String, Long>(x, occurrencies));
		});

		Collections.sort(sortedMap, new Comparator<Tuple2<String, Long>>() {
			public int compare(Tuple2<String, Long> e1, Tuple2<String, Long> e2) {
				return (e1._2()).compareTo(e2._2()) * (-1);
			}
		});

		return sortedMap;
	}


	private void reducer() {

		JavaRDD<String[]> reduce = mapper();
		reduce
		.filter(x -> x != null)
		.mapToPair(x -> new Tuple2<Long, List<String>> (
				unixYearConverter(x[7]), 
				getToken(x[8])
				)
				)
		.flatMapValues(x -> x)
		.groupByKey()
		.mapToPair(x -> new Tuple2<Long, List<Tuple2<String, Long>>>(x._1(), getWordOccurencies(x._2())))
		.mapValues(x -> x.stream().limit(10).collect(Collectors.toList()))
		.sortByKey(true)
		.coalesce(1).saveAsTextFile(outputPath);
	}


	public static void main(String[] args) {
		double inizio = System.currentTimeMillis();

		if(args.length < 1) {  
			System.exit(1);
		}

		new ParolePerAnni(args[0]).reducer();
		System.out.println("Tempo impiegato per eseguire il Job1 :" + (System.currentTimeMillis()-inizio)/1000);


	}


}


