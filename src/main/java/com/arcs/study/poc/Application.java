package com.arcs.study.poc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class Application {

	private static String SPACE = " ";
	private static String APP_NAME = "WORD_COUNT";
	private static String URL_MASTER = "local[2]";
	private static String TEXT_PATH = "C:\\Users\\alyss\\Workspace\\study.poc\\assets\\APACHE_TEST.txt";

	public static void main(String[] args) {
		// START SPARK CONFIGURATION
		SparkConf sparkConf = new SparkConf()
				.setAppName(APP_NAME)
				.setMaster(URL_MASTER);

		// START SPARK CONTEXT
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		// LOAD CONTENT FROM FILE
		JavaRDD<String> lines = ctx.textFile(TEXT_PATH);

		// COUNT QUANTITY OF WORDS
		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(SPACE)).iterator());

		// MOUNT A DICTIONARY WITH THE WORDS
		JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<>(word, 1));

		// COUNT QUANTITY BY KEY
		JavaPairRDD<String, Integer> counts = ones.reduceByKey((Integer i1, Integer i2) -> i1 + i2);

		List<Tuple2<String, Integer>> output = counts.collect();
		for(Tuple2<?, ?> tuple : output){
			System.out.println(tuple._1() + ": " + tuple._2());
		}

		ctx.stop();
	}
}
