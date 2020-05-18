package com.amsidh.spark;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import static java.lang.Long.MAX_VALUE;
import static java.util.Arrays.asList;
import static java.util.concurrent.ThreadLocalRandom.current;

public class WordCount {
private static final String PATH_SEPEARTOR = "/";
    public static void main(String[] args) {

        wordCount(args[0], args[1]);

    }

    public static void wordCount(String inputFile, String outputFilePath) {
        SparkConf sparkConf = getSparkConf();

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = javaSparkContext.textFile(inputFile);
        JavaRDD<String> words = lines.flatMap(line -> asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(word -> new Tuple2<>(word, 1));// converted (word,1)
        JavaPairRDD<String, Integer> wordAndCountSum = wordAndOne.groupByKey().mapValues(Iterables::size);

        wordAndCountSum.saveAsTextFile(outputFilePath + PATH_SEPEARTOR + "word-count-result" + PATH_SEPEARTOR
               + current().nextLong(0, MAX_VALUE));
    }

    private static SparkConf getSparkConf() {
        SparkConf sparkConf = new SparkConf();
        //sparkConf.setMaster("local");
        sparkConf.setAppName("WordCountApp");
        return sparkConf;
    }
}
