package org.moita;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class CountWords {

    private SparkSession spark;

    private String inputDataFilePath;

    private MapFunction<String, String> formatDataFunction =
            line -> line.replaceAll("[^a-zA-Z\\s]", "")
                    .trim()
                    .toLowerCase();

    private FlatMapFunction<String, String> splitWordsFunction =
            line -> Arrays.asList(line.split(" ")).iterator();

    private FilterFunction<String> removeNonWordsFilter =
            word -> word.trim().length() > 1;

    public CountWords() {
        Logger.getLogger("org.apache").setLevel(Level.ERROR);

        spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("spark_counter")
                .getOrCreate();

       // spark.sparkContext().setLogLevel("ERROR");

        inputDataFilePath = this.getClass().getResource("/look_away.srt").getPath();
    }

    public void execute() {
        spark
                .read()
                .textFile(inputDataFilePath)
                .map(formatDataFunction, Encoders.STRING())
                .flatMap(splitWordsFunction, Encoders.STRING())
                .filter(removeNonWordsFilter)
                .toJavaRDD()
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey((v1, v2) -> v1 + v2)
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                .sortByKey(false)
                .take(50)
                .forEach(System.out::println);
    }


    public static void main(String[] args) {
        new CountWords().execute();
    }
}