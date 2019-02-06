package org.moita;

import jersey.repackaged.com.google.common.collect.Maps;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.moita.json.QuarterOfYear;
import org.moita.json.QuarterOfYearSer;
import scala.Tuple2;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static java.math.BigDecimal.ZERO;
import static java.math.BigDecimal.valueOf;
import static org.apache.spark.sql.Encoders.*;
import static org.apache.spark.sql.Encoders.bean;
import static org.moita.json.QuarterOfYear.of;

public class ReduceByKey {

    private final String filePath;
    private final SparkSession.Builder builder;
    private Dataset<Row> data;

    public ReduceByKey() {

        Logger.getLogger("org.apache").setLevel(Level.ERROR);

        builder = SparkSession
                .builder()
                .master("local")
                .appName("reduceByKey");

        filePath = this.getClass().getResource("/ccars.csv").getPath();
    }

    public void execute() {

        final Instant start = Instant.now();

        try (SparkSession spark = builder.getOrCreate())
        {

            data = spark.read()
                    .option("header", true)
                    .csv(filePath);

            data.createOrReplaceTempView("ccar");

            Dataset<Row> results = spark.sql("SELECT id, scenario, date, value FROM ccar ");

//            results.show();
//
//            Dataset<Tuple2<QuarterOfYearSer, BigDecimal>> map = results.map((MapFunction<Row, Tuple2<QuarterOfYearSer, BigDecimal>>) row -> {
//
//                final QuarterOfYearSer key = new QuarterOfYearSer(row.getString(2));
//
//                return new Tuple2<>(key, new BigDecimal(row.getString(3)));
//
//            }, tuple(Encoders.javaSerialization(QuarterOfYearSer.class), kryo(BigDecimal.class)));
//
//            map.foreach((ForeachFunction<Tuple2<QuarterOfYearSer, BigDecimal>>) t -> System.out.println(t._1 + " " + t._2));

            JavaPairRDD<QuarterOfYearSer, BigDecimal> pairsJavaPairRDD = results.toJavaRDD().mapToPair((PairFunction<Row, QuarterOfYearSer, BigDecimal>) row -> {
                final QuarterOfYearSer key = new QuarterOfYearSer(row.getString(2));

                return new Tuple2<>(key, new BigDecimal(row.getString(3)));

            });

            JavaPairRDD<QuarterOfYearSer, BigDecimal> reduce = pairsJavaPairRDD.reduceByKey((Function2<BigDecimal, BigDecimal, BigDecimal>) (value1, value2) -> {
                return value1.add(value2);
            });

            reduce.foreach(System.out::println);

            final Instant finish = Instant.now();

            System.out.println("Time elapsed: " + Duration.between(start, finish).toMillis() + " ms.");

            results.explain();
        }
    }

    public static void main(String[] args) {
        new ReduceByKey().execute();
    }
}
