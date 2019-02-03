package org.moita;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.Duration;
import java.time.Instant;

public class SqlAggregateAlgorithms {

    private final String filePath;
    private final SparkSession.Builder builder;
    private Dataset<Row> data;

    public SqlAggregateAlgorithms() {

        Logger.getLogger("org.apache").setLevel(Level.ERROR);

        builder = SparkSession
                .builder()
                .master("local[*]")
                .appName("sql");

        filePath = this.getClass().getResource("/biglog.txt").getPath();
    }

    public void execute() {

        final Instant start = Instant.now();

//        try (SparkSession spark = builder.getOrCreate()) {
        SparkSession spark = builder.getOrCreate();

        data = spark.read()
                .option("header", true)
                .csv(filePath);

        data.createOrReplaceTempView("logging_table");

        // SortAggregate and HashAggregate (HashAggregation algorithms are much faster)
        // All the fields that does NOT belong to GROUPING sections must be mutable in order
        //  to Spark use HashAggregate algorithm.

        // Time elapsed: 45905 ms.
        // SortAggregate: it has 'month_num' as String
        Dataset<Row> results = spark.sql("SELECT level, DATE_FORMAT(datetime, 'MMMM') as month, FIRST(DATE_FORMAT(datetime, 'M')) as month_num, count(1) as total" +
                " FROM logging_table" +
                " GROUP BY level, month" +
                " ORDER BY level, CAST(month_num as int)");

        // Time elapsed: 15080 ms.
        // HashAggregate: it has 'month_num' cast to int
        results = spark.sql("SELECT level, DATE_FORMAT(datetime, 'MMMM') as month, FIRST(CAST(DATE_FORMAT(datetime, 'M') as int)) as month_num, count(1) as total" +
                " FROM logging_table" +
                " GROUP BY level, month" +
                " ORDER BY level, month_num");

        // Time elapsed: 15992 ms.
        // HashAggregate: it has 'month_num' as part of grouping section
        results = spark.sql("SELECT level, DATE_FORMAT(datetime, 'MMMM') as month, DATE_FORMAT(datetime, 'M') as month_num, count(1) as total" +
                " FROM logging_table" +
                " GROUP BY level, month, month_num" +
                " ORDER BY level, month_num");

        // this column is being used ony for ordering by month. It is not possible to use 'month' column
        // because we do NOT want to order it alphabetically.
        results.drop("month_num");
        results.show();

        final Instant finish = Instant.now();

        System.out.println("Time elapsed: " + Duration.between(start, finish).toMillis() + " ms.");

        results.explain();
//    }
    }

    public static void main(String[] args) {
        new SqlAggregateAlgorithms().execute();
    }
}
