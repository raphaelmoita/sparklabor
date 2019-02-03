package org.moita;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

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

        Dataset<Row> results;

        try (SparkSession spark = builder.getOrCreate()) {

            data = spark.read()
                    .option("header", true)
                    .csv(filePath);

            data.createOrReplaceTempView("logging_table");

            // SortAggregate and HashAggregate (HashAggregation algorithms are much faster)
            // All the fields that does NOT belong to GROUPING sections must be mutable in order
            //  to Spark use HashAggregate algorithm.

            // Time elapsed: ~45s
            // SortAggregate: it has 'month_num' as String
//            results = sortedAggregateExecution(spark);

            // Time elapsed: ~15s ms.
            // HashAggregate: it has 'month_num' cast to int
//            results = hashAggregatedExecution(spark);

            // Time elapsed: ~15s ms.
            // HashAggregate: it has 'month_num' as part of grouping section
//            results = hashAggregatedExecution2(spark);

            // Time elapsed: ~15s ms.
            // Using Java API version
            results = javaApiVersion();

            results.show();

            final Instant finish = Instant.now();

            System.out.println("Time elapsed: " + Duration.between(start, finish).toMillis() + " ms.");

            results.explain();
        }
    }

    private Dataset<Row> javaApiVersion() {

        Dataset<Row> results;
        Column levelCol = col("level");
        Column datetimeCol = col("datetime");

        results = data.select(levelCol,
                date_format(datetimeCol, "MMMM").alias("month"),
                date_format(datetimeCol, "M").alias("month_num").cast(DataTypes.IntegerType));

        RelationalGroupedDataset relationalGroupedDataset = results.groupBy("level", "month", "month_num");

        results = relationalGroupedDataset.count().as("total");

        results = results.orderBy("month_num");

        // this column is being used ony for ordering by month. It is not possible to use 'month' column
        // because we do NOT want to order it alphabetically.
        results = results.drop("month_num");

        return results;
    }

    private Dataset<Row> hashAggregatedExecution2(SparkSession spark) {

        Dataset<Row> results = spark.sql("SELECT level, DATE_FORMAT(datetime, 'MMMM') as month, DATE_FORMAT(datetime, 'M') as month_num, count(1) as total" +
                " FROM logging_table" +
                " GROUP BY level, month, month_num" +
                " ORDER BY level, month_num");

        // this column is being used ony for ordering by month. It is not possible to use 'month' column
        // because we do NOT want to order it alphabetically.
        results.drop("month_num");

        return results;
    }

    private Dataset<Row> hashAggregatedExecution(SparkSession spark) {

        Dataset<Row> results = spark.sql("SELECT level, DATE_FORMAT(datetime, 'MMMM') as month, FIRST(CAST(DATE_FORMAT(datetime, 'M') as int)) as month_num, count(1) as total" +
                " FROM logging_table" +
                " GROUP BY level, month" +
                " ORDER BY level, month_num");

        // this column is being used ony for ordering by month. It is not possible to use 'month' column
        // because we do NOT want to order it alphabetically.
        results.drop("month_num");

        return results;
    }

    private Dataset<Row> sortedAggregateExecution(SparkSession spark) {

        Dataset<Row> results = spark.sql("SELECT level, DATE_FORMAT(datetime, 'MMMM') as month, FIRST(DATE_FORMAT(datetime, 'M')) as month_num, count(1) as total" +
                " FROM logging_table" +
                " GROUP BY level, month" +
                " ORDER BY level, CAST(month_num as int)");

        results.drop("month_num");

        return results;
    }

    public static void main(String[] args) {
        new SqlAggregateAlgorithms().execute();
    }
}
