package org.moita;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.time.Duration;
import java.time.Instant;

public class Sql {

    private final String filePath;
    private final SparkSession.Builder builder;
    private Dataset<Row> data;

    public Sql() {

        Logger.getLogger("org.apache").setLevel(Level.ERROR);

        builder = SparkSession
                .builder()
                .master("local")
                .appName("sql");

        filePath = this.getClass().getResource("/students_small.csv").getPath();
    }

    public void execute() {

        final Instant start = Instant.now();

        SparkSession spark = builder.getOrCreate();
        {

            data = spark.read()
                        .option("header", true)
                        .csv(filePath);

            data.createOrReplaceTempView("students");

            Dataset<Row> results = spark.sql("SELECT subject, DATE_FORMAT(year, 'MMM') as year, SUM(score) as total_score " +
                    "FROM students " +
                    "GROUP BY subject, year " +
                    "ORDER BY year");

            results.show();

            results.write().option("header", true)
                .csv("/home/rmoita/dev/projects/sparklabor/data/out");

            final Instant finish = Instant.now();

            System.out.println("Time elapsed: " + Duration.between(start, finish).toMillis() + " ms.");

            results.explain();
        }
    }

    public static void main(String[] args) {
        new Sql().execute();
    }
}
