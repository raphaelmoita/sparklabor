package org.moita;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

public class Pivot {

    private final SparkSession spark;
    private final String filePath;
    private final Dataset<Row> data;

    public Pivot() {

        Logger.getLogger("org.apache").setLevel(Level.ERROR);

        spark = SparkSession
                .builder()
                .master("local")
                .appName("pivot")
                .getOrCreate();

        filePath = this.getClass().getResource("/students_small.csv").getPath();

        data = spark.read()
                .option("header", true)
                .csv(filePath);
    }

    public void execute() {
        Dataset<Row> ds = data.groupBy("subject")
                              .pivot("year")
                              .agg(
                                      round(avg(col("score")), 2).alias("average"),
                                      round(stddev(col("score")), 2).alias("std deviation"));
        ds.show();
    }

    public static void main(String[] args) {
        new Pivot().execute();
    }
}
