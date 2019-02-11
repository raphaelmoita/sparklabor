package org.moita;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import java.time.Duration;
import java.time.Instant;

public class UserDefinedFunc {

    private final String filePath;
    private final SparkSession.Builder builder;
    private Dataset<Row> data;

    public UserDefinedFunc() {

        Logger.getLogger("org.apache").setLevel(Level.ERROR);

        builder = SparkSession
                .builder()
                .master("local")
                .appName("reduceByKey");

        filePath = this.getClass().getResource("/ccars.csv").getPath();
    }

    // TODO: why it does not work when not defined inline to register?
    private UserDefinedFunction dateToQuarter = functions.udf(
            (String date) -> {
                String year = date.substring(0,4);
                String month = date.substring(4,6);

                switch (month) {
                    case "01":
                    case "02":
                    case "03":
                        return year + "Q1";
                    case "04":
                    case "05":
                    case "06":
                        return year + "Q2";
                    case "07":
                    case "08":
                    case "09":
                        return year + "Q3";
                    case "10":
                    case "11":
                    case "12":
                        return year + "Q4";
                    default:
                        return "";
                }
            }, DataTypes.StringType
    );

    private UserDefinedFunction key = functions.udf(
            (String col1, String col2) -> col1 + "-" + col2 , DataTypes.StringType);

    public void execute() {

        final Instant start = Instant.now();

        try (SparkSession spark = builder.getOrCreate())
        {

            data = spark.read()
                    .option("header", true)
                    .csv(filePath);

            data.createOrReplaceTempView("ccar");

            spark.sqlContext().udf().register("dateToQuarter", (String date) -> {
                        String year = date.substring(0,4);
                        String month = date.substring(4,6);

                        switch (month) {
                            case "01":
                            case "02":
                            case "03":
                                return year + "Q1";
                            case "04":
                            case "05":
                            case "06":
                                return year + "Q2";
                            case "07":
                            case "08":
                            case "09":
                                return year + "Q3";
                            case "10":
                            case "11":
                            case "12":
                                return year + "Q4";
                            default:
                                return "";
                        }
                    }, DataTypes.StringType
            );

            spark.sqlContext().udf().register("key", key);

            Dataset<Row> results = spark.sql("SELECT key(id, scenario) as key, dateToQuarter(date) as quarter_of_year, value FROM ccar ");

            //results.show();

            RelationalGroupedDataset relationalGroupedDataset = results.groupBy("key", "quarter_of_year");

//            relationalGroupedDataset = relationalGroupedDataset.pivot("value");

            Dataset<Row> agg = relationalGroupedDataset.agg(functions.sum("value").alias("value"));

            Dataset<Row> ordered = agg.orderBy("quarter_of_year");

            ordered.show();

            final Instant finish = Instant.now();

            System.out.println("Time elapsed: " + Duration.between(start, finish).toMillis() + " ms.");

        }
    }

    public static void main(String[] args) {
        new UserDefinedFunc().execute();
    }
}
