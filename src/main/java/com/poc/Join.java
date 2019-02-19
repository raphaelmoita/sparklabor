package com.poc;

import com.poc.domain.CategoryValue;
import com.poc.domain.Employee;
import com.poc.domain.EnrichedEmployee;
import com.poc.domain.KeyValue;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.Scanner;

import static org.apache.spark.sql.Encoders.*;
import static org.apache.spark.sql.functions.*;

public class Join {

    private static final String DELIMITER = "\\t";
    public static final String LEFT_OUTER = "left_outer";

    private static String EMPLOYEE = "/home/rmoita/dev/projects/sparklabor/src/main/resources/poc/EMPLOYEES.csv";

    private static String KEY_VALUE = "/home/rmoita/dev/projects/sparklabor/src/main/resources/poc/KEY_VALUE.csv";

    private static String CATEGORY_VALUE = "/home/rmoita/dev/projects/sparklabor/src/main/resources/poc/CATEGORY_VALUE.csv";

    private SparkSession spark;

    private Dataset<Row> employeeDataSet;

    private Dataset<Row> keyValueDataSet;

    private Dataset<Row> categoryValueDataSet;

    public Join() {
        spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("spark_labor")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        final DataFrameReader reader = spark.read()
                .format("csv")
                .option("header", "true")
                .option("delimiter", DELIMITER);

        employeeDataSet = reader.load(EMPLOYEE);

        keyValueDataSet = reader.load(KEY_VALUE);

        categoryValueDataSet = reader.load(CATEGORY_VALUE);
    }

    public void calculate() {
        employeeDataSet.show(); keyValueDataSet.show(); categoryValueDataSet.show();

        // Renaming to avoid collision
        keyValueDataSet = keyValueDataSet.withColumnRenamed(KeyValue.VALUE, EnrichedEmployee.KEY_VALUE);
        categoryValueDataSet = categoryValueDataSet.withColumnRenamed(CategoryValue.VALUE, EnrichedEmployee.CATEGORY_VALUE);

        final Column keyValueJoinCondition = employeeDataSet.col(Employee.KEY).equalTo(keyValueDataSet.col(KeyValue.KEY));
        final Column categoryValueJoinCondition = employeeDataSet.col(Employee.CATEGORY).equalTo(categoryValueDataSet.col(CategoryValue.CATEGORY));

        Dataset<Row> join = employeeDataSet
                .join(keyValueDataSet, keyValueJoinCondition, LEFT_OUTER)
                .join(categoryValueDataSet, categoryValueJoinCondition, LEFT_OUTER);

        // drop duplicates columns
        join = join.drop(keyValueDataSet.col(KeyValue.KEY))
                   .drop(categoryValueDataSet.col(CategoryValue.CATEGORY));

        // default value when column is null
        join = join.withColumn(EnrichedEmployee.KEY_VALUE, coalesce(keyValueDataSet.col(EnrichedEmployee.KEY_VALUE), lit("N/A")));
        join = join.withColumn(EnrichedEmployee.CATEGORY_VALUE, coalesce(categoryValueDataSet.col(EnrichedEmployee.CATEGORY_VALUE), lit("-")));

        join.show();

        Dataset<EnrichedEmployee> enrichedEmployeeDataset = join.map((MapFunction<Row, EnrichedEmployee>) EnrichedEmployee::new, kryo(EnrichedEmployee.class));

        enrichedEmployeeDataset.collectAsList().forEach(System.out::println);

        new Scanner(System.in).nextLine();
    }

    public static void main(String[] args) {
        new Join().calculate();
    }
}
