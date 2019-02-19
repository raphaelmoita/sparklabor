package com.poc;

import com.poc.domain.CategoryValue;
import com.poc.domain.Employee;
import com.poc.domain.EnrichedEmployee;
import com.poc.domain.KeyValue;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

import static org.apache.spark.sql.Encoders.kryo;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.lit;

public class JoinTyped {

    private static final String DELIMITER = "\\t";

    private static String EMPLOYEE = "/home/rmoita/dev/projects/sparklabor/src/main/resources/poc/EMPLOYEES.csv";

    private static String KEY_VALUE = "/home/rmoita/dev/projects/sparklabor/src/main/resources/poc/KEY_VALUE.csv";

    private static String CATEGORY_VALUE = "/home/rmoita/dev/projects/sparklabor/src/main/resources/poc/CATEGORY_VALUE.csv";

    private SparkSession spark;

    private Dataset<Employee> employeeDataSet;

    private Dataset<KeyValue> keyValueDataSet;

    private Dataset<CategoryValue> categoryValueDataSet;

    public JoinTyped() {
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

        employeeDataSet = reader.load(EMPLOYEE)
                .map((MapFunction<Row, Employee>) Employee::new, kryo(Employee.class));

        keyValueDataSet = reader.load(KEY_VALUE)
                .map((MapFunction<Row, KeyValue>) KeyValue::new, kryo(KeyValue.class));

        categoryValueDataSet = reader.load(CATEGORY_VALUE)
                .map((MapFunction<Row, CategoryValue>) CategoryValue::new, kryo(CategoryValue.class));
    }

    public void calculate() {
        employeeDataSet.show(); keyValueDataSet.show(); categoryValueDataSet.show();

        // Renaming
//        keyValueDataSet = keyValueDataSet.withColumnRenamed(KeyValue.VALUE, EnrichedEmployee.KEY_VALUE);
//        categoryValueDataSet = categoryValueDataSet.withColumnRenamed(CategoryValue.VALUE, EnrichedEmployee.CATEGORY_VALUE);

        Dataset<Row> join = employeeDataSet
                .join(keyValueDataSet,
                    employeeDataSet.col(Employee.KEY).equalTo(keyValueDataSet.col(KeyValue.KEY)), "left_outer")
                .join(categoryValueDataSet,
                    employeeDataSet.col(Employee.CATEGORY).equalTo(categoryValueDataSet.col(CategoryValue.CATEGORY)), "left_outer");

        join = join.drop(keyValueDataSet.col(KeyValue.KEY))
                   .drop(categoryValueDataSet.col(CategoryValue.CATEGORY));

        // default value
        join = join.withColumn(EnrichedEmployee.KEY_VALUE, coalesce(keyValueDataSet.col(EnrichedEmployee.KEY_VALUE), lit("N/A")));
        join = join.withColumn(EnrichedEmployee.CATEGORY_VALUE, coalesce(categoryValueDataSet.col(EnrichedEmployee.CATEGORY_VALUE), lit("-")));

        Dataset<EnrichedEmployee> enrichedDataSet = join.map((MapFunction<Row, EnrichedEmployee>) EnrichedEmployee::new, kryo(EnrichedEmployee.class));

        enrichedDataSet.collectAsList().forEach(System.out::println);

        new Scanner(System.in).nextLine();
    }

    public static void main(String[] args) {
        new JoinTyped().calculate();
    }
}
