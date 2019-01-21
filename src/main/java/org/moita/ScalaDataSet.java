package org.moita;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.moita.domain.Person;

import static java.lang.Integer.parseInt;

public class ScalaDataSet {
    private static final String TAB = "\\t";

    private static String TABLE_1 = "/home/rmoita/dev/projects/sparklabor/data/TABLE1.txt";
    private static String TABLE_2 = "/home/rmoita/dev/projects/sparklabor/data/TABLE2.txt";

    private SparkSession spark;

    private Dataset<Row> table1DataSet;
    private Dataset<Row> table2DataSet;

    public ScalaDataSet() {

        spark = SparkSession
                .builder()
                .master("local")
                .appName("spark_labor")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
    }

    public void execute() {
        table1DataSet = spark.read()
                .format("csv")
                .option("header", "false")
                .option("delimiter", TAB)
                .load(TABLE_1);

        table1DataSet.show();

        Dataset<Person> personDF = table1DataSet.map((MapFunction<Row, Person>) line -> {
            Person person = new Person();
            person.setName(line.getString(0));
            person.setAge(Integer.valueOf(line.getString(1)));
            person.setNationality(line.getString(2));
            return person;
        }, Encoders.bean(Person.class));

        personDF.collectAsList().forEach(System.out::println);
    }

    public static void main(String[] args) {
        new ScalaDataSet().execute();
    }
}
