package org.moita;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.moita.domain.Country;
import org.moita.domain.Person;

import static java.lang.Integer.parseInt;

public class ScalaSql {
    private static final String TAB = "\\t";

    private static String TABLE_1 = "/home/rmoita/dev/projects/sparklabor/data/TABLE1.txt";
    private static String TABLE_2 = "/home/rmoita/dev/projects/sparklabor/data/TABLE2.txt";

    private SparkSession spark;

    private JavaRDD<Person> personRdd;
    private JavaRDD<Country> countryRdd;

    private JavaRDD<Row> countryRowJavaRDD;

    private StructType countrySchema;

    public ScalaSql() {
        spark = SparkSession
                .builder()
                .master("local")
                .appName("spark_labor")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
    }

    public void execute() {
        personRdd = spark.read()
                .textFile(TABLE_1)
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(TAB);
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(parseInt(parts[1]));
                    person.setNationality(parts[2]);
                    return person;
                });

        countryRdd = spark.read()
                .textFile(TABLE_2)
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(TAB);
                    Country country = new Country();
                    country.setNationality(parts[0]);
                    country.setLanguage(parts[1].trim());
                    return country;
                });

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(personRdd, Person.class);
        // Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("people");

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> countryDF = spark.createDataFrame(countryRdd, Country.class);
        // Register the DataFrame as a temporary view
        countryDF.createOrReplaceTempView("country");

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> query = spark.sql(
                "SELECT name, language " +
                        "FROM people p, country c " +
                        "WHERE p.nationality = c.nationality");

        query.show();

        query.collectAsList().forEach(System.out::println);
    }

    public static void main(String[] args) {
        new ScalaSql().execute();
    }
}
