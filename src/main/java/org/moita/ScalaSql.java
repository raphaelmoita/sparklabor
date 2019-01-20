package org.moita;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.moita.domain.Country;
import org.moita.domain.Person;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Integer.parseInt;

public class ScalaSql {
    private static final String TAB = "\\t";

    private static String TABLE_1 = "/home/rmoita/dev/projects/sparklabor/data/TABLE1.txt";
    private static String TABLE_2 = "/home/rmoita/dev/projects/sparklabor/data/TABLE2.txt";

    private SparkSession spark;

    private Dataset<Row> table1DataSet;
    private Dataset<Row> table2DataSet;

    private JavaRDD<Person> personJavaRDD;
    private JavaRDD<Country> countryJavaRDD;
    private JavaRDD<Row> countryRowJavaRDD;

    private StructType countrySchema;

    public ScalaSql() {
        //System.setProperty("hadoop.home.dir", "/apps/spark-2.4.0-bin-hadoop2.7");

        spark = SparkSession
                .builder()
                .master("local")
                .appName("spark_labor")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
    }

    public void load() {
        table1DataSet = spark.read()
                .format("csv")
                .option("header", "false")
                .option("delimiter", TAB)
                .load(TABLE_1);

        table1DataSet.show();

        table2DataSet = spark.read()
                .format("csv")
                .option("header", "false")
                .option("delimiter", TAB)
                .load(TABLE_2);

        table2DataSet.show();
    }

    public void createRDDs() {
        personJavaRDD = spark.read()
                .textFile(TABLE_1)
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(TAB);
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(parseInt(parts[1]));
                    person.setNacionality(parts[2]);
                    return person;
                });

        countryJavaRDD = spark.read()
                .textFile(TABLE_2)
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(TAB);
                    Country country = new Country();
                    country.setNacionality(parts[0]);
                    country.setLanguage(parts[1].trim());
                    return country;
                });

        // create schema
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("nacionality", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("language", DataTypes.IntegerType, true));
        countrySchema = DataTypes.createStructType(structFields);

        countryRowJavaRDD = spark.read()
                .textFile(TABLE_2)
                .javaRDD()
                .map(line -> {
                    String[] cols = line.split(TAB);
                    return RowFactory.create(cols[0], cols[1]);
                });
    }

    public void createDataFrames() {
        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(personJavaRDD, Person.class);
        // Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("people");

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> countryDF = spark.createDataFrame(countryJavaRDD, Country.class);
        // Register the DataFrame as a temporary view
        countryDF.createOrReplaceTempView("country");

        // Apply the schema to the RDD
        Dataset<Row> countryRowDF = spark.createDataFrame(countryRowJavaRDD, countrySchema);
        // Register the DataFrame as a temporary view
        countryDF.createOrReplaceTempView("row_country");
    }

    public void filter() {
        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> teenagersDF = spark.sql("SELECT name, language FROM people p, country c WHERE p.nacionality = c.nacionality");

        System.out.println("teenagersDF >>");
        teenagersDF.show();

        JavaRDD<Person> under41 = personJavaRDD.filter(person -> person.getAge() < 200);
        Dataset<Row> peopleUnder41DF = spark.createDataFrame(under41, Person.class);
        // Register the DataFrame as a temporary view
        peopleUnder41DF.createOrReplaceTempView("peopleUnder41");

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> peopleUnder41Sql = spark.sql("SELECT name FROM peopleUnder41");

        System.out.println("peopleUnder41 >>");
        peopleUnder41Sql.show();

        under41.collect().forEach(System.out::println);
    }

    public void execute() {
        try {
            load();
            createRDDs();
            createDataFrames();
            filter();
        } finally {
            spark.stop();
        }
    }

    public static void main(String[] args) {
        new ScalaSql().execute();
    }
}
