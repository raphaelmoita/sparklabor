package org.moita;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.moita.domain.Person;

import static java.lang.Integer.parseInt;

public class ScalaPojo {

    private static final String TAB = "\\t";

    private static String TABLE_1 = "/home/rmoita/dev/projects/sparklabor/data/TABLE1.txt";

    private SparkSession spark;

    private JavaRDD<Person> personJavaRDD;

    public ScalaPojo() {
        spark = SparkSession
                .builder()
                .master("local")
                .appName("spark_labor")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
    }

    public void execute() {
        personJavaRDD = spark.read()
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

        JavaRDD<Person> under41 = personJavaRDD.filter(person -> person.getAge() < 41);

        under41.collect().forEach(System.out::println);
    }

    public static void main(String[] args) {
        new ScalaPojo().execute();
    }
}
