package com.ubs.playground;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.moita.domain.Coeff;
import org.moita.domain.Country;
import org.moita.domain.Person;
import org.moita.domain.Result;

import java.io.Serializable;
import java.math.BigDecimal;

public class JoinDataset {

    private static final String TAB = "\\t";

    private static String PERSON_TAB = "/home/rmoita/dev/projects/sparklabor/data/PERSON.txt";
    private static String COUNTRY_TAB = "/home/rmoita/dev/projects/sparklabor/data/COUNTRY.txt";
    private static String COEFF_TAB = "/home/rmoita/dev/projects/sparklabor/data/COEFF.txt";

    private SparkSession spark;

    private Dataset<Row> personDataSet;
    private Dataset<Row> countryDataSet;
    private Dataset<Row> coeffDataSet;

    public JoinDataset() {

        spark = SparkSession
                .builder()
                .master("local")
                .appName("spark_labor")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
    }

    public void loadData()
    {
        final DataFrameReader frameReader = spark.read()
            .format("csv")
            .option("header", "false")
            .option("delimiter", TAB);

        personDataSet = frameReader.load(PERSON_TAB);

        countryDataSet = frameReader.load(COUNTRY_TAB);

        coeffDataSet = frameReader.load(COEFF_TAB);
    }

    public void execute() {

        loadData();

        Dataset<Person> personDF = personDataSet.map(new RowToPersonMapper(), Encoders.bean(Person.class));
        Dataset<Country> countryDF = countryDataSet.map(new RowToCountryMapper(), Encoders.bean(Country.class));
        Dataset<Coeff> coeffDF = coeffDataSet.map(new RowToCoeffMapper(), Encoders.bean(Coeff.class));

        personDF.join(countryDF, )

        final Dataset<Row> join =
                personDF.join(countryDF, personDF.col("nationality"), "left")
                        .join(coeffDF, "language")
                        .filter((FilterFunction<Row>) r -> !"ana".equals(r.getAs("name")));

        final Dataset<Result> results = join.map((MapFunction<Row, Result>) Result::calculate, Encoders.bean(Result.class));
        final Dataset<Result> results2 = join.map((MapFunction<Row, Result>) Result::calculate, Encoders.bean(Result.class));

        JavaRDD<Result> union = results.toJavaRDD().union(results2.toJavaRDD());

        Dataset<Row> unionDataSet = spark.createDataFrame(union, Result.class);

        Dataset<Result> unionDataSet_NPE = spark.createDataFrame(union, Result.class)
                .map((MapFunction<Row, Result>) Result::calculate, Encoders.bean(Result.class));

        results.show();
        results2.show();
        unionDataSet.show();
    }

    private static class Sum implements Function2<Row, Row, Row> {
        @Override
        public Row call(Row r1, Row r2) {
            return RowFactory.create(r1);
        }
    }

    static class RowToPersonMapper implements MapFunction<Row, Person>, Serializable {

        @Override
        public Person call(Row row) throws Exception {
            Person person = new Person();
            person.setName(row.getString(0));
            person.setAge(Integer.valueOf(row.getString(1)));
            person.setNationality(row.getString(2));
            return person;
        }
    }

    static class RowToCountryMapper implements MapFunction<Row, Country>, Serializable {

        @Override
        public Country call(Row row) throws Exception {
            Country country = new Country();
            country.setNationality(row.getString(0));
            country.setLanguage(row.getString(1));
            return country;
        }
    }

    static class RowToCoeffMapper implements MapFunction<Row, Coeff>, Serializable {

        @Override
        public Coeff call(Row row) throws Exception {
            Coeff coeff = new Coeff();
            coeff.setLanguage(row.getString(0));
            coeff.setCoeff(new BigDecimal(row.getString(1)));
            return coeff;
        }
    }

    public static void main(String[] args) {
        new JoinDataset().execute();
    }
}
