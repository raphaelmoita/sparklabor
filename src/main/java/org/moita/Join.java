package org.moita;

import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.moita.domain.Coeff;
import org.moita.domain.Country;
import org.moita.domain.Person;

public class Join {

    private static final String TAB = "\\t";

    private static String PERSON_TAB = ""/home/rmoita/dev/projects/sparklabor/data/PERSON.txt";
    private static String COUNTRY_TAB = ""/home/rmoita/dev/projects/sparklabor/data/COUNTRY.txt";
    private static String COEFF_TAB = ""/home/rmoita/dev/projects/sparklabor/data/COEFF.txt";

    private SparkSession spark;

    private Dataset<Row> personDataSet;
    private Dataset<Row> countryDataSet;
    private Dataset<Row> coeffDataSet;

    public Join() {

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

        final Dataset<Row> join = personDF.join(countryDF, "nationality")
            .join(coeffDF, "language");

        join.show();
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
        new ScalaDataSet().execute();
    }
}
