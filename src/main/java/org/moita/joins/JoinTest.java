package org.moita.joins;

import org.apache.spark.sql.*;

public class JoinTest {

    private static final String TAB = "\\t";

    private static String EMPLOYEE = "/home/rmoita/dev/projects/sparklabor/src/main/resources/joins/employee.csv";
    private static String COMPANY = "/home/rmoita/dev/projects/sparklabor/src/main/resources/joins/company.csv";

    private SparkSession spark;

    private Dataset<Row> employeeDataSet;
    private Dataset<Row> companyDataSet;

    public JoinTest() {

        spark = SparkSession
                .builder()
                .master("local")
                .appName("spark_labor")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        final DataFrameReader frameReader = spark.read()
                .format("csv")
                .option("header", "true")
                .option("delimiter", TAB);

        employeeDataSet = frameReader.load(EMPLOYEE);

        companyDataSet = frameReader.load(COMPANY);

        employeeDataSet.show();
        companyDataSet.show();

        // company columns
        final Column companyCode = companyDataSet.col("CODE");
        final Column companyName = companyDataSet.col("NAME");

        // employee columns
        final Column employeeCompanyCode = employeeDataSet.col("COMPANY_CODE");
        final Column employeeName = employeeDataSet.col("NAME");
        final Column employeeAge = employeeDataSet.col("AGE");

        Dataset<Row> join = employeeDataSet.join(companyDataSet, employeeCompanyCode.equalTo(companyCode));

        join = join.drop(companyCode).drop(employeeCompanyCode);

        join = join.select(employeeName, employeeAge, companyName.alias("COMPANY_NAME"));

//        join = join.withColumnRenamed(companyName.named().name(), "COMPANY_NAME");

        join.show();

    }

    public static void main(String[] args) {
        new JoinTest();
    }
}
