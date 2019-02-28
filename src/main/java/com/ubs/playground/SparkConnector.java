package com.ubs.playground;

import com.poc.domain.KeyValue;
import com.ubs.playground.domain.Expenses;
import com.ubs.playground.domain.KeyValueLookup;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SparkConnector {

    private SparkSession spark;

    private Dataset<Expenses> expensesDataset;
    private Dataset<KeyValue> keyValueDataset;
    private Dataset<KeyValueLookup> keyValueLookupDataset;

    public SparkConnector() {

        spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("spark_icpm")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");


    }
    /*
        SELECT
            CASE kv1 IS NOT NUL: 'xxx'
            CASE kv2 IS NOT NUL: 'yyy'
            ELSE 'zzz' AS business,



    */
}
