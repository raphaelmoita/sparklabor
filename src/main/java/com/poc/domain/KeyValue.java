package com.poc.domain;

import org.apache.spark.sql.Row;

import java.util.Objects;

public class KeyValue {

    public static final String KEY = "KEY";

    public static final String VALUE = "VALUE";

    private final Row row;

    public KeyValue(Row row) {
        this.row = row;
    }

    public String getKey() {
        return row.getAs(KEY);
    }

    public String getValue() {
        return row.getAs(VALUE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyValue keyValue = (KeyValue) o;
        return Objects.equals(row, keyValue.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(row);
    }
}
