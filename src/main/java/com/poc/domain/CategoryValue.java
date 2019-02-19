package com.poc.domain;

import org.apache.spark.sql.Row;

import java.util.Objects;

public class CategoryValue {

    public static final String CATEGORY = "CATEGORY";

    public static final String VALUE = "VALUE";

    private final Row row;

    public CategoryValue(Row row) {
        this.row = row;
    }

    public String getCategory() {
        return row.getAs(CATEGORY);
    }

    public String getValue() {
        return row.getAs(VALUE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CategoryValue keyValue = (CategoryValue) o;
        return Objects.equals(row, keyValue.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(row);
    }
}
