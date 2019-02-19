package com.poc.domain;

import org.apache.spark.sql.Row;

import java.util.Objects;

public class Employee {

    public static final String NAME = "NAME";

    public static final String KEY = "KEY";

    public static final String CATEGORY = "CATEGORY";

    private final Row row;

    public Employee(Row row) {
        this.row = row;
    }

    public String getName() {
        return row.getAs(NAME);
    }

    public String getKey() {
        return row.getAs(KEY);
    }

    public String getCategory() {
        return row.getAs(CATEGORY);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Employee employee = (Employee) o;
        return Objects.equals(row, employee.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(row);
    }
}
