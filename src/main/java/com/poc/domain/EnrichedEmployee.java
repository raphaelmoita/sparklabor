package com.poc.domain;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.spark.sql.Row;

import java.util.Objects;

public class EnrichedEmployee {

    public static final String NAME = "NAME";

    public static final String KEY = "KEY";

    public static final String KEY_VALUE = "KEY_VALUE";

    public static final String CATEGORY = "CATEGORY";

    public static final String CATEGORY_VALUE = "CATEGORY_VALUE";

    private final Row row;

    public EnrichedEmployee(Row row) {
        this.row = row;
    }

    public String getName() {
        return row.getAs(NAME);
    }

    public String getKey() {
        return row.getAs(KEY);
    }

    public String getKeyValue() {
        return row.getAs(KEY_VALUE);
    }

    public String getCategory() {
        return row.getAs(CATEGORY);
    }

    public String getCategoryValue() {
        return row.getAs(CATEGORY_VALUE);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("row", row)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnrichedEmployee that = (EnrichedEmployee) o;
        return Objects.equals(row, that.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(row);
    }
}
