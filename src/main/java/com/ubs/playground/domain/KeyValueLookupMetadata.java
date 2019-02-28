package com.ubs.playground.domain;

import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.StringType;

public final class KeyValueLookupMetadata {

    public static class ColName
    {
        public static final String CATEGORY = "CATEGORY";

        public static final String KEY = "KEY";

        public static final String VALUE = "VALUE";
    }

    public static class Col
    {
        public static final Column CATEGORY = col(ColName.CATEGORY).cast(StringType);

        public static final Column KEY = col(ColName.KEY).cast(StringType);

        public static final Column VALUE = col(ColName.VALUE).cast(StringType);
    }
}
