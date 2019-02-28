package com.ubs.playground.domain;

import com.ubs.playground.annotation.Field;

import java.util.Objects;

import static com.ubs.playground.domain.KeyValueLookupMetadata.ColName.CATEGORY;
import static com.ubs.playground.domain.KeyValueLookupMetadata.ColName.KEY;
import static com.ubs.playground.domain.KeyValueLookupMetadata.ColName.VALUE;

public class KeyValueLookup {

    @Field(value = CATEGORY)
    private String category;

    @Field(value = KEY)
    private String key;

    @Field(value = VALUE)
    private String value;

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyValueLookup that = (KeyValueLookup) o;
        return Objects.equals(category, that.category) &&
                Objects.equals(key, that.key) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(category, key, value);
    }
}
