package org.moita.domain;

import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.math.BigDecimal;

import static java.math.BigDecimal.ONE;

public class Result implements Serializable {

    private String name;
    private Integer age;
    private String nationality;
    private String language;
    private BigDecimal coeff;

    public Result() {}

    public static Result calculate(Row row) {
        Result r = new Result();
        r.setName(row.getAs("name"));
        r.setCoeff(row.getDecimal(4).add(ONE));
        return r;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getNationality() {
        return nationality;
    }

    public void setNationality(String nationality) {
        this.nationality = nationality;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public BigDecimal getCoeff() {
        return coeff;
    }

    public void setCoeff(BigDecimal coeff) {
        this.coeff = coeff;
    }
}
