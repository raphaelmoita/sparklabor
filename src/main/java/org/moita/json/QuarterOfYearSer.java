package org.moita.json;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class QuarterOfYearSer implements Comparable<QuarterOfYearSer>, Serializable
{
    private int quarter;

    private int year;

    public QuarterOfYearSer() {}

    public QuarterOfYearSer(String date) {
        DateFormat df = new SimpleDateFormat("yyyyMM");
        Calendar calendar = Calendar.getInstance();
        try {
            calendar.setTime(df.parse(date));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        int month = calendar.get(Calendar.MONTH);
        int year = calendar.get(Calendar.YEAR);
        int quarter = month / 3 + 1;

        this.quarter = quarter;
        this.year = year;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getQuarter() {
        return quarter;
    }

    public void setQuarter(int quarter) {
        this.quarter = quarter;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("quarter", quarter)
                .append("year", year)
                .toString();
    }

    @Override
    public int compareTo(QuarterOfYearSer o) {
        return new CompareToBuilder()
                .append(o.year, this.year)
                .append(o.quarter, this.quarter)
                .toComparison();
    }
}
