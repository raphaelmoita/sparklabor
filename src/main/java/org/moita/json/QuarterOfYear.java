package org.moita.json;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

@JsonDeserialize(builder = QuarterOfYear.Builder.class)
public class QuarterOfYear
{
    public enum Quarter {
        Q1(1),
        Q2(2),
        Q3(3),
        Q4(4);

        int quarter;

        Quarter(int quarter) {
            this.quarter = quarter;
        }

        int asInt() {
            return quarter;
        }
    }

    private Quarter quarter;

    private int year;

    private QuarterOfYear(Quarter quarter, int year) {
        this.quarter = quarter;
        this.year = year;
    }

    public Quarter getQuarter() {
        return quarter;
    }

    public int getYear() {
        return year;
    }

    public QuarterOfYear next() {
        QuarterOfYear quarterOfYear;
        if (quarter.equals(Quarter.Q4)) {
            quarterOfYear = new QuarterOfYear(Quarter.Q1, year + 1);
        } else {
            quarterOfYear = new QuarterOfYear(Quarter.values()[quarter.ordinal() + 1], year);
        }
        return quarterOfYear;
    }

    public QuarterOfYear previous() {
        QuarterOfYear quarterOfYear;
        if (quarter.equals(Quarter.Q1)) {
            quarterOfYear = new QuarterOfYear(Quarter.Q4, year - 1);
        } else {
            quarterOfYear = new QuarterOfYear(Quarter.values()[quarter.ordinal() - 1], year);
        }
        return quarterOfYear;
    }

    public static QuarterOfYear of(int quarter, int year) {
        return of("Q" + quarter, year);
    }

    public static QuarterOfYear of(Quarter quarter, int year) {
        return new QuarterOfYear(quarter, year);
    }

    public static QuarterOfYear of(String quarter, int year) {
        String strQuarter = quarter.startsWith("Q") ? quarter : "Q" + quarter;
        return of(Quarter.valueOf(strQuarter), year);
    }

    public static QuarterOfYear of(String quarter, String year) {
        return of(quarter, Integer.valueOf(year));
    }

    public static QuarterOfYear of(String date) {
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

        return of(quarter, year);
    }

    @JsonPOJOBuilder()
    static class Builder {

        private int year;

        private Quarter quarter;

        public Builder withYear(int year) {
            this.year = year;
            return this;
        }

        public Builder withQuarter(Quarter quarter) {
            this.quarter = quarter;
            return this;
        }

        public QuarterOfYear build() {
            return new QuarterOfYear(quarter, year);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        QuarterOfYear that = (QuarterOfYear) o;

        return new EqualsBuilder()
                .append(year, that.year)
                .append(quarter, that.quarter)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(year)
                .append(quarter)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("year", year)
                .append("quarter", quarter)
                .toString();
    }

    // FIXME: remove it
    public static void main(String[] args) {
        QuarterOfYear quarterOfYear = QuarterOfYear.of(Quarter.Q1, 2018);
        System.out.println(quarterOfYear);
        System.out.println(quarterOfYear.next());
        System.out.println(quarterOfYear.previous());
        System.out.println(quarterOfYear);
        System.out.println(QuarterOfYear.of(Quarter.Q3, 1977));
        System.out.println(QuarterOfYear.of("Q3", 1987));
        System.out.println(QuarterOfYear.of("Q2", "1977"));
        System.out.println(QuarterOfYear.of("2", "1979"));
        System.out.println(QuarterOfYear.of("201812"));
    }
}
