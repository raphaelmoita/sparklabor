package org.moita.json.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.moita.json.Person;
import org.moita.json.QuarterOfYear;

import java.io.IOException;

public class JsonSerializer {

    public static void main(String[] args) throws IOException {
        serializePerson();
        serializeQuarterOfYear();
    }

    private static void serializePerson() throws IOException {
        Person person = new Person.Builder()
                .withName("Person Name")
                .withAge(333)
                .build();

        ObjectMapper mapper = new ObjectMapper();

        String json = mapper.writeValueAsString(person);

        Person personDes = mapper.readValue(json, Person.class);

        System.out.println(json);

        System.out.println(personDes.toString());
    }

    private static void serializeQuarterOfYear() throws IOException {
        QuarterOfYear quarterOfYear = QuarterOfYear.of(QuarterOfYear.Quarter.Q1, 2018);
        System.out.println(quarterOfYear);
        System.out.println(quarterOfYear.next());
        System.out.println(quarterOfYear.previous());
        System.out.println(quarterOfYear);

        ObjectMapper mapper = new ObjectMapper();

        String json = mapper.writeValueAsString(quarterOfYear);

        QuarterOfYear quarterOfYear1Des = mapper.readValue(json, QuarterOfYear.class);

        System.out.println(json);

        System.out.println(quarterOfYear1Des.toString());
    }

}
