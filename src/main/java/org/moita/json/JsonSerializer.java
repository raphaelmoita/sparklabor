package org.moita.json;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JsonSerializer {

    public static void main(String[] args) throws IOException {

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

}
