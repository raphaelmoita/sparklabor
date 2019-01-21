package org.moita.domain;

import java.io.Serializable;

public class Country implements Serializable {

    private String nationality;
    private String language;

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
}