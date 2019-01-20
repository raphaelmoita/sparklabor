package org.moita.domain;

import java.io.Serializable;

public class Country implements Serializable {

    private String nacionality;
    private String language;

    public String getNacionality() {
        return nacionality;
    }

    public void setNacionality(String nacionality) {
        this.nacionality = nacionality;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }
}