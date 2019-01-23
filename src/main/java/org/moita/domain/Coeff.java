package org.moita.domain;

import java.io.Serializable;
import java.math.BigDecimal;

public class Coeff implements Serializable {

    private String language;
    private BigDecimal coeff;

    public String getLanguage()
    {
        return language;
    }

    public void setLanguage(String language)
    {
        this.language = language;
    }

    public BigDecimal getCoeff()
    {
        return coeff;
    }

    public void setCoeff(BigDecimal coeff)
    {
        this.coeff = coeff;
    }
}
