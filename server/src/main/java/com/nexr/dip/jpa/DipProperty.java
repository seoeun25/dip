package com.nexr.dip.jpa;


import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;
@Entity
@NamedQueries({

        @NamedQuery(name = "GET_DIPPROPERTY_BY_NAME", query = "select a.name, a.value " +
                "from DipProperty a where a.name = :name "),
        @NamedQuery(name = "GET_DIPPROPERTY_ALL", query = "select a.name, a.value from DipProperty a ")

})
@Table(name="dipprops")
@XmlRootElement
public class DipProperty {

    @Column(name = "name")
    private String name;

    @Column(name = "value")
    private String value;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
