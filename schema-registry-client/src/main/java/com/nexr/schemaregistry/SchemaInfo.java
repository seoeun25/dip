package com.nexr.schemaregistry;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.IOException;
import java.util.Calendar;

@Entity
@NamedQueries({

        @NamedQuery(name = "GET_BYSCHEMA", query = "select a.name, a.id, a.schemaStr, a.created " +
                "from SchemaInfo a where a.schemaStr = :schemaStr "),
        @NamedQuery(name = "GET_BYID", query = "select a.name, a.id, a.schemaStr, a.created " +
                "from SchemaInfo a where a.id = :id "),
        @NamedQuery(name = "GET_BYTOPICLATEST", query = "select a.name, a.id, a.schemaStr, a.created " +
                "from SchemaInfo a where a.name = :name order by a.created desc "),
        @NamedQuery(name = "GET_BYTOPICALL", query = "select a.name, a.id, a.schemaStr, a.created " +
                "from SchemaInfo a where a.name = :name order by a.created desc "),
        @NamedQuery(name = "GET_BYTOPICANDID", query = "select a.name, a.id, a.schemaStr, a.created " +
                "from SchemaInfo a where a.name = :name and a.id = :id "),
        @NamedQuery(name = "GET_ALL", query = "select a.name, a.id, a.schemaStr, a.created from SchemaInfo a where a.id " +
                "in (select max(a.id) from SchemaInfo a group by a.name) ")

})
@Table(name = "schemainfo")
@XmlRootElement
public class SchemaInfo {

    @Column(name = "name")

    private String name;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id" )
    private int id;

    @Column(name = "schemaStr", length = 20480)
    private String schemaStr;

    @Column(name = "created")
    @Temporal(TemporalType.TIMESTAMP)
    private Calendar created;

    public SchemaInfo() {
        this.created = Calendar.getInstance();
    }

    public SchemaInfo(String name, String schemaStr) {
        this.name = name;
        this.schemaStr = schemaStr;
        this.created = Calendar.getInstance();
    }

    public SchemaInfo(String name, int id, String schemaStr) {
        this.name = name;
        this.id = id;
        this.schemaStr = schemaStr;
        this.created = Calendar.getInstance();
    }

    public SchemaInfo(@JsonProperty("name") String name, @JsonProperty("id") int id, @JsonProperty("schemaStr") String
            schemaStr, @JsonProperty("created") Long created) {
        this.name = name;
        this.id = id;
        this.schemaStr = schemaStr;
        Calendar createdCalendar = Calendar.getInstance();
        createdCalendar.setTimeInMillis(created.longValue());
        this.created = createdCalendar;
    }

    @JsonProperty("id")
    public int getId() {
        return id;
    }

    @JsonProperty("id")
    public void setId(int id) {
        this.id = id;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("schemaStr")
    public String getSchemaStr() {
        return schemaStr;
    }

    @JsonProperty("schemaStr")
    public void setSchemaStr(String schemaStr) {
        this.schemaStr = schemaStr;
    }

    @JsonProperty("created")
    public Calendar getCreated() {
        return created;
    }

    @JsonProperty("created")
    public void setCreated(Calendar created) {
        this.created = created;
    }

    public Schema parseSchema() {
        Schema schema = new Schema.Parser().parse(schemaStr);
        return schema;
    }

    public String toJson() throws IOException {
        return (new ObjectMapper()).writeValueAsString(this);
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("id="+id + ",");
        builder.append("name="+name + ",");
        builder.append("created=" + created.getTimeInMillis() + ",");
        builder.append("schemaStr=" + schemaStr);
        return builder.toString();
    }

    public Object[] toParams() {
        Object[] params = new java.lang.Object[]{getName(), getId(), getSchemaStr(), getCreated().getTimeInMillis()};
        return params;
    }

    public boolean eqaulsSchema(Schema schema) {
        Schema schema1 = new Schema.Parser().parse(schemaStr);
        return schema1.equals(schema);
    }


}
