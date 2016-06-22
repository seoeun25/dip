package com.nexr.schemaregistry;

import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.Schema;
import org.json.simple.JSONObject;

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
import java.util.Calendar;

@Entity
@NamedQueries({

        @NamedQuery(name = "GET_BYSCHEMA", query = "select a.name, a.id, a.schemaStr, a.created " +
                "from SchemaInfo a where a.schemaStr = :schemaStr "),
        @NamedQuery(name = "GET_BYID", query = "select a.name, a.id, a.schemaStr, a.created " +
                "from SchemaInfo a where a.id = :id "),
        @NamedQuery(name = "GET_BYTOPICLATEST", query = "select a.name, a.id, a.schemaStr, a.created " +
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
    private long id;

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

    public SchemaInfo(String name, long id, String schemaStr) {
        this.name = name;
        this.id = id;
        this.schemaStr = schemaStr;
        this.created = Calendar.getInstance();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSchemaStr() {
        return schemaStr;
    }

    public void setSchemaStr(String schemaStr) {
        this.schemaStr = schemaStr;
    }

    public Calendar getCreated() {
        return created;
    }

    public void setCreated(Calendar created) {
        this.created = created;
    }

    public JSONObject toJsonOblect() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", name);
        jsonObject.put("id", id);
        jsonObject.put("schemaStr", schemaStr);
        jsonObject.put("created", created.getTimeInMillis());
        return jsonObject;
    }

    public String toString() {
        return toJsonOblect().toString();
    }

    public Object[] toParams() {
        Object[] params = new java.lang.Object[]{getName(), getId(), getSchemaStr(), getCreated().getTimeInMillis()};
        return params;
    }

    @VisibleForTesting
    public boolean eqaulsSchema(Schema schema) {
        Schema schema1 = new Schema.Parser().parse(schemaStr);
        return schema1.equals(schema);
    }


}
