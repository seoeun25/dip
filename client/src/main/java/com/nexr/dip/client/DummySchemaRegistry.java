package com.nexr.dip.client;

import com.linkedin.camus.schemaregistry.SchemaDetails;
import com.linkedin.camus.schemaregistry.SchemaNotFoundException;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Dummy Schema Registry.
 */
public class DummySchemaRegistry implements SchemaRegistry<Schema> {

    public static final String employee = "employee";
    public static final String employee_schema = "{\"namespace\": \"com.nexr.dip.avro.schema\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"employee\",\n" +
            "  \"fields\": [\n" +
            "      {\"name\": \"name\", \"type\": \"string\"},\n" +
            "      {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
            "      {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]},\n" +
            "      {\"name\": \"wrk_dt\", \"type\": \"long\"},\n" +
            "      {\"name\": \"srcinfo\", \"type\": \"string\"},\n" +
            "      {\n" +
            "         \"name\": \"header\",\n" +
            "         \"type\": {\n" +
            "             \"type\" : \"record\",\n" +
            "             \"name\" : \"headertype\",\n" +
            "             \"fields\" : [\n" +
            "                 {\"name\": \"time\", \"type\": \"long\"}\n" +
            "             ]\n" +
            "         }\n" +
            "      }\n" +
            "  ]\n" +
            " }";

    public static final String ftthif = "ftthif";
    public static final String ftthif_schema = "{\"namespace\": \"com.nexr.dip.avro.schema\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"ftthif\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"neosscode\", \"type\": \"string\"},\n" +
            "     {\"name\": \"ifname\",  \"type\": \"string\"},\n" +
            "     {\"name\": \"onuid\", \"type\": \"string\"},\n" +
            "     {\"name\": \"ontmac\", \"type\": \"string\"},\n" +
            "     {\"name\": \"extrainfo\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"wrk_dt\", \"type\": \"long\"},\n" +
            "     {\n" +
            "          \"name\": \"header\",\n" +
            "          \"type\": {\n" +
            "              \"type\" : \"record\",\n" +
            "              \"name\" : \"headertype\",\n" +
            "              \"fields\" : [\n" +
            "                  {\"name\": \"time\", \"type\": \"long\"}\n" +
            "              ]\n" +
            "          }\n" +
            "     }\n" +
            " ]\n" +
            "}";


    private Map<String, Schema> schemaMap = new HashMap<String, Schema>();

    @Override
    public void init(Properties properties) {
        //To change body of implemented methods use File | Settings | File Templates.
        register(employee, Schema.parse(employee_schema));
        register(ftthif, Schema.parse(ftthif_schema));
    }

    @Override
    public String register(String topic, Schema schema) {
        String id = String.valueOf(schemaMap.size() + 1);
        schemaMap.put(id, schema);
        return id;
    }

    @Override
    public Schema getSchemaByID(String topic, String id) {
        Schema schema = schemaMap.get(id);
        if (schema == null) {
            throw new SchemaNotFoundException(topic + ", " + id);
        }
        if (schema.getName().equalsIgnoreCase(topic)) {
            return schema;
        } else {
            throw new SchemaNotFoundException(topic + ", " + id);
        }
    }

    @Override
    public SchemaDetails<Schema> getLatestSchemaByTopic(String topic) {
        for (Map.Entry<String, Schema> entry : schemaMap.entrySet()) {
            Schema schema = entry.getValue();
            if (schema.getName().equalsIgnoreCase(topic)) {
                return new SchemaDetails<Schema>(topic, entry.getKey(), schema);
            }
        }
        throw new SchemaNotFoundException(topic);
    }

}
