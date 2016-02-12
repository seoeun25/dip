package com.nexr.camus.schemaregistry;

import com.linkedin.camus.schemaregistry.MemorySchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaDetails;
import com.linkedin.camus.schemaregistry.SchemaNotFoundException;
import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.Map;

public class SimpleSchemaRegistry extends MemorySchemaRegistry<Schema> {


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
            "         \"namespace\": \"header\",\n" +
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

    // topic, id
    private Map<String, String> topicIdMap = new HashMap<String, String>();

    public SimpleSchemaRegistry() {
        super();
        topicIdMap.put(employee, super.register(employee, new org.apache.avro.Schema.Parser().parse(employee_schema)));
        topicIdMap.put(ftthif, super.register(ftthif, new Schema.Parser().parse(ftthif_schema)));
        System.out.println("---- SimpleSchemaRegistry");
    }

    @Override
    public Schema getSchemaByID(String topicName, String idStr) {
        System.out.println("---- getSchemaByID : " + topicName);
        if (topicName.equals(SimpleSchemaRegistry.employee)) {
            return new Schema.Parser().parse(employee_schema);
        } else if (topicName.equals(SimpleSchemaRegistry.ftthif)) {
            return new Schema.Parser().parse(ftthif_schema);
        }else {
            throw new SchemaNotFoundException("Supplied a non-long id string. : " + topicName );
        }
    }

    @Override
    public SchemaDetails<Schema> getLatestSchemaByTopic(String topicName) {
        System.out.println("---- getLatestSchemaByTopic : " + topicName);
        Schema schema = null;
        if (topicName.equals(SimpleSchemaRegistry.employee)) {
            schema = new Schema.Parser().parse(employee_schema);
        } else if (topicName.equals(SimpleSchemaRegistry.ftthif)) {
            schema = new Schema.Parser().parse(ftthif_schema);
        }

        if (schema == null) {
            throw new SchemaNotFoundException();
        }

        return new SchemaDetails<Schema>(topicName, topicIdMap.get(topicName), schema);
    }

    public Schema getSchema(String topicName) {
        return super.getSchemaByID(topicName, topicIdMap.get(topicName));
    }

}
