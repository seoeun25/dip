package com.nexr.schemaregistry;

import com.linkedin.camus.schemaregistry.SchemaDetails;
import com.linkedin.camus.schemaregistry.SchemaNotFoundException;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import com.nexr.client.DipSchemaRepoClient;
import org.apache.avro.Schema;

import java.util.Properties;

public class AvroSchemaRegistry implements SchemaRegistry<Schema> {

    public static final String ETL_SCHEMA_REGISTRY_URL = "etl.schema.registry.url";
    private DipSchemaRepoClient client;

    public AvroSchemaRegistry() {

    }

    public AvroSchemaRegistry(String url) {
        Properties properties = new Properties();
        properties.put(ETL_SCHEMA_REGISTRY_URL, url);
        init(properties);
    }

    @Override
    public void init(Properties properties) {
        destroy();
        client = new DipSchemaRepoClient(properties.getProperty(ETL_SCHEMA_REGISTRY_URL));
    }

    @Override
    public String register(String topicName, Schema schema) {
        // TODO validation and throw SchemaViolationException
        return client.register(topicName, schema.toString());
    }

    public String register(String topicName, String schemaStr) {
        Schema schema = new Schema.Parser().parse(schemaStr);
        return register(topicName, schema);
    }

    @Override
    public Schema getSchemaByID(String topicName, String id) {
        SchemaInfo schemaInfo = client.getSchemaBySubjectAndId(topicName, id);
        if (schemaInfo == null) {
            throw new SchemaNotFoundException("Schema not found for [" + topicName + "], id[" + id + "]");
        }
        return Schema.parse(schemaInfo.getSchemaStr());
    }

    @Override
    public SchemaDetails<Schema> getLatestSchemaByTopic(String topicName) {
        SchemaInfo schemaInfo = client.getSchemaBySubject(topicName);
        if (schemaInfo == null) {
            throw new SchemaNotFoundException("Schema not found for [" + topicName + "]");
        }
        return new SchemaDetails<Schema>(topicName, String.valueOf(schemaInfo.getId()), Schema.parse(schemaInfo.getSchemaStr()));
    }

    public void destroy() {
        if (client != null) {
            client.destroy();
            client = null;
        }
    }

}
