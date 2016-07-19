package com.nexr.schemaregistry;

import java.io.IOException;
import java.util.List;

public interface SchemaRegistryClient {
    String register(String topic, String schema) throws IOException, SchemaClientException;

    SchemaInfo getSchemaById(String topic, String id) throws IOException, SchemaClientException;

    SchemaInfo getLatestSchemaByTopic(String topic) throws IOException, SchemaClientException;

    List<SchemaInfo> getLatestSchemaAll() throws IOException, SchemaClientException;
}
