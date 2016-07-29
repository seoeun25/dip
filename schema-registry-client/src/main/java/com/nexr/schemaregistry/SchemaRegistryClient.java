package com.nexr.schemaregistry;

import java.io.IOException;
import java.util.List;

public interface SchemaRegistryClient {
    String register(String topic, String schema) throws IOException, SchemaClientException;

    /**
     * Gets the schema by topic and id
     * @param topic
     * @param id
     * @return
     * @throws IOException
     * @throws SchemaClientException
     */
    SchemaInfo getSchemaByTopicAndId(String topic, String id) throws IOException, SchemaClientException;

    /**
     * Gets the latest schema by topic
     * @param topic
     * @return
     * @throws IOException
     * @throws SchemaClientException
     */
    SchemaInfo getLatestSchemaByTopic(String topic) throws IOException, SchemaClientException;

    /**
     * Gets the all schemas by topic
     * @param topic
     * @return
     * @throws IOException
     * @throws SchemaClientException
     */
    List<SchemaInfo> getSchemaAllByTopic(String topic) throws IOException, SchemaClientException;

    /**
     * Gets the all latest schemas
     * @return
     * @throws IOException
     * @throws SchemaClientException
     */
    List<SchemaInfo> getLatestSchemaAll() throws IOException, SchemaClientException;
}
