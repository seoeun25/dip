package com.nexr.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexr.dip.common.ErrorObject;
import com.nexr.schemaregistry.SchemaClientException;
import com.nexr.schemaregistry.SchemaInfo;
import com.nexr.schemaregistry.SchemaRegistryClient;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.representation.Form;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;

public class DipSchemaRepoClient implements SchemaRegistryClient {

    private static Logger log = LoggerFactory.getLogger(DipSchemaRepoClient.class);

    private String url;

    private Client client;

    public DipSchemaRepoClient(String url) {
        this.url = url;
        this.client = Client.create();
    }

    /**
     * Register the schema under the subject.
     *
     * @param topicName the name of subject
     * @param schema    the avro schema
     * @return the id of the registered schema. <code>null</code> if failed.
     */
    public String register(String topicName, String schema) throws IOException, SchemaClientException{
        Form form = new Form();
        form.add("subject", topicName);
        form.add("schema", schema);

        ClientResponse response = client.resource(url).path("subjects/" + topicName)
                .type("application/json").post(ClientResponse.class, form);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            String id = response.getEntity(String.class);
            return id;
        } else {
            ErrorObject errorObject = new ErrorObject(response.getStatus(), printError(response));
            response.close();
            throw new SchemaClientException(errorObject.toString());
        }
    }

    public List<SchemaInfo> getLatestSchemaAll() throws IOException, SchemaClientException{
        ClientResponse response = client.resource(url).path("subjects/").type(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse
                .class);

        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            ObjectMapper objectMapper = new ObjectMapper();
            List<SchemaInfo> schemaInfos = new ArrayList<>();
            try {
                schemaInfos.addAll(objectMapper.<Collection<? extends SchemaInfo>>readValue(response.getEntityInputStream(),
                        objectMapper.getTypeFactory().constructCollectionType(List.class, SchemaInfo.class))) ;
            } catch (IOException e) {
                log.warn("Fail to convert SchemaInfo from jsonString", e);
            }
            return schemaInfos;
        } else {
            ErrorObject errorObject = new ErrorObject(response.getStatus(), printError(response));
            response.close();
            throw new SchemaClientException(errorObject.toString());
        }
    }

    public SchemaInfo getLatestSchemaByTopic(String topic) throws IOException, SchemaClientException{
        ClientResponse response = client.resource(url).path("schema/" + topic)
                .accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            SchemaInfo schemaInfo = response.getEntity(SchemaInfo.class);
            return schemaInfo;
        } else {
            ErrorObject errorObject = new ErrorObject(response.getStatus(), printError(response));
            response.close();
            throw new SchemaClientException(errorObject.toString());
        }
    }

    public List<SchemaInfo> getSchemaAllByTopic(String topic) throws IOException, SchemaClientException{
        ClientResponse response = client.resource(url).path("subjects/" + topic).type(MediaType.APPLICATION_JSON_TYPE).get
                (ClientResponse
                .class);

        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            ObjectMapper objectMapper = new ObjectMapper();
            List<SchemaInfo> schemaInfos = new ArrayList<>();
            try {
                schemaInfos.addAll(objectMapper.<Collection<? extends SchemaInfo>>readValue(response.getEntityInputStream(),
                        objectMapper.getTypeFactory().constructCollectionType(List.class, SchemaInfo.class))) ;
            } catch (IOException e) {
                log.warn("Fail to convert SchemaInfo from jsonString", e);
            }
            return schemaInfos;
        } else {
            ErrorObject errorObject = new ErrorObject(response.getStatus(), printError(response));
            response.close();
            throw new SchemaClientException(errorObject.toString());
        }
    }

    public SchemaInfo getSchemaByTopicAndId(String topic, String id) throws IOException, SchemaClientException{
        ClientResponse response = client.resource(url).path("subjects/" + topic + "/ids/" + id)
                .accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);

        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            SchemaInfo schemaInfo = response.getEntity(SchemaInfo.class);
            return schemaInfo;
        } else {
            ErrorObject errorObject = new ErrorObject(response.getStatus(), printError(response));
            response.close();
            throw new SchemaClientException(errorObject.toString());
        }
    }

    /**
     * Gets ths schema by id
     *
     * @param id
     * @return schemaInfo
     */
    public SchemaInfo getSchemaById(String id) throws Exception{
        ClientResponse response = client.resource(url).path("schema/ids/" + id)
                .accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);

        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            SchemaInfo schemaInfo = response.getEntity(SchemaInfo.class);
            return schemaInfo;
        } else {
            ErrorObject errorObject = new ErrorObject(response.getStatus(), printError(response));
            response.close();
            throw new Exception(errorObject.toString());
        }
    }

    public void destroy() {
        if (client != null) {
            client.destroy();
        }
    }

    public String printError(ClientResponse clientResponse) {
        clientResponse.bufferEntity();
        String errorMessage = clientResponse.toString();
        if (clientResponse.hasEntity()) {
            errorMessage = errorMessage + " " + clientResponse.getEntity(String.class);
        }
        log.info(errorMessage);
        return errorMessage;
    }

}
