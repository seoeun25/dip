package com.nexr.client;

import com.nexr.schemaregistry.SchemaInfo;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.representation.Form;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class DipSchemaRepoClient {

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
    public String register(String topicName, String schema) {
        Form form = new Form();
        form.add("subject", topicName);
        form.add("schema", schema);

        ClientResponse response = client.resource(url).path("subjects/" + topicName)
                .type("application/json").post(ClientResponse.class, form);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            String id = response.getEntity(String.class);
            return id;
        } else {
            response.close();
            return null;
        }
    }

    /**
     * Get all the schema.
     *
     * @return list<SchemaInfo>
     */
    public List<SchemaInfo> getSchemaLatestAll() {
        ClientResponse response = client.resource(url).path("subjects/")
                .type(MediaType.TEXT_PLAIN_TYPE).get(ClientResponse.class);

        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            List<SchemaInfo> schemaInfos = new ArrayList<SchemaInfo>();
            String[] subjects = response.getEntity(String.class).split("\n");
            for (String subject : subjects) {
                JSONObject jsonObject = (JSONObject) JSONValue.parse(subject);
                SchemaInfo schemaInfo = new SchemaInfo();
                schemaInfo.setId(Long.parseLong(jsonObject.get("id").toString()));
                schemaInfo.setName(jsonObject.get("name").toString());
                schemaInfo.setSchemaStr(jsonObject.get("schemaStr").toString());
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(Long.parseLong(jsonObject.get("created").toString()));
                schemaInfo.setCreated(calendar);
                schemaInfos.add(schemaInfo);
            }
            return schemaInfos;
        } else {
            response.close();
            return null;
        }
    }

    /**
     * Gets the schema by subject.
     *
     * @param subject the subject name
     * @return schemaInfo
     */
    public SchemaInfo getSchemaBySubject(String subject) {
        ClientResponse response = client.resource(url).path("subjects/" + subject)
                .accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            SchemaInfo schemaInfo = response.getEntity(SchemaInfo.class);
            return schemaInfo;
        } else {
            response.close();
            return null;
        }

    }

    /**
     * Gets the schema by subject and id
     *
     * @param subject the subject name
     * @param id      the subject id
     * @return schemaInfo
     */
    public SchemaInfo getSchemaBySubjectAndId(String subject, String id) {
        ClientResponse response = client.resource(url).path("subjects/" + subject + "/ids/" + id)
                .accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);

        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            SchemaInfo schemaInfo = response.getEntity(SchemaInfo.class);
            return schemaInfo;
        } else {
            response.close();
            return null;
        }
    }

    /**
     * Gets ths schema by id
     *
     * @param id
     * @return schemaInfo
     */
    public SchemaInfo getSchemaById(String id) {
        ClientResponse response = client.resource(url).path("schema/ids/" + id)
                .accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);

        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            SchemaInfo schemaInfo = response.getEntity(SchemaInfo.class);
            return schemaInfo;
        } else {
            response.close();
            return null;
        }
    }

}
