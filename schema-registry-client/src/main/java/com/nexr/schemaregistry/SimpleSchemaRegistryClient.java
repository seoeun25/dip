package com.nexr.schemaregistry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleSchemaRegistryClient implements SchemaRegistryClient {

    private static Logger log = LoggerFactory.getLogger(SimpleSchemaRegistryClient.class);

    private static TypeReference<String> ID_TYPE = new TypeReference<String>() {
    };
    private static TypeReference<SchemaInfo> SCHEMAINFO_TYPE = new TypeReference<SchemaInfo>() {
    };
    private static TypeReference<List<SchemaInfo>> LIST_SCHMEAINFO_TYPE = new TypeReference<List<SchemaInfo>>() {
    };

    private static ObjectMapper jsonDeserializer = new ObjectMapper();

    private final String baseUrl;

    public SimpleSchemaRegistryClient(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    @Override
    public String register(String topic, String schema) throws IOException, SchemaClientException {
        String path = String.format("/subjects/%s", new Object[]{topic});
        String jsonStrring = "schema=" + schema;

        Map<String, String> param = new HashMap<>();
        param.put("schema", schema);
        String id = httpRequest(path, "POST", jsonStrring.getBytes(), param, ID_TYPE);
        return id;
    }

    @Override
    public SchemaInfo getSchemaByTopicAndId(String topic, String id) throws IOException, SchemaClientException {
        String path = String.format("/subjects/%s/ids/%s", new Object[]{topic, id});
        SchemaInfo schemaInfo = httpRequest(path, "GET", null, new HashMap<String, String>(), SCHEMAINFO_TYPE);
        return schemaInfo;
    }

    @Override
    public SchemaInfo getLatestSchemaByTopic(String topic) throws IOException, SchemaClientException {
        String path = String.format("/schema/%s", new Object[]{topic});
        SchemaInfo schemaInfo = httpRequest(path, "GET", null, new HashMap<String, String>(), SCHEMAINFO_TYPE);
        return schemaInfo;
    }

    @Override
    public List<SchemaInfo> getSchemaAllByTopic(String topic) throws IOException, SchemaClientException {
        String path = String.format("/subjects/%s", new Object[]{topic});
        List<SchemaInfo> schemaInfos = httpRequest(path, "GET", null, new HashMap<String, String>(), LIST_SCHMEAINFO_TYPE);
        return schemaInfos;
    }

    @Override
    public List<SchemaInfo> getLatestSchemaAll() throws IOException, SchemaClientException {
        String path = String.format("/subjects/");
        List<SchemaInfo> schemaInfos = httpRequest(path, "GET", null, new HashMap<String, String>(), LIST_SCHMEAINFO_TYPE);
        return schemaInfos;
    }

    private <T> T httpRequest(String path, String method,
                              byte[] requestBodyData, Map<String, String> requestProperties,
                              TypeReference<T> responseFormat) throws IOException, SchemaClientException {
        for (int i = 0, n = 3; i < n; i++) {
            try {
                return sendHttpRequest(baseUrl + path, method, requestBodyData, requestProperties, responseFormat);
            } catch (IOException e) {
                log.warn("Fail to sendHttpRequest {}, try {}", baseUrl, n);
                if (i == n - 1) throw e; // Raise the exception since we have no more urls to try
            }
        }
        throw new IOException("Internal HTTP retry error"); // Can't get here
    }

    private <T> T sendHttpRequest(String baseUrl, String method, byte[] requestBodyData,
                                  Map<String, String> requestProperties,
                                  TypeReference<T> responseFormat)
            throws IOException, SchemaClientException {
        log.info(String.format("Sending %s with input %s to %s",
                method, requestBodyData == null ? "null" : new String(requestBodyData),
                baseUrl));

        HttpURLConnection connection = null;
        try {
            URL url = new URL(baseUrl);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod(method);

            // connection.getResponseCode() implicitly calls getInputStream, so always set to true.
            // On the other hand, leaving this out breaks nothing.
            connection.setDoInput(true);

            for (Map.Entry<String, String> entry : requestProperties.entrySet()) {
                connection.setRequestProperty(entry.getKey(), entry.getValue());
            }

            connection.setUseCaches(false);
            if (requestBodyData != null) {
                connection.setDoOutput(true);
                OutputStream os = null;
                try {
                    os = connection.getOutputStream();
                    os.write(requestBodyData);
                    os.flush();
                } catch (IOException e) {
                    log.error("Failed to send HTTP request to endpoint: " + url, e);
                    throw e;
                } finally {
                    if (os != null) os.close();
                }
            }

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                InputStream is = connection.getInputStream();
                T result = jsonDeserializer.readValue(is, responseFormat);
                is.close();
                return result;
            } else if (responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
                return null;
            } else {
                InputStream es = connection.getErrorStream();
                ErrorStatus errorObj;
                try {
                    errorObj = jsonDeserializer.readValue(es, ErrorStatus.class);
                } catch (JsonProcessingException e) {
                    errorObj = new ErrorStatus(500, e.getMessage());
                }
                es.close();
                throw new SchemaClientException(String.format("[%s, %s]", new Object[]{errorObj.getStatus(), errorObj.getMessage()}));
            }

        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }


}
