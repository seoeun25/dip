package com.nexr.schemaregistry;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Created by seoeun on 7/20/16.
 */
public class RegisterSchemaRequest {
    private String schema;

    public RegisterSchemaRequest() {
    }

    public static RegisterSchemaRequest fromJson(String json) throws IOException {
        return (RegisterSchemaRequest)(new ObjectMapper()).readValue(json, RegisterSchemaRequest.class);
    }

    @JsonProperty("schema")
    public String getSchema() {
        return this.schema;
    }

    @JsonProperty("schema")
    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String toJson() throws IOException {
        return (new ObjectMapper()).writeValueAsString(this);
    }


}
