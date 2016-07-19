package com.nexr.dip.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class ErrorObject {
    private int status;
    private String message;

    public ErrorObject(@JsonProperty("status") int status, @JsonProperty("message") String message) {
        this.status = status;
        this.message = message;
    }

    @JsonProperty("status")
    public int getStatus() {
        return this.status;
    }

    @JsonProperty("status")
    public void setStatus(int status) {
        this.status = status;
    }

    @JsonProperty
    public String getMessage() {
        return this.message;
    }

    @JsonProperty
    public void setMessage(String message) {
        this.message = message;
    }

    public String toJson() throws IOException {
        return (new ObjectMapper()).writeValueAsString(this);
    }

    public String toString() {
        return status + " " + message;
    }
}
