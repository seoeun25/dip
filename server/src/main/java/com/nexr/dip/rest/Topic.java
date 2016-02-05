package com.nexr.dip.rest;

import com.nexr.dip.DipLoaderException;
import com.nexr.dip.jpa.DipPropertyQueryExecutor;
import com.nexr.dip.loader.Loader;
import com.nexr.dip.loader.ScheduledService;
import com.nexr.dip.loader.TopicManager;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

@Path("/v1")
public class Topic {

    private static Logger LOG = LoggerFactory.getLogger(Topic.class);

    private DipPropertyQueryExecutor dipPropsQueryExecutor;

    private ScheduledService scheduledService;

    public Topic() {
    }

    @GET
    @Path("admin")
    @Produces("application/json")
    public Response admin() {
        return Response.status(200).entity("Hello there, It's a beautiful day!").build();
    }

    @GET
    @Path("topic")
    @Produces("application/json")
    public Response getTopic() {

        try {
            JSONObject jsonObject = ScheduledService.getInstance().toJsonObject();
            return Response.status(200).entity(jsonObject.toJSONString()).build();
        } catch (Exception e) {
            return Response.status(500).entity(e.getMessage()).build();
        }

    }

    @GET
    @Path("topic/{topic}")
    @Produces("application/json")
    public Response getTopicOf(@PathParam("topic") String topicName) {
        try {
            JSONObject jsonObject = ScheduledService.getInstance().toJsonObject(topicName);
            if (jsonObject == null) {
                throw new IllegalArgumentException("Topic not found : " + topicName);
            }
            return Response.status(200).entity((jsonObject.toJSONString())).build();
        } catch (Exception e) {
            return Response.status(500).entity(e.getMessage()).build();
        }
    }

    @PUT
    @Path("topic/{topic}")
    public Response manageTopic(@PathParam("topic") String topic, @FormParam("action") String action) {
        LOG.info(" topic scheduler : " + topic + ", " + action);

        TopicManager.STATUS status = null;
        try {
            if (action == null) {
                throw new DipLoaderException("Illegal action : " + action);
            }
            if (action.equals("start")) {
                status = ScheduledService.getInstance().restartTopicManager(topic);
            } else if (action.equals("end")) {
                status = ScheduledService.getInstance().closeTopicManager(topic);
            } else {
                throw new DipLoaderException("Illegal action : " + action);
            }
            return Response.status(200).entity("PUT : " + topic + " : " + action + " >>> " + status + "\n").build();
        } catch (DipLoaderException e) {
            return Response.status(500).entity(e.getMessage()).build();
        }
    }

}

