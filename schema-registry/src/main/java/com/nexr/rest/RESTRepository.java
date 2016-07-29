package com.nexr.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.nexr.dip.DipException;
import com.nexr.dip.common.Utils;
import com.nexr.jpa.SchemaInfoQueryExceutor;
import com.nexr.schemaregistry.SchemaInfo;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("repo")
public class RESTRepository {

    private static Logger LOG = LoggerFactory.getLogger(RESTRepository.class);

    private SchemaInfoQueryExceutor queryExecutor;

    public RESTRepository() {

    }

    @Inject
    public void setSchemaInfoQueryExceutor(SchemaInfoQueryExceutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @GET
    @Path("/")
    @Produces("application/json")
    public Response healthCheck() {
        return Response.status(200).entity("Hello there, Welcome to schemarepo.").build();
    }

    @GET
    @Path("schema/ids/{id}")
    @Produces("application/json")
    public Response getSchemabyId(@PathParam("id") String id) {
        try {
            long idValue = Long.parseLong(id);
            SchemaInfo schemaInfo = queryExecutor.get(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_BYID, new
                    Object[]{idValue});
            return Response.status(200).entity(schemaInfo.toJson()).build();
        } catch (DipException e) {
            return Response.status(404).entity(Utils.convertErrorObjectToJson(404, "Schema Not Found: id=" + id)).build();
        } catch (Exception e) {
            return Response.status(500).entity(Utils.convertErrorObjectToJson(500, e.getMessage())).build();
        }
    }

    @GET
    @Path("schema/{subject}")
    @Produces("application/json")
    public Response getSchemaByTopic(@PathParam("subject") String topicName) {
        try {
            SchemaInfo schemaInfo = queryExecutor.getListMaxResult1(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_BYTOPICLATEST, new
                    Object[]{topicName});
            if (schemaInfo == null) {
                throw new DipException("Schema Not Found");
            }
            return Response.status(200).entity(schemaInfo.toJson()).build();
        } catch (DipException e) {
            return Response.status(404).entity(Utils.convertErrorObjectToJson(404, "Schema Not Found: topic=" + topicName)).build();
        } catch (Exception e) {
            return Response.status(500).entity(Utils.convertErrorObjectToJson(500, e.getMessage())).build();
        }
    }

    @GET
    @Path("subjects")
    @Produces({"application/json"})
    public Response getSubjectList() {
        try {
            List<SchemaInfo> list = queryExecutor.getList(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_ALL);
            ObjectMapper mapper = new ObjectMapper();
            String jsonStr = mapper.writeValueAsString(list);
            return Response.status(200).entity(jsonStr).build();

        } catch (Exception e) {
            return Response.status(500).entity(Utils.convertErrorObjectToJson(500, e.getMessage())).build();
        }
    }

    @GET
    @Path("subjects/{subject}")
    @Produces("application/json")
    public Response getSchema(@PathParam("subject") String topicName) {
        try {
            List<SchemaInfo> list = queryExecutor.getList(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_BYTOPICALL, new
                    Object[]{topicName});
            ObjectMapper mapper = new ObjectMapper();
            String jsonStr = mapper.writeValueAsString(list);
            return Response.status(200).entity(jsonStr).build();
        } catch (DipException e) {
            return Response.status(404).entity(Utils.convertErrorObjectToJson(404, "Schema Not Found")).build();
        } catch (Exception e) {
            return Response.status(500).entity(Utils.convertErrorObjectToJson(500, e.getMessage())).build();
        }
    }

    @GET
    @Path("subjects/{subject}/ids/{id}")
    @Produces("application/json")
    public Response getSchema(@PathParam("subject") String topicName, @PathParam("id") String id) {
        try {
            long idValue = Long.parseLong(id);
            SchemaInfo schemaInfo = queryExecutor.get(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_BYTOPICANDID, new Object[]{topicName,
                    idValue});
            return Response.status(200).entity(schemaInfo.toJson()).build();
        } catch (DipException e) {
            return Response.status(404).entity(Utils.convertErrorObjectToJson(404, "Schema Not Found: topic=" + topicName + "id="
                    + id)).build();
        } catch (Exception e) {
            return Response.status(500).entity(Utils.convertErrorObjectToJson(500, e.getMessage())).build();
        }
    }

    @POST
    @Path("subjects/{subject}")
    @Produces("application/json")
    public Response registerSchema(@PathParam("subject") String subject, @FormParam("schema") String schema) {
        LOG.debug("registerSchema, subject : [{}], schema : [{}]", subject, schema);
        if (subject == null || schema == null) {
            return Response.status(500).entity(Utils.convertErrorObjectToJson(500, "subject, schema can not be null")).build();
        }

        SchemaInfo schemaInfo = null;
        // all schemas of given topic
        try {
            List<SchemaInfo> list = queryExecutor.getList(SchemaInfoQueryExceutor.SchemaInfoQuery.GET_BYTOPICALL, new
                    Object[]{subject});
            for (SchemaInfo schemaInfo1 : list) {
                if (schemaInfo1.eqaulsSchema(new Schema.Parser().parse(schema))) {
                    schemaInfo = schemaInfo1;
                }
            }
        } catch (DipException e) {
            LOG.warn("Fail to get all the schema of topic {}", subject);
            return Response.status(500).entity(Utils.convertErrorObjectToJson(500, "Fail to get all the schema of topic " +
                    subject + ", " + e.getMessage())).build();
        }

        try {
            if (schemaInfo == null) {
                schemaInfo = new SchemaInfo(subject, schema);
                Integer obj = (Integer) queryExecutor.insertR(schemaInfo);
                LOG.debug("new registered id :" + obj + " / " + subject);
                schemaInfo.setId(obj.intValue());
            } else {
                LOG.debug("already exist : " + schemaInfo.getId() + " / " + subject);
            }
            return Response.status(200).entity(String.valueOf(schemaInfo.getId())).build();
        } catch (Exception e) {
            return Response.status(500).entity(Utils.convertErrorObjectToJson(500, e.getMessage())).build();
        }

    }
}
