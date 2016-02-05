package com.nexr.schemaregistry;

import com.linkedin.camus.schemaregistry.SchemaDetails;
import com.nexr.Schemas;
import com.nexr.server.DipSchemaRepoServer;
import junit.framework.Assert;
import org.apache.avro.Schema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

public class AvroSchemaRegistryTest {

    private static AvroSchemaRegistry schemaRegistry;

    private static String id;

    @BeforeClass
    public static void setupClass() {
        startServer();

        schemaRegistry = new AvroSchemaRegistry();
        Properties properties = new Properties();
        properties.put("etl.schema.registry.url","http://localhost:18181/schemarepo");
        schemaRegistry.init(properties);

        initData();
    }

    @AfterClass
    public static void tearDown() {
        try {
            schemaRegistry = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
        shutdownServer();
    }
    private static DipSchemaRepoServer server;
    private static void startServer() {
        try {
            Thread t1 = new Thread(new Runnable() {
                @Override
                public void run() {
//                    DipSchemaRepoContext.getContext().setConfig("schemarepo." + JDBCService.CONF_URL, "jdbc:derby:memory:myDB;" +
//                            "create=true");
//                    DipSchemaRepoContext.getContext().setConfig("schemarepo." + JDBCService.CONF_DRIVER, "org.apache.derby.jdbc" +
//                            ".EmbeddedDriver");
                    server = DipSchemaRepoServer.getInstance();
                    try {
                        server.start();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            });
            t1.start();

            Thread.sleep(7000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void shutdownServer() {
        try {
            server.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void initData() {
        try {
            String topicName = Schemas.gpx_port;
            Schema schema = new Schema.Parser().parse(Schemas.gpx_port_schema);
            schemaRegistry.register(topicName, schema);
            Thread.sleep(2000);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testRegister() {

        String topicName = Schemas.gpx_port;
        Schema schema = new Schema.Parser().parse(Schemas.gpx_port_schema);
        id = schemaRegistry.register(topicName, schema);
        System.out.println("id : " + id);

        String id2 = schemaRegistry.register(topicName, schema);
        Assert.assertEquals(id, id2);
    }

    @Test
    public void testGet() {
        SchemaDetails<Schema> schemaDetails = schemaRegistry.getLatestSchemaByTopic(Schemas.gpx_port);
        Schema schema = schemaDetails.getSchema();
        String schmeaStr = schema.toString();

        Schema schema2 = schemaRegistry.getSchemaByID(Schemas.gpx_port, id);
        Assert.assertEquals(schema, schema2);


        schemaRegistry.register(Schemas.ftth_if, new Schema.Parser().parse(Schemas.ftth_if_schema));
        String schemaStr = schemaRegistry.getLatestSchemaByTopic(Schemas.ftth_if).getSchema().toString();
        System.out.println("--- \n" + schemaStr);
    }

}
