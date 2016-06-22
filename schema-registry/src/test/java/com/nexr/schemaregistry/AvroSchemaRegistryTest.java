package com.nexr.schemaregistry;

import com.linkedin.camus.schemaregistry.SchemaDetails;
import com.linkedin.camus.schemaregistry.SchemaNotFoundException;
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

    private static DipSchemaRepoServer server;

    @BeforeClass
    public static void setupClass() {
        startServer();

        schemaRegistry = new AvroSchemaRegistry();
        Properties properties = new Properties();
        properties.put(AvroSchemaRegistry.ETL_SCHEMA_REGISTRY_URL, "http://localhost:2828/repo");
        schemaRegistry.init(properties);
    }

    @AfterClass
    public static void tearDown() {
        try {
            schemaRegistry.destroy();
            schemaRegistry = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
        shutdownServer();
    }

    private static void startServer() {
        try {
            Thread t1 = new Thread(new Runnable() {
                @Override
                public void run() {
                    System.setProperty("persistUnit", "repo-test-hsql");
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
            server.getJdbcService().instrument();
            server.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRegisterAndGet() {
        String topic = Schemas.employee;
        try {
            SchemaDetails<Schema> schemaDetails = schemaRegistry.getLatestSchemaByTopic(topic);
            Assert.fail("schemaDetails should be null but " + schemaDetails.getId());
        } catch (SchemaNotFoundException e) {

        }

        String id = schemaRegistry.register(topic, Schemas.employee_schema1);
        System.out.println("id : " + id);
        Assert.assertTrue(Long.parseLong(id) > 0);
        String id2 = schemaRegistry.register(topic, Schemas.employee_schema2);
        // same schema, contains spaces
        Assert.assertEquals(id, id2);


        SchemaInfo schemaInfo = new SchemaInfo(topic, Long.parseLong(id), schemaRegistry.getSchemaByID(topic, id).toString());

        Schema employee3 = new Schema.Parser().parse(Schemas.employee_schema3);
        Assert.assertFalse(schemaInfo.eqaulsSchema(employee3));
        String id3 = schemaRegistry.register(topic, Schemas.employee_schema3);
        System.out.println("id3 : " + id3);
        Assert.assertNotSame(id3, id);


        // different nullable
        Schema employee4 = new Schema.Parser().parse(Schemas.employee_schema4);
        Assert.assertFalse(schemaInfo.eqaulsSchema(employee4));
        String id4 = schemaRegistry.register(topic, Schemas.employee_schema4);
        System.out.println("id4 : " + id4);
        Assert.assertNotSame(id3, id4);

        schemaRegistry.getLatestSchemaByTopic(topic);

        //lastSchema
        Assert.assertEquals(id4, schemaRegistry.getLatestSchemaByTopic(topic).getId());

        // get by topic, id. regardless of latest one
        Assert.assertNotNull(schemaRegistry.getSchemaByID(topic, id3));

        schemaRegistry.register(Schemas.ftth_if, Schemas.ftth_if_schema);

        //lastSchema again
        Assert.assertEquals(id4, schemaRegistry.getLatestSchemaByTopic(topic).getId());

    }

    @Test
    public void testGetByTopicNegative() {

        try {
            SchemaDetails schemaInfo1 = schemaRegistry.getLatestSchemaByTopic("not-exist");
            Assert.fail("schemaInfo should be null");
        } catch (SchemaNotFoundException e) {
            //e.printStackTrace();
        }
    }


}
