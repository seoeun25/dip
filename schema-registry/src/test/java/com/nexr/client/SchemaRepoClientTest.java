package com.nexr.client;

import com.nexr.Schemas;
import com.nexr.schemaregistry.SchemaInfo;
import com.nexr.server.DipSchemaRepoServer;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class SchemaRepoClientTest {

    private static DipSchemaRepoServer server;

    private static DipSchemaRepoClient client;

    private static String ftthifId;

    @BeforeClass
    public static void setupClass() {
        startServer();
        client = new DipSchemaRepoClient("http://localhost:18181/schemarepo");
        initData();
    }

    @AfterClass
    public static void tearDown() {
        shutdownServer();
    }

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

            Thread.sleep(10000);
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
            ftthifId = client.register(Schemas.ftth_if, Schemas.ftth_if_schema);
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRegister() {
        String topicName = "hello-seoeun";
        long currentTime = System.currentTimeMillis();
        String schemaStr = "{\"namespace\" : \"" + currentTime + "-latest\"}";

        try {
            String id = client.register(topicName, schemaStr);
            Assert.assertNotNull(id);
            System.out.println("registered id : " + id);
            // already exists that schema under the subject
            String id2 = client.register(topicName, schemaStr);
            Assert.assertEquals(id, id2);

            // same schema under the different subject
            String newId = client.register("new-topic-" + currentTime, schemaStr);
            Assert.assertFalse(id.equals(newId));

            client.register(Schemas.ftth_if, "test-schema");
            ftthifId = client.register(Schemas.ftth_if, Schemas.ftth_if_schema);
            Assert.assertNotNull(ftthifId);

            Thread.sleep(1000);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSubjects() {
        List<SchemaInfo> schemaInfoList = client.getSchemaLatestAll();
        for (SchemaInfo subject : schemaInfoList) {
            System.out.println("--- schema : " + subject);
        }
    }

    @Test
    public void testGetByTopic() {
        client.register(Schemas.ftth_if, Schemas.ftth_if_schema);
        SchemaInfo schemaInfo = client.getSchemaBySubject(Schemas.ftth_if);
        Assert.assertEquals("ftth_if", schemaInfo.getName());
        Assert.assertEquals(Schemas.ftth_if_schema, schemaInfo.getSchemaStr());

        try {
            SchemaInfo schemaInfo1 = client.getSchemaBySubject("not-exist");
            Assert.assertNull(schemaInfo1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetByTopicAndId() {
        SchemaInfo schemaInfo = client.getSchemaBySubjectAndId(Schemas.ftth_if, ftthifId);
        Assert.assertEquals("ftth_if", schemaInfo.getName());
        Assert.assertEquals(ftthifId, String.valueOf(schemaInfo.getId()));

    }

    @Test
    public void testGetByID() {
        SchemaInfo schemaInfo = client.getSchemaById(ftthifId);
        Assert.assertEquals(ftthifId, String.valueOf(schemaInfo.getId()));
        Assert.assertEquals(Schemas.ftth_if, schemaInfo.getName());

        try {
            SchemaInfo schemaInfo1 = client.getSchemaById("1000");
            Assert.assertNull(schemaInfo1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
