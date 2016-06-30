package com.nexr.client;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.nexr.Schemas;
import com.nexr.schemaregistry.SchemaInfo;
import com.nexr.server.DipSchemaRepoServer;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class DipSchemaRepoClientTest {

    private static DipSchemaRepoServer server;

    private static DipSchemaRepoClient client;

    @BeforeClass
    public static void setupClass() {
        System.setProperty("persistenceUnit", "repo-test-hsql");
        startServer();
        client = new DipSchemaRepoClient("http://localhost:2828/repo");
    }

    @AfterClass
    public static void tearDown() {
        client.destroy();
        shutdownServer();
    }

    private static void startServer() {
        try {
            Thread t1 = new Thread(new Runnable() {
                @Override
                public void run() {
                    Injector injector = Guice.createInjector(new DipSchemaRepoServer());
                    server = injector.getInstance(DipSchemaRepoServer.class);
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
        String topicName = Schemas.employee;
        long currentTime = System.currentTimeMillis();

        try {
            String id = client.register(topicName, Schemas.employee_schema1);
            Assert.assertNotNull(id);
            System.out.println("registered id : " + id);
            // already exists that schema under the subject
            String id2 = client.register(topicName, Schemas.employee_schema2);
            Assert.assertEquals(id, id2);

            // same schema under the different subject
            String newId = client.register("new-topic-" + currentTime, Schemas.employee_schema2);
            Assert.assertFalse(id.equals(newId));

            String ffttId = client.register(Schemas.ftth_if, Schemas.ftth_if_schema);
            // all subjects
            List<SchemaInfo> schemaInfoList = client.getSchemaLatestAll();
            for (SchemaInfo subject : schemaInfoList) {
                System.out.println("--- schema : " + subject);
            }
            Assert.assertEquals(3, schemaInfoList.size());


            String id3 = client.register(topicName, Schemas.employee_schema3);
            String id4 = client.register(topicName, Schemas.employee_schema4);
            System.out.println("id3 : " + id3);
            System.out.println("id4 : " + id4);

            Thread.sleep(100);
            // get by topic
            Assert.assertEquals(Long.parseLong(id4), client.getSchemaBySubject(topicName).getId());

            // get by subject by id
            Assert.assertNotNull(client.getSchemaBySubjectAndId(topicName, id3));


            Thread.sleep(1000);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetByTopic() {

        try {
            SchemaInfo schemaInfo1 = client.getSchemaBySubject("not-exist");
            Assert.assertNull(schemaInfo1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
