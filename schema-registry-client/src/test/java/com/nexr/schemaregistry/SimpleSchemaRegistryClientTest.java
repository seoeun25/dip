package com.nexr.schemaregistry;

import junit.framework.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class SimpleSchemaRegistryClientTest {

    private static final String BASE_URL = "http://localhost:18181/repo";

    public static final String test_topic = "{\"namespace\": \"example.avro\", \"type\": \"record\",\"name\": \"User\", " +
            "\"fields\": " +
            "[{\"name\": \"name\", \"type\": \"string\"},{\"name\": \"favorite_number\", \"type\": [\"int\", \"null\"]},{\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]} ]}";


    @Test
    public void test() {

        SimpleSchemaRegistryClient client = new SimpleSchemaRegistryClient(BASE_URL);

        String topic = "employee";

        try {
            SchemaInfo schemaInfo = client.getSchemaBySubject(topic);
            Assert.assertNotNull(schemaInfo);
            long id1 = schemaInfo.getId();

            SchemaInfo schemaInfo2 = client.getSchemaBySubjectAndId(topic, String.valueOf(id1));
            Assert.assertNotNull(schemaInfo2);

            Assert.assertEquals(id1, schemaInfo2.getId());

            long start = System.currentTimeMillis();
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(1000, TimeUnit.MILLISECONDS);
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    countDownLatch.countDown();

                }
            }) ;
            //t.start();
            //countDownLatch.await();
            long end = System.currentTimeMillis();
            System.out.println(String.valueOf(end - start));

            try {
                SchemaInfo schemaInfo3 = client.getSchemaBySubjectAndId(topic, String.valueOf(3445677));
                Assert.fail();
            } catch (SchemaClientException e) {
                Assert.assertTrue(e.getMessage().contains("Schema Not Found"));
            }

            try {
                SchemaInfo schemaInfo4 = client.getSchemaBySubject("haha-test");
                Assert.fail();
            } catch (SchemaClientException e) {
                Assert.assertTrue(e.getMessage().contains("Schema Not Found"));
            }

            List<SchemaInfo> list = client.getSchemaLatestAll();
            for (SchemaInfo schemaInfo1: list) {
                System.out.println(schemaInfo1.toJson());
            }

            String id = client.register("test-topic", test_topic);
            Assert.assertNotNull(id);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testBlockingQueue() throws InterruptedException {
        ArrayBlockingQueue<String> blockingQueue = new ArrayBlockingQueue<String>(2) ;
        blockingQueue.offer("String-1", 1000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(true, blockingQueue.offer("222"));
        Assert.assertEquals(false, blockingQueue.offer("333"));
        Assert.assertEquals(false, blockingQueue.offer("444", 1000, TimeUnit.MILLISECONDS));
        long start = System.currentTimeMillis();
        Assert.assertEquals("String-1", blockingQueue.poll());
        long end = System.currentTimeMillis();
        System.out.println("diff : " + String.valueOf(end-start));

        Thread.sleep(2000);
        System.out.println(blockingQueue.toString());
        Assert.assertEquals(1, blockingQueue.size());
    }

}
