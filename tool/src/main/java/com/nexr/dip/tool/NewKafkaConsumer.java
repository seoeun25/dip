package com.nexr.dip.tool;

import com.google.common.annotations.VisibleForTesting;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.consumer.Consumer;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * SimpleKafkaConsumer consume the message from kafka cluster.
 */
public class NewKafkaConsumer {

    public static int timeout = 60000;
    public static int bufferSize = 64 * 1024;

    private String groupId;
    private List<String> topics;
    private ExecutorService executorService;
    private List<String> brokers = new ArrayList<String>();
    private final List<ConsumerLoop> consumers;

    public NewKafkaConsumer(int numConsumers, String groupId, List<String> topics) {
        this.groupId = groupId;
        this.topics = topics;
        executorService = Executors.newFixedThreadPool(numConsumers);
        brokers = new ArrayList<String>();
        consumers = new ArrayList<>();
        for (int i=0; i <numConsumers; i++) {
            ConsumerLoop consumer = new ConsumerLoop(i, groupId, topics);
            consumers.add(consumer);
            executorService.submit(consumer);
        }

    }

    public void shutdown() {
        System.out.println("-- shutdown");
        for (ConsumerLoop consumer: consumers) {
            consumer.shutdown();
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static void main(String args[]) {
        System.out.println("-- Application start");
        int numConsumers = 3;
        String groupId = "camus-test";
        List<String> topoics = Arrays.asList("employee");

        final NewKafkaConsumer kafkaConsumer = new NewKafkaConsumer(numConsumers, groupId, topoics);

        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                kafkaConsumer.shutdown();
            }
        });
        System.out.println("-- Application end");

    }

    public void getData() {

    }

    static class ConsumerLoop implements Runnable {

        private final KafkaConsumer<String, byte[]> consumer;
        private final List<String> topoics;
        private final int id;
        private final String PREFIX;

        ConsumerLoop(int id, String groupId, List<String> topoics) {
            this.id = id;
            this.topoics = topoics;
            this.PREFIX = "consumer-" + id +" ";
            Properties props = getDefaultProperties();
            props.put("group.id", groupId);
            this.consumer = new KafkaConsumer<>(props);
        }

        private Properties getDefaultProperties() {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("auto.offset.reset", "earliest");
            props.put("enable.auto.commit", "false");
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", ByteArrayDeserializer.class.getName());
            return  props;
        }

        @Override
        public void run() {
            System.out.println(PREFIX + "started... :" + topoics);
            try {
                consumer.subscribe(topoics);

                //for (int i =0; i<5; i++) {
                while(true) {
                    ConsumerRecords<String,byte[]> records = consumer.poll(100);
                    System.out.println(PREFIX + " records  count : " + records.count());
                    for (ConsumerRecord<String, byte[]> record : records) {
                        Map<String, Object> data = new HashMap<>();
                        data.put("partition", record.partition());
                        data.put("offset", record.offset());
                        data.put("value", new String(record.value()));
                        System.out.println(PREFIX + " : " + data);
                        try {
                            consumer.commitSync();
                        } catch (CommitFailedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            } catch (WakeupException e) {
                e.printStackTrace();
            } finally {
                System.out.println(PREFIX + "run end, consumer close ");
                consumer.close();
            }

        }

        public void shutdown() {
            consumer.wakeup();
        }

    }

}

