package com.nexr.dip.tool;

import com.nexr.dip.common.Utils;
import junit.framework.Assert;
import kafka.api.ConsumerMetadataRequest;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.ConsumerMetadataResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.network.BlockingChannel;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SimpleKafkaConsumerTest {

    final static TopicAndPartition testPartition0 = new TopicAndPartition("employee", 0);
    final static TopicAndPartition testPartition1 = new TopicAndPartition("employee", 1);
    final static TopicAndPartition testPartition2 = new TopicAndPartition("employee", 2);
    final static TopicAndPartition testPartition3 = new TopicAndPartition("employee", 3);
    final static TopicAndPartition testPartition4 = new TopicAndPartition("employee", 4);
    final static TopicAndPartition testPartition5 = new TopicAndPartition("employee", 5);
    final static TopicAndPartition testPartition6 = new TopicAndPartition("employee", 6);
    final static TopicAndPartition testPartition7 = new TopicAndPartition("employee", 7);
    final static TopicAndPartition testPartition8 = new TopicAndPartition("employee", 8);
    final static TopicAndPartition testPartition9 = new TopicAndPartition("employee", 9);



    final static String MY_GROUP = "testGroup";
    final static String MY_CLIENTID = "demoClientId";
    private static SimpleKafkaConsumer simpleKafkaConsumer;
    private static String broker;
    private static List<String> brokers;
    private static int port = 9092;

    @BeforeClass
    public static void setupClass() {
        simpleKafkaConsumer = new SimpleKafkaConsumer();
        broker = "sembp.nexr.com";
        brokers = new ArrayList<String>();
        brokers.add(broker);
        port = 9092;
    }

    @Test
    public void testPartitionMeta() {
        String topic = "employee";
        int parition = 0;

        TopicMetadata topicMetadata = simpleKafkaConsumer.getTopicMetadata(brokers, port, topic);
        Assert.assertNotNull(topicMetadata);

        List<PartitionMetadata> partitionMetadataList = topicMetadata.partitionsMetadata();
        System.out.println(topic + ", paritions : " + partitionMetadataList.size());
        for (PartitionMetadata partitionMetadata : partitionMetadataList) {
            System.out.println(partitionMetadata.partitionId() + " : size = " + partitionMetadata.sizeInBytes());
        }
    }

    @Test
    public void testFindLeader() {
        String topic = "employee";
        int partition = 0;
        PartitionMetadata partitionMetadata = simpleKafkaConsumer.findLeader(brokers, port, topic, partition);
        Assert.assertNotNull(partitionMetadata);
        kafka.cluster.Broker broker = partitionMetadata.leader();
        System.out.println("broker host : " + broker.host());
    }

    @Test
    public void testGetLatestOffset() throws Exception {
        String clientName = "lookupLatestOffset";
        String topic = "employee";
        int partition = 1;
        long whichtime = Utils.parseTime("2015-10-14", "yyyy-MM-dd");
        whichtime = kafka.api.OffsetRequest.LatestTime();
        System.out.println("whichtime latestTime: " + Utils.getDateString(whichtime));
        System.out.println("earlistTime : " + Utils.getDateString(kafka.api.OffsetRequest.EarliestTime()));
        SimpleConsumer simpleConsumer = new SimpleConsumer(broker, port, SimpleKafkaConsumer.timeout, SimpleKafkaConsumer.bufferSize,
                clientName);
        long offset = simpleKafkaConsumer.getLastOffset(simpleConsumer, topic, partition, clientName);
        System.out.println("latestOffset : " + offset);
    }

    @Test
    public void testOffsets() throws Exception{
        String clientName = "offsetTest";
        String topic = "hello";

        TopicMetadata topicMetadata = simpleKafkaConsumer.getTopicMetadata(brokers, port, topic);
        Assert.assertNotNull(topicMetadata);

        List<PartitionMetadata> partitionMetadataList = topicMetadata.partitionsMetadata();
        System.out.println(topic + ", paritions : " + partitionMetadataList.size());
        for (PartitionMetadata partitionMetadata : partitionMetadataList) {
            int partition = partitionMetadata.partitionId();
            testGetEarlistOffset(topic, partition);
        }
    }

    public void testGetEarlistOffset(String topic, int partition) throws Exception {
        String clientName = "earliestOffset";
        SimpleConsumer consumer = createSimpleConsumer(clientName);
        long earliestOffset = simpleKafkaConsumer.getEarliestOffset(consumer, topic, partition, clientName);

        long latestOffest = simpleKafkaConsumer.getLastOffset(consumer, topic, partition, "latestOffest");
        System.out.println(topic + ", " + partition + ", earliest = " + earliestOffset + ", latestOffest = " + latestOffest);

    }

    @Test
    public void testRun() throws Exception {

        String clientName = "runTest";
        String topic = "__consumer_offsets";
        int partition = 49;
        SimpleConsumer consumer = createSimpleConsumer(clientName);
        long earliestOffset = simpleKafkaConsumer.getEarliestOffset(consumer, topic, partition, clientName);
        long maxReadCount = 20;
        long latestOffset = simpleKafkaConsumer.getLastOffset(consumer, topic, partition, clientName);
        System.out.println("earliestOffset : " + earliestOffset);
        System.out.println("latestOffset : " + latestOffset);

        simpleKafkaConsumer.run(earliestOffset, maxReadCount, topic, partition, brokers, port);
    }

    private SimpleConsumer createSimpleConsumer(String clientName) {
        SimpleConsumer simpleConsumer = new SimpleConsumer(broker, port, SimpleKafkaConsumer.timeout, SimpleKafkaConsumer.bufferSize,
                clientName);
        return simpleConsumer;
    }

    @Test
    public void testOffsetManager() {
        String topic = "employee";
        BlockingChannel channel = new BlockingChannel("sembp", 9092,
                BlockingChannel.UseDefaultBufferSize(),
                BlockingChannel.UseDefaultBufferSize(),
                5000 /* read timeout in millis */);
        channel.connect();

        int correlationId = 0;

        channel.send(new ConsumerMetadataRequest(MY_GROUP, ConsumerMetadataRequest.CurrentVersion(), correlationId++, MY_CLIENTID));
        ConsumerMetadataResponse metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());

        if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
            Broker offsetManager = metadataResponse.coordinator();
            // if the coordinator is different, from the above channel's host then reconnect
            channel.disconnect();
            channel = new BlockingChannel(offsetManager.host(), offsetManager.port(),
                    BlockingChannel.UseDefaultBufferSize(),
                    BlockingChannel.UseDefaultBufferSize(),
                    5000 /* read timeout in millis */);
            channel.connect();
            System.out.println("---- ConsumerMetadataRequest : NoError : offsetManager : " + offsetManager.connectionString());
            System.out.println(offsetManager.id() + ", " + offsetManager.toString());
        } else {
            // retry (after backoff)
            System.out.println("---- Failed to get OffsetManger meta : errorCode : " + metadataResponse.errorCode());
        }

        // commit
        long now = System.currentTimeMillis();
        Map<TopicAndPartition, OffsetAndMetadata> offsets = new LinkedHashMap<TopicAndPartition, OffsetAndMetadata>();
        offsets.put(testPartition0, new OffsetAndMetadata(100L, "--170--associated metadata", now));
        offsets.put(testPartition1, new OffsetAndMetadata(200L, "--170--more metadata", now));
        OffsetCommitRequest commitRequest = new OffsetCommitRequest(
                MY_GROUP,
                offsets,
                correlationId++,
                MY_CLIENTID,
                (short) 1 /* version */); // version 1 and above commit to Kafka, version 0 commits to ZooKeeper
        try {
            channel.send(commitRequest.underlying());
            OffsetCommitResponse commitResponse = OffsetCommitResponse.readFrom(channel.receive().buffer());
            if (commitResponse.hasError()) {
                Object error = commitResponse.errors().values();
                System.out.println("error : " + error.getClass().getName());
                for (Object errors: commitResponse.errors().values()) {
                    short partitionErrorCode = 0;
                    if (partitionErrorCode == ErrorMapping.OffsetMetadataTooLargeCode()) {
                        // You must reduce the size of the metadata if you wish to retry
                    } else if (partitionErrorCode == ErrorMapping.NotCoordinatorForConsumerCode() || partitionErrorCode == ErrorMapping.ConsumerCoordinatorNotAvailableCode()) {
                        channel.disconnect();
                        // Go to step 1 (offset manager has moved) and then retry the commit to the new offset manager
                    } else {
                        // log and retry the commit
                    }
                }
            }
        } catch (Exception ioe) {
            channel.disconnect();
            // Go to step 1 and then retry the commit
        }

        // How to fetch offsets
        System.out.println("correlationId : " + correlationId);
        List<TopicAndPartition> partitions = new ArrayList<TopicAndPartition>();
        partitions.add(testPartition0);
        partitions.add(testPartition1);

        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                MY_GROUP,
                partitions,
                (short) 1 /* version */, // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
                correlationId++,
                MY_CLIENTID);
        try {
            channel.send(fetchRequest.underlying());
            OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(channel.receive().buffer());
            OffsetMetadataAndError result = fetchResponse.offsets().get(testPartition1);
            System.out.println("result : " + result.toString());
            short offsetFetchErrorCode = result.error();
            if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
                channel.disconnect();
                System.out.println("---- NotCoordinatorForConsumerCode");
                // Go to step 1 and retry the offset fetch
            //} else if (errorCode == ErrorMapping.OffsetsLoadInProgress()) {
                // retry the offset fetch (after backoff)
            } else if (offsetFetchErrorCode == ErrorMapping.NoError()) {
                System.out.println("---- NoError");
                long retrievedOffset = result.offset();
                String retrievedMetadata = result.metadata();
                System.out.println("retrievedOffset : " + retrievedOffset);
                System.out.println("retrievedMetadata : " + retrievedMetadata);
            } else {
                System.out.println("offsetFetchErrorCode : " + offsetFetchErrorCode);

            }
        }
        catch (Exception e) {
            channel.disconnect();
            // Go to step 1 and then retry offset fetch after backoff
        }
    }

    @Test
    public void testOffsetManager2() throws Exception{
        long now = System.currentTimeMillis();
        Map<TopicAndPartition, OffsetAndMetadata> offsets = new LinkedHashMap<TopicAndPartition, OffsetAndMetadata>();
        offsets.put(testPartition0, new OffsetAndMetadata(42682, "-- associated metadata", -1L));
        offsets.put(testPartition1, new OffsetAndMetadata(15821, "-- more metadata 1", -1L));

        int correlationId = 1;
        OffsetCommitRequest commitRequest = new OffsetCommitRequest(
                MY_GROUP,
                offsets,
                correlationId,
                MY_GROUP,
                (short) 1 /* version */); // version 1 and above commit to Kafka, version 0 commits to ZooKeeper

        SimpleConsumer simpleConsumer = createSimpleConsumer(MY_GROUP);
        OffsetCommitResponse commitResponse = simpleConsumer.commitOffsets(commitRequest);
        System.out.println("result : " + commitResponse.getClass().getName());

        if (commitResponse.hasError()) {
            Object error = commitResponse.errors().values();
            System.out.println("error : " + error.getClass().getName());
//            for (Object errors: commitResponse.errors().values()) {
//                short partitionErrorCode = 0;
//                if (partitionErrorCode == ErrorMapping.OffsetMetadataTooLargeCode()) {
//                    // You must reduce the size of the metadata if you wish to retry
//                } else if (partitionErrorCode == ErrorMapping.NotCoordinatorForConsumerCode() || partitionErrorCode == ErrorMapping.ConsumerCoordinatorNotAvailableCode()) {
//                    channel.disconnect();
//                    // Go to step 1 (offset manager has moved) and then retry the commit to the new offset manager
//                } else {
//                    // log and retry the commit
//                }
//            }
        }

        simpleConsumer = createSimpleConsumer(MY_GROUP);
        Thread.sleep(1000);
        List<TopicAndPartition> partitions = new ArrayList<TopicAndPartition>();
        //TopicAndPartition testPartition38 = new TopicAndPartition("__consumer_offsets", 18);
        partitions.add(testPartition0);
        partitions.add(testPartition1);

        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                MY_GROUP,
                partitions,
                (short) 1 /* version */, // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
                correlationId,
                MY_GROUP);

        Map<TopicAndPartition,OffsetMetadataAndError> offsets1=
                simpleConsumer.fetchOffsets(fetchRequest).offsets();
        for (Map.Entry<TopicAndPartition, OffsetMetadataAndError> entry: offsets1.entrySet()) {
            System.out.println(entry.getKey().topic() + ", " + entry.getKey().partition());
            System.out.println("-- metadata : " + entry.getValue().metadata());
            System.out.println("-- error : " + entry.getValue().error() + " : " + getError(entry.getValue().error()));
            System.out.println("-- offset : " + entry.getValue().offset());
        }
    }

    private String getError(short errorCode) {
        String errorStr = "";
        if (errorCode == ErrorMapping.UnknownTopicOrPartitionCode()) {
            errorStr = "UnknownTopicOrPartitionCode";
        }
        return errorStr;

    }


}
