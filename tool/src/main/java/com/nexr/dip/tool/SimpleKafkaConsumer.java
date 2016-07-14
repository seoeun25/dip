package com.nexr.dip.tool;

import com.google.common.annotations.VisibleForTesting;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SimpleKafkaConsumer consume the message from kafka cluster.
 */
public class SimpleKafkaConsumer {

    public static int timeout = 60000;
    public static int bufferSize = 64 * 1024;
    private List<String> brokers = new ArrayList<String>();

    public SimpleKafkaConsumer() {
        brokers = new ArrayList<String>();
    }

    public static void main(String args[]) {
        if (args.length == 0) {
            System.out.println("brokerhost, brokerport, topic, parition, offset, maxread");
            System.out.println("localhost, 9092, hello, 7, 10, 5");
            System.exit(0);
            args = new String[]{"localhost", "9092", "az-test", "1", "426810", "10"};
        }
        List<String> seedBrokers = new ArrayList<String>();
        seedBrokers.add(args[0]);
        int port = Integer.parseInt(args[1]);
        String topic = args[2];
        int partition = Integer.parseInt(args[3]);
        long offset = Long.parseLong(args[4]);
        long maxReadCount = Long.parseLong(args[5]);

        SimpleKafkaConsumer example = new SimpleKafkaConsumer();
        try {
            if (args.length > 5) {
                example.run(offset, maxReadCount, topic, partition, seedBrokers, port);
            } else {
                example.run(maxReadCount, topic, partition, seedBrokers, port);
            }
        } catch (Exception e) {
            System.out.println("Oops:" + e);
            e.printStackTrace();
        }
    }

    @VisibleForTesting
    public long getLastOffset(SimpleConsumer consumer, String topic, int partition, String clientName) {
        long whichTime = OffsetRequest.LatestTime();
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    @VisibleForTesting
    public long getEarliestOffset(SimpleConsumer consumer, String topic, int partition, String clientName) {
        Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        offsetInfo.put(new TopicAndPartition(topic, partition),
                new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1));
        OffsetResponse response =
                consumer.getOffsetsBefore(new kafka.javaapi.OffsetRequest(offsetInfo, kafka.api.OffsetRequest.CurrentVersion(),
                        clientName));
        long[] endOffset = response.offsets(topic, partition);
        consumer.close();
        return endOffset[0];
    }

    public void run(long maxReadCount, String topic, int partition, List<String> seedBrokers, int port) throws
            Exception {
        PartitionMetadata metadata = findLeader(seedBrokers, port, topic, partition);
        if (metadata == null) {
            System.out.println("Can't find metadata for Topic and Partition. Exiting");
            return;
        }
        if (metadata.leader() == null) {
            System.out.println("Can't find Leader for Topic and Partition. Exiting");
            return;
        }
        String leadBroker = metadata.leader().host();
        String clientName = "Client_" + topic + "_" + partition;

        SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, timeout, bufferSize, clientName);
        long readOffset = getLastOffset(consumer, topic, partition, clientName);
        System.out.println("-- readOffset : lastOffset from " + OffsetRequest.EarliestTime() + " : " + readOffset);

        run(readOffset, maxReadCount, topic, partition, seedBrokers, port);

    }

    @VisibleForTesting
    public void run(long offset, long maxReadCount, String topic, int partition, List<String> seedBrokers, int port) throws
            Exception {
        // find the meta data about the topic and partition we are interested in
        //
        PartitionMetadata metadata = findLeader(seedBrokers, port, topic, partition);
        if (metadata == null) {
            System.out.println("Can't find metadata for Topic and Partition. Exiting");
            return;
        }
        if (metadata.leader() == null) {
            System.out.println("Can't find Leader for Topic and Partition. Exiting");
            return;
        }
        String leadBroker = metadata.leader().host();
        String clientName = "Client_" + topic + "_" + partition;

        SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, timeout, bufferSize, clientName);
        long lastOffset = getLastOffset(consumer, topic, partition, clientName);
        long readOffset = offset;
        System.out.println("----  offset : " + offset);
        System.out.println("----  latestOffset : " + lastOffset);
        System.out.println("----  maxRead : " + maxReadCount);

        int tryCount = 0;
        int numErrors = 0;
        while (maxReadCount > 0) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, port, timeout, bufferSize, clientName);
            }
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(topic, partition, readOffset, 100) // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
                    .build();
            FetchResponse fetchResponse = consumer.fetch(req);

            if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(topic, partition);
                System.out.println("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                if (numErrors > 5) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    readOffset = getLastOffset(consumer, topic, partition, clientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                leadBroker = findNewLeader(leadBroker, topic, partition, port);
                continue;
            }
            numErrors = 0;

            long numRead = 0;
            Object resultSet = fetchResponse.messageSet(topic, partition);

            //System.out.println("-- resultSet : " + resultSet.getClass().getName());
            kafka.javaapi.message.ByteBufferMessageSet messageAndOffsetSet = fetchResponse.messageSet(topic, partition);
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                    System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();

                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                System.out.println("offset=" + String.valueOf(messageAndOffset.offset()) + ", " + new String(bytes, "UTF-8"));
                numRead++;
                maxReadCount--;
                if (maxReadCount == 0) {
                    break;
                }
            }

            if (numRead == 0) {
                try {
                    System.out.println("unRead == 0");
                    tryCount++;
                    if (tryCount > 5) {
                        maxReadCount = 0;
                    }
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                }
            }
        }
        if (consumer != null) consumer.close();
    }

    public String findNewLeader(String a_oldLeader, String a_topic, int a_partition, int a_port) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(brokers, a_port, a_topic, a_partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    @VisibleForTesting
    public PartitionMetadata findLeader(List<String> seedBrokers, int port, String topic, int partition) {
        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed : seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, port, timeout, bufferSize, "leaderLookup");
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    List<PartitionMetadata> partitionMetadatas = item.partitionsMetadata();
                    System.out.println(item.topic() + " : partitionMetas : " + partitionMetadatas.size());
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + topic
                        + ", " + partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null) {
            brokers.clear();
            for (kafka.cluster.BrokerEndPoint replica : returnMetaData.replicas()) {
                brokers.add(replica.host());
            }
        }
        return returnMetaData;
    }

    @VisibleForTesting
    public TopicMetadata getTopicMetadata(List<String> seedBrokers, int port, String topic) {
        TopicMetadata returnMetaData = null;
        loop:
        for (String seed : seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, port, timeout, bufferSize, "topicLookup");
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    if (item.topic().equals(topic)) {
                        returnMetaData = item;
                        break;
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + topic
                        + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        return returnMetaData;
    }
}
