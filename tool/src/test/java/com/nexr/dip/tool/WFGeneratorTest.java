package com.nexr.dip.tool;

import org.apache.kafka.common.utils.Utils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class WFGeneratorTest {

    public static final String TOPIC_VAR = "${topic}";

    private static WFGenerator executorGenerator;

    @BeforeClass
    public static void beforeClass() throws Exception {
        executorGenerator = new WFGenerator(WFGenerator.ExecuteType.Workflow);
    }

    @Test
    public void testWFAppGenerate() {
        try {
            executorGenerator.generate(executorGenerator.getAvroSrcInfos(), executorGenerator.getTextSrcInfos());
        }catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testGenerateClearTopics() throws IOException{

        File shellFile = new File("clear-topic.sh");
        FileWriter fileWriter = null;

        String template = "/usr/lib/kafka/bin/kafka-topics.sh --zookeeper aih012:2181 --topic ${topic} --alter --config " +
                "retention.ms=1";
        String avroSrcInfos = executorGenerator.getAvroSrcInfos();
        String textSrcInfos = executorGenerator.getTextSrcInfos();
        List<String> commands = replaced(avroSrcInfos, template);
        commands.addAll(replaced(textSrcInfos, template));

        try {
            fileWriter = new FileWriter(shellFile);
            for (String command: commands) {
                fileWriter.write(command + "\n");
            }

            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileWriter != null) {
                fileWriter.close();
            }
        }

        System.out.println("==== shell File : " + shellFile.getAbsolutePath());

    }

    @Test
    public void testGenerateRecoveryRetentionTopics() throws IOException{

        File shellFile = new File("recovery-topic-retention.sh");
        FileWriter fileWriter = null;

        //long retention = 259200000l; // 3days
        long retention = 1000 * 60 * 60 * 24 * 3;
//        String template = "/usr/lib/kafka/bin/kafka-topics.sh --zookeeper aih012:2181 --topic ${topic} --alter --config " +
//                "retention.ms=604800000";
        String template = "/usr/lib/kafka/bin/kafka-topics.sh --zookeeper aih012:2181 --topic ${topic} --alter --config " +
                "retention.ms="  + String.valueOf(retention);
        String avroSrcInfos = executorGenerator.getAvroSrcInfos();
        String textSrcInfos = executorGenerator.getTextSrcInfos();
        List<String> commands = replaced(avroSrcInfos, template);
        commands.addAll(replaced(textSrcInfos, template));

        try {
            fileWriter = new FileWriter(shellFile);
            for (String command: commands) {
                fileWriter.write(command + "\n");
            }

            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileWriter != null) {
                fileWriter.close();
            }
        }

        System.out.println("==== shell File : " + shellFile.getAbsolutePath());

    }

    @Test
    public void testGenerateDeleteTopics() throws IOException{

        File shellFile = new File("delete-topic.sh");
        FileWriter fileWriter = null;

        String template = "/usr/lib/kafka/bin/kafka-topics.sh --zookeeper aih012:2181 --delete --topic ${topic}";
        String avroSrcInfos = executorGenerator.getAvroSrcInfos();
        String textSrcInfos = executorGenerator.getTextSrcInfos();
        List<String> commands = replaced(avroSrcInfos, template);
        commands.addAll(replaced(textSrcInfos, template));

        try {
            fileWriter = new FileWriter(shellFile);
            for (String command: commands) {
                fileWriter.write(command + "\n");
            }

            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileWriter != null) {
                fileWriter.close();
            }
        }

        System.out.println("==== shell File : " + shellFile.getAbsolutePath());

    }

    @Test
    public void testGenerateTopics() throws IOException{

        File shellFile = new File("create-topic.sh");
        FileWriter fileWriter = null;

        String template = "/usr/lib/kafka/bin/kafka-topics.sh --zookeeper aih012:2181 --create --topic ${topic} --partitions 10 " +
                "--replication-factor 3";
        String avroSrcInfos = executorGenerator.getAvroSrcInfos();
        String textSrcInfos = executorGenerator.getTextSrcInfos();
        List<String> commands = replaced(avroSrcInfos, template);
        commands.addAll(replaced(textSrcInfos, template));

        try {
            fileWriter = new FileWriter(shellFile);
            for (String command: commands) {
                fileWriter.write(command + "\n");
            }

            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileWriter != null) {
                fileWriter.close();
            }
        }

        System.out.println("==== shell File : " + shellFile.getAbsolutePath());

    }

    public List<String> replaced(String srcInfos, String template) {
        List<String> commands = new ArrayList<>();
        String[] srcInfoArray = srcInfos.split(",");
        for (String srcInfo : srcInfoArray) {
            if (template.contains(TOPIC_VAR)) {
                commands.add(template.replace(TOPIC_VAR, srcInfo));
            } else {
                commands.add(template);
            }
        }
        return commands;
    }

    @Test
    public void testKafkaPartition() throws Exception{

        int numPartitions = 10;

        for (int i = 0; i < 10; i++) {
            double a = i;
            int partition = getKafkaPartition(String.valueOf(a), numPartitions);
            int bytePartiton = getKafkaBytePartition(String.valueOf(i), numPartitions);
            System.out.println(String.valueOf(a) + " ==> " + partition + " ,  " + bytePartiton);
        }
    }

    @Test
    public void testKafkaPartition2() throws Exception{

        int numPartitions = 10;

        for (int i = 0; i < 10; i++) {
            int partition = getKafkaPartition(i, numPartitions);
            System.out.println(i + " ==> " + partition);
        }
    }

    private int getKafkaPartition(String key, int numPartitions) {
        // hash the key to choose a partition

        int murmur2 = Utils.murmur2(key.getBytes());
        System.out.println("murmur2 : " + murmur2);

        int abs = Utils.abs(Utils.murmur2(key.getBytes()));
        System.out.println("abs : " + abs);
        // kafka Partitioner hash method : See Partitioner.java at Kafka
        return Utils.abs(Utils.murmur2(key.getBytes())) % numPartitions;
    }

    private int getKafkaBytePartition(String key, int numPartitions) {
        //Utils.abs(java.util.Arrays.hashCode(key.asInstanceOf[Array[Byte]])) % numPartitions

        int abs = Utils.abs(java.util.Arrays.hashCode(key.getBytes()));
        int partition = abs % numPartitions;
        return partition;
        //return Utils.abs(java.util.Arrays.hashCode(key.getBytes()) % numPartitions;
    }

    private int getKafkaPartition(int key, int numPartitions) {
        // hash the key to choose a partition

        byte[] bytes = ByteBuffer.allocate(4).putInt(key).array();
        System.out.println(bytes.toString());

        int murmur2 = Utils.murmur2(bytes);
        System.out.println("murmur2 : " + murmur2);

        return Utils.abs(Utils.murmur2(bytes)) % numPartitions;
    }

}
