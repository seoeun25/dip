package com.nexr.dip.tool;

import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageEncoder;
import com.nexr.dip.DipException;
import com.nexr.dip.client.DipClient;
import com.nexr.dip.common.Utils;
import com.nexr.dip.conf.Configurable;
import com.nexr.dip.loader.ScheduledService;
import com.nexr.dip.record.DipRecordBase;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MessageGenerator for the various usages include performance test.
 */
public class MessageGenerator {

    public static final String PREFIX = "[Generator] ";
    public static final String BROKER = "broker";
    public static final String TOPIC = "topic";
    public static final String MAX_COUNT = "maxcount";
    public static final String CONCURRENT = "concurrent";
    public static final String SCHEMA_REGISTRY = "schemaregistry";
    public static final String SCHEMA_REPO = "schemarepo";
    public static final String TYPE = "type";
    public static final String BATCH_SIZE = "batchsize";
    public static final String BUFFER_SIZE = "buffersize";
    public static final String FILE = "file";
    public static final String VERBOSE = "verbose";
    private static long startTime;
    private final String AVRO_SCHEMA_REGISTRY = "com.nexr.schemaregistry.AvroSchemaRegistry";
    private String topic;
    private int maxCount = 1000;
    private int concurrent = 1;
    private long batchSize = 16384 * 20;
    private long bufferSize = 33554432 * 20;
    private boolean verbose = true;
    private DipClient dipClient;
    private DipClient.MESSAGE_TYPE messageType = DipClient.MESSAGE_TYPE.AVRO;
    private Schema schema;
    private AtomicInteger sendCount = new AtomicInteger(0);
    private AtomicInteger failCount = new AtomicInteger(0);
    private Object lock = new Object();
    private Map<String, String> configMap = new HashMap<String, String>();
    private ExecutorService executorService;
    private AtomicInteger senderNumber = new AtomicInteger(0);

    public MessageGenerator(String configs) {

        initDefaultConfig();
        parseArgs(configs);

        init();
        try {
            initSender();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String... args) {
        String configs = "";
        if (args.length == 2) {
            if (args[0].equals("--config")) {
                configs = args[1];
            }
        }

        System.out.println(PREFIX + "[ configs :" + configs + " ] ");
        try {
            MessageGenerator generator = new MessageGenerator(configs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void produceRecord() {
        try {
            if (topic.equals("employee")) {
                produceEmployeeRecord();
            } else if (topic.equals("gpx_port")) {
                produceGpxportRecord(configMap.get(FILE));
            } else if (topic.equals("network_info")) {
                produceNetworkPacketRecord();
            } else if (topic.equals("sip")) {
                produceSipJson();
            } else {
                System.out.println("no producer for " + topic);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void init() {
        try {
            DipClient.MESSAGE_TYPE messageType = DipClient.MESSAGE_TYPE.AVRO;
            if (!configMap.get(TYPE).equals("avro")) {
                messageType = DipClient.MESSAGE_TYPE.TEXT;
            }

            System.out.println(PREFIX + topic + ", maxCount : " + maxCount);

            dipClient = new DipClient(configMap.get(BROKER), topic, messageType, getProperteis());
            dipClient.start();

            if (configMap.get(TYPE).equals("text")) {
                schema = DipClient.TEXT_FORMAT_SCHEMA;
            } else {
                if (configMap.get(SCHEMA_REGISTRY).equals(AVRO_SCHEMA_REGISTRY)) {
                    schema = dipClient.getSchema(topic);
                }
            }

            System.out.println(PREFIX + "schema : " + schema + "\n");

            executorService = Executors.newFixedThreadPool(20);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void parseArgs(String args) {
        String[] configs = args.split(",");
        for (String config : configs) {
            String[] keyValue = config.trim().split("=");
            if (keyValue.length == 2) {
                configMap.put(keyValue[0].toString().trim(), keyValue[1].toString().trim());
            }
        }

        topic = configMap.get(TOPIC);
        try {
            maxCount = Integer.parseInt(configMap.get(MAX_COUNT));
            if (maxCount == -1) {
                maxCount = Integer.MAX_VALUE;
            } else if (maxCount < 0) {
                maxCount = 1000;
            }
        } catch (Exception e) {

        }
        try {
            concurrent = Integer.parseInt(configMap.get(CONCURRENT));
        } catch (Exception e) {

        }

        try {
            batchSize = Long.parseLong(configMap.get(BATCH_SIZE));
        } catch (Exception e) {

        }
        try {
            bufferSize = Long.parseLong(configMap.get(BUFFER_SIZE));
        } catch (Exception e) {

        }
        try {
            verbose = Boolean.valueOf(configMap.get(VERBOSE));
        } catch (Exception e) {

        }
        for (Map.Entry<String, String> entry : configMap.entrySet()) {
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }

    }

    private void initDefaultConfig() {
        configMap.put(BROKER, "localhost:9092");
        configMap.put(TOPIC, "employee");
        configMap.put(MAX_COUNT, "1000");
        configMap.put(CONCURRENT, "1");
        configMap.put(SCHEMA_REGISTRY, "com.nexr.schemaregistry.AvroSchemaRegistry");
        configMap.put(SCHEMA_REPO, "http://localhost:18181/repo");
        configMap.put(TYPE, "avro");
        configMap.put(BATCH_SIZE, String.valueOf(batchSize));
        configMap.put(BUFFER_SIZE, String.valueOf(bufferSize));
        configMap.put(VERBOSE, "true");
    }

    private void initSender() throws Exception {
        startTime = System.currentTimeMillis();
        for (int i = 0; i < concurrent; i++) {
            executorService.submit(createSender());
            senderNumber.incrementAndGet();
            Thread.sleep(10);
        }
    }

    private void isSendFinish() {
        System.out.println(PREFIX + Utils.formatTime(System.currentTimeMillis()) + " Sender finish, " + senderNumber
                .decrementAndGet() + "/" + concurrent + " is running");

        if (senderNumber.get() == 0) {
            //System.out.println(PREFIX + "sendFinish");
            saveResult();
        }

    }

    private void saveResult() {
        int sendCount = this.sendCount.get();
        System.out.println(PREFIX + Utils.formatTime(System.currentTimeMillis())
                + ", sent [" + sendCount + "], " + "failed [" + failCount.get() + "]");

        long endTime = System.currentTimeMillis();
        long period = (endTime - startTime) / 1000;
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(PREFIX + "FINISH : Start : " + Utils.getDateString(startTime) + " ");
        stringBuilder.append("[" + topic + "] ");
        stringBuilder.append("[" + sendCount + "] ");
        stringBuilder.append("[" + period + "s] ");
        System.out.println(stringBuilder.toString());

        try {
            String fileName = Utils.getDateString(startTime) + "_" + sendCount + "_" + topic + "_" + period + "s";
            File f = new File(fileName);
            f.createNewFile();
        } catch (Exception e) {
            e.printStackTrace();
        }

        close();
    }


    private Runnable createSender() {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                System.out.println(PREFIX + Utils.formatTime(System.currentTimeMillis()) + " Sender [" + Thread.currentThread()
                        .getName() + "] start ......");
                try {
                    produceRecord();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                isSendFinish();
            }
        };
        return runnable;
    }

    public void send(DipRecordBase recordBase) throws InterruptedException {
        try {
            dipClient.send(recordBase);

            int count = sendCount.incrementAndGet();
            if (count % 1000000 == 0) {
                if (verbose) {
                    System.out.println(Utils.getDateString(System.currentTimeMillis()) + ", send [" +
                            Thread.currentThread().getName() + "] " + count);
                    System.out.println("memory : total : " + Runtime.getRuntime().totalMemory() + ", max : " + Runtime
                            .getRuntime().maxMemory() + " , free " + Runtime.getRuntime().freeMemory());
                }
                Thread.sleep(1);
            }
        } catch (DipException e) {
            System.out.println("Failed to send : " + e.getMessage() + ", skip this line : " + recordBase.getData().toString());
            failCount.incrementAndGet();
            e.printStackTrace();
            Thread.sleep(10);
        } catch (NullPointerException e) {
            System.out.println("Failed to send : " + e.getMessage() + ", skip this line : " + recordBase.getData().toString());
            failCount.incrementAndGet();
            e.printStackTrace();
            Thread.sleep(10);
        }
    }

    private Properties getProperteis() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configMap.get(BROKER));
        if (configMap.get(TYPE).equals("avro")) {
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        } else {
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }
        properties.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "false");
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(Configurable.SCHEMAREGISTRY_CLASS, configMap.get(SCHEMA_REGISTRY)); //com.nexr.schemaregistry.AvroSchemaRegistry
        properties.put(Configurable.SCHEMAREGIDTRY_URL, configMap.get(SCHEMA_REPO));
        properties.put(KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS, configMap.get(SCHEMA_REGISTRY));
        if (batchSize != 0) {
            properties.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(batchSize));
        }
        if (bufferSize != 0) {
            properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(bufferSize));
        }
        return properties;
    }

    public void close() {
        dipClient.close();
        System.exit(0);
    }

    public void produceEmployeeRecord() throws Exception {
        //long time = getTime(2015, 07, 27, 20, 30);
        long time = System.currentTimeMillis();

        int i = 0;
        while (true) {
            GenericRecord record = new GenericData.Record(schema);
            synchronized (lock) {
                String payload = String.valueOf(i) + "::: seoeun---azrael-seoeun---azrael-seoeun---azrael-seoeun---azrael-seoeun" +
                        "---azrael-seoeun---azrael-seoeun---azrael-seoeun---azrael-seoeun---azrael-seoeun" +
                        "---azrael-seoeun---azrael-seoeun---azrael-END";
                record.put("name", payload);
                record.put("favorite_number", String.valueOf(i));
                record.put("wrk_dt", time);
                record.put("src_info", "employee");
            }
            DipRecordBase<GenericRecord> recordBase = new DipRecordBase<>(topic, record, messageType);
            send(recordBase);

            if (sendCount.get() >= maxCount) {
                break;
            }
        }
    }

    public void produceNetworkPacketRecord() throws Exception {
        System.out.println(PREFIX + Utils.formatTime(System.currentTimeMillis()) + " : produce record for " +
                topic);

        //long time = getTime(2015, 07, 27, 20, 30);
        long time = System.currentTimeMillis();

        int i = 0;
        while (true) {
            GenericRecord record = new GenericData.Record(schema);
            String srcMac = String.valueOf(i) + "::AB::AB::AA";
            String srcIp = "192.168.70.1";
            String destMac = String.valueOf(i) + "::DC:EF:FF";
            String destIp = "192.168.0.2";
            record.put("src_mac", srcMac);
            record.put("src_ip", srcIp);
            record.put("src_port", "");
            record.put("dest_mac", destMac);
            record.put("dest_ip", destIp);
            record.put("dest_port", "");
            record.put("host", "www.abc.example.com");
            record.put("referer", "");
            record.put("payload_length", srcMac);
            record.put("wrk_dt", time);
            record.put("src_info", topic);

            DipRecordBase<GenericRecord> recordBase = new DipRecordBase<>(topic, record, messageType);
            send(recordBase);

            if (sendCount.get() >= maxCount) {
                break;
            }
        }
        System.out.println(PREFIX + Utils.formatTime(System.currentTimeMillis()) + " : produce end : " +
                sendCount.get());
    }

    public void produceSipJson() throws Exception {
        System.out.println(PREFIX + Utils.formatTime(System.currentTimeMillis()) + " : produce record for " +
                topic);

        //{"timestamp": "2016-05-08T07:19:03Z", "sip": "144.95.206.85", "url": "www.VGOFV.com", "packet_total": "215"}

        //long time = getTime(2015, 07, 27, 20, 30);
        long time = System.currentTimeMillis();

        while (true) {
            String timeLable = Utils.formatTime(time, "yyyy-MM-dd", "UTC");
            timeLable = timeLable + "T" + Utils.formatTime(time, "hh:mm:ss", "UTC") + "Z";
            String msg = "";
            if ((sendCount.get() + 1) % 2 == 0) {
                msg = "{\"timestamp\": \"" + timeLable + "\", \"sip\": \"a\", \"packet_total\": \"14\"}";
            } else {
                msg = "{\"timestamp\": \"" + timeLable + "\", \"sip\": \"b\", \"packet_total\": \"10\"}";
            }
            GenericRecord record = new GenericData.Record(schema);
            record.put(DipRecordBase.MESSAGE_FIELD, msg);

            DipRecordBase<GenericRecord> recordBase = new DipRecordBase<>(topic, record, messageType);
            send(recordBase);

            if (sendCount.get() >= maxCount) {
                break;
            }
        }
        System.out.println(PREFIX + Utils.formatTime(System.currentTimeMillis()) + " : produce end : " +
                sendCount.get());
    }

    public GenericRecord createGpxPort(String line, long time) {
        //221.161.133.15gi8, BSBG03871, 221.161.133.15, 8, ./down, auto, auto/auto, ., 15, 104000/-, ,2015-10-25 20:00:09
        GenericRecord record = new GenericData.Record(schema);
        String[] datas = line.split(",");
        record.put("nescode", datas[1]);
        record.put("equip_ip", datas[2]);
        record.put("port", datas[3]);
        record.put("setup_val", datas[4]);
        record.put("nego", datas[5]);
        record.put("setup_speed", datas[6]);
        record.put("curnt_speed", datas[7]);
        record.put("mac_cnt", datas[8]);
        record.put("downl_speed_val", datas[9]);
        record.put("etc_extrt_info", datas[10]);
        record.put("wrk_dt", time);
        record.put("src_info", topic);
        return record;
    }

    public void produceGpxportRecord(String file) throws Exception {
        System.out.println(PREFIX + "produce record for " + topic + ", maxCount : " + maxCount + ", from [" +
                file + "]");

        String wrk_dt = "2015-09-25";
        long time = Utils.parseTime(wrk_dt, "yyyy-MM-dd");
        System.out.println("wrk_dt : " + wrk_dt + ", " + Utils.formatTime(time) + " : start : " + Utils
                .formatTime(System.currentTimeMillis()));

        RandomAccessFile aFile = new RandomAccessFile(file, "r");
        FileChannel inChannel = aFile.getChannel();
        ByteBuffer buffer = ByteBuffer.allocate(4096);

        int readByte = 0;
        StringBuilder stringBuilder = new StringBuilder();
        while ((readByte = inChannel.read(buffer)) > 0) {
            buffer.flip();
            for (int i = 0; i < buffer.limit(); i++) {
                byte b = buffer.get();
                if (b == '\n') {
                    if (!stringBuilder.toString().isEmpty()) {
                        GenericRecord record = createGpxPort(stringBuilder.toString(), time);
                        DipRecordBase<GenericRecord> recordBase = new DipRecordBase(topic, record, messageType);
                        send(recordBase);
                        stringBuilder.delete(0, stringBuilder.length());
                    }
                } else {
                    stringBuilder.append((char) b);
                }
            }
            buffer.clear(); // do something with the data and clear/compact it.
            if ((sendCount.get() % 1000) == 0) {
                Thread.sleep(10);
            }
            if (sendCount.get() >= maxCount) {
                break;
            }
        }

        System.out.println(PREFIX + Utils.formatTime(System.currentTimeMillis()) + " : produce end : " +
                sendCount.get());

    }



    public void testReadFile() throws IOException {
        System.out.println("testReadFile");
        RandomAccessFile aFile = new RandomAccessFile("portinfo.dat", "r");
        FileChannel inChannel = aFile.getChannel();
        ByteBuffer buffer = ByteBuffer.allocate(512);

        Charset charset = Charset.forName("UTF-8");
        int readByte = 0;
        while ((readByte = inChannel.read(buffer)) > 0) {
            //System.out.println("\nreadByte : " + readByte);
            buffer.flip();
            for (int i = 0; i < buffer.limit(); i++) {
                byte b = buffer.get();
                System.out.print((char) b);
                if (b == '\n') {
                    System.out.println("**");
                }
                //System.out.print(charset.decode(buffer));
                //System.out.println(String.format("%02X ", b));
            }
            buffer.clear(); // do something with the data and clear/compact it.
        }
        inChannel.close();
        aFile.close();
    }

}
