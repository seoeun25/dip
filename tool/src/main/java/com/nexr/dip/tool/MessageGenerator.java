package com.nexr.dip.tool;

import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageEncoder;
import com.nexr.dip.DipException;
import com.nexr.dip.client.DipClient;
import com.nexr.dip.client.DummySchemaRegistry;
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
import org.apache.kafka.clients.tools.ProducerPerformance;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MessageGenerator for the various usages include performance test.
 */
public class MessageGenerator {

    public static final String PREFIX = "[MessageGenerator] ";
    public static final String BROKER = "broker";
    public static final String TOPIC = "topic";
    public static final String MAX_COUNT = "maxcount";
    public static final String CONCURRENT = "concurrent";
    public static final String QUEUE = "queue";
    public static final String SCHEMA_REGISTRY = "schemaregistry";
    public static final String TYPE = "type";
    public static final String BATCH_SIZE = "batchsize";
    public static final String BUFFER_SIZE = "buffersize";
    public static final String FILE = "file";
    private static long startTime;
    private final String DUMMY_SCHEMA_REGISTRY = "com.nexr.dip.client.DummySchemaRegistry";
    private final String AVRO_SCHEMA_REGISTRY = "com.seoeun.schemaregistry.AvroSchemaRegistry";
    private String topic;
    private int maxCount = 1000;
    private int concurrent = 5;
    private int queueSzie = 1000000; //4g mem
    private long batchSize = 16384;
    private long bufferSize = 33554432;
    private DipClient dipClient;
    private DipRecordBase<GenericRecord> dipRecordBase;
    private Schema schema;
    private AtomicInteger recordCount = new AtomicInteger(0);
    private AtomicInteger sendCount = new AtomicInteger(0);
    private AtomicInteger failCount = new AtomicInteger(0);
    private Map<String, String> configMap = new HashMap<String, String>();
    private ExecutorService executorService;
    private BlockingQueue<GenericRecord> blockingQueue;
    private boolean finished;

    private String gpx_port_schema = "{\"namespace\": \"com.nexr.dip.avro.schema\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"gpx_port\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"nescode\", \"type\": \"string\"},\n" +
            "     {\"name\": \"equip_ip\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"port\", \"type\": \"string\"},\n" +
            "     {\"name\": \"setup_val\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"nego\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"setup_speed\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"curnt_speed\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"mac_cnt\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"downl_speed_val\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"etc_extrt_info\", \"type\": [\"string\", \"null\"]},\n" +
            "     {\"name\": \"wrk_dt\", \"type\": \"long\"},\n" +
            "     {\"name\": \"src_info\", \"type\": \"string\"},\n" +
            "     {\n" +
            "         \"name\": \"header\",\n" +
            "         \"type\": {\n" +
            "             \"type\" : \"record\",\n" +
            "             \"name\" : \"headertype\",\n" +
            "             \"fields\" : [\n" +
            "                 {\"name\": \"time\", \"type\": \"long\"}\n" +
            "             ]\n" +
            "         }\n" +
            "      }\n" +
            " ]\n" +
            "}";

    public MessageGenerator(String configs) {

        initConfigs();
        parseArgs(configs);

        init();
        try {
            initSender();
        } catch (Exception e) {
            e.printStackTrace();
        }

        produceRecord();
    }

    private void produceRecord() {
        try {
            if (topic.equals("employee")) {
                produceEmployeeRecord();
            } else if (topic.equals("gpx_port")) {
                produceGpxportRecord(configMap.get(FILE));
                //produceGpxportRecordTest();
                //testPerformance();
            } else {
                System.out.println("no producer for " + topic);
            }
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

    private void init() {
        try {
            DipClient.MESSAGE_TYPE messageType = DipClient.MESSAGE_TYPE.AVRO;
            if (!configMap.get(TYPE).equals("avro")) {
                messageType = DipClient.MESSAGE_TYPE.TEXT;
            }

            System.out.println(PREFIX + topic + ", maxCount : " + maxCount);

            dipClient = new DipClient(configMap.get(BROKER), topic, messageType, getProperteis());
            dipClient.start();

            if (configMap.get(SCHEMA_REGISTRY).equals(AVRO_SCHEMA_REGISTRY)) {
                schema = dipClient.getSchema(topic);
            } else if (configMap.get(SCHEMA_REGISTRY).equals(DUMMY_SCHEMA_REGISTRY)) {
                if (topic.equals("employee")) {
                    schema = Schema.parse(DummySchemaRegistry.employee_schema);
                } else if (topic.equals("gpx_port")) {
                    schema = Schema.parse(gpx_port_schema);
                }
            }

            System.out.println("schema : " + schema + "\n");

            //dipRecordBase = new DipRecordBase<GenericRecord>(schema);
            blockingQueue = new LinkedBlockingQueue<>(queueSzie);
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
                //System.out.println(keyValue[0] + "=" + keyValue[1]);
            }
        }

        if (configMap.get(SCHEMA_REGISTRY).equals("dummy")) {
            configMap.put(SCHEMA_REGISTRY, DUMMY_SCHEMA_REGISTRY);
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
            queueSzie = Integer.parseInt(configMap.get(QUEUE));
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
        for (Map.Entry<String, String> entry : configMap.entrySet()) {
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }

    }

    private void initConfigs() {
        configMap.put(BROKER, "localhost:9092");
        configMap.put(TOPIC, "employee");
        configMap.put(MAX_COUNT, "1000");
        configMap.put(CONCURRENT, "5");
        configMap.put(QUEUE, "1000000");
        configMap.put(SCHEMA_REGISTRY, "com.seoeun.schemaregistry.AvroSchemaRegistry");
        configMap.put(TYPE, "avro");
        configMap.put(BATCH_SIZE, "16384");
        configMap.put(BUFFER_SIZE, "33554432");
    }

    private void initSender() throws Exception {
        for (int i = 0; i < concurrent; i++) {
            executorService.submit(createSender());
            Thread.sleep(10);
        }
    }

    private void isSendFinish() {
        if (finished) {
            return;
        }
        int sendCount = this.sendCount.get();
        System.out.println(Utils.formatTime(System.currentTimeMillis())
                + ", send finish, send [" + sendCount + "], " + "fail[" + failCount.get() + "], / record [" + recordCount.get() + "]");
        if ((sendCount + failCount.get()) >= recordCount.get()) {
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
    }

    private Runnable createSender() {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                System.out.println("Sender start 111 ......");
                int retry = 0;
                try {
                    while (true) {
                        GenericRecord record = blockingQueue.poll();
                        if (record != null) {
                            DipRecordBase<GenericRecord> recordBase =
                                    new DipRecordBase<GenericRecord>(record.getSchema().getName(), record,
                                            DipClient.MESSAGE_TYPE.AVRO);
                            try {
                                dipClient.send(recordBase);

            int count = sendCount.incrementAndGet();
            if (count % 100000 == 0 && waitTime > 0) {
                System.out.println(Utils.getDateString(System.currentTimeMillis()) + ", send [" +
                        Thread.currentThread().getName() + "] " + count );
                System.out.println("memory : total : " + Runtime.getRuntime().totalMemory() + ", max : " + Runtime
                        .getRuntime().maxMemory() + " , free " + Runtime.getRuntime().freeMemory());
                Thread.sleep(waitTime);
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
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(Configurable.SCHEMAREGISTRY_CLASS, configMap.get(SCHEMA_REGISTRY)); //com.seoeun.schemaregistry.AvroSchemaRegistry
        properties.put(Configurable.SCHEMAREGIDTRY_URL, "http://aih013:18181/repo");
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
        finished = true;
        System.exit(0);
    }

    public void produceEmployeeRecord() throws Exception {
        startTime = System.currentTimeMillis();
        System.out.println(PREFIX + Utils.formatTime(System.currentTimeMillis()) + " : produce record for " +
                topic);

        //long time = getTime(2015, 07, 27, 20, 30);
        long time = System.currentTimeMillis();

        int i = 0;
        while (true) {
            GenericRecord record = new GenericData.Record(schema);
            String payload = String.valueOf(i) + "::: seoeun---azrael-seoeun---azrael-seoeun---azrael-seoeun---azrael-seoeun" +
                    "---azrael-seoeun---azrael-seoeun---azrael-seoeun---azrael-seoeun---azrael-seoeun" +
                    "---azrael-seoeun---azrael-seoeun---azrael-END";
            record.put("name", payload);
            record.put("favorite_number", String.valueOf(i));
            record.put("wrk_dt", time);
            record.put("srcinfo", "employee");

            boolean offered = blockingQueue.offer(record);
            while (!offered) {
                System.out.println("queue is full : " + blockingQueue.size() + ", wait in 100 ms");
                Thread.sleep(100);
                offered = blockingQueue.offer(record);
            }

            i = recordCount.incrementAndGet();
            if ((i % 10000) == 0) {
                Thread.sleep(10);
            }
            if (i == maxCount) {
                break;
            }
        }
        System.out.println(PREFIX + Utils.formatTime(System.currentTimeMillis()) + " : produce Record end : " +
                recordCount.get());
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
        startTime = System.currentTimeMillis();
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
                        boolean offered = blockingQueue.offer(record);
                        while (!offered) {
                            //System.out.println("queue is full : " + blockingQueue.size() );
                            Thread.sleep(1000);
                            offered = blockingQueue.offer(record);
                        }
                        recordCount.getAndIncrement();
                        stringBuilder.delete(0, stringBuilder.length());
                    }
                } else {
                    stringBuilder.append((char) b);
                }
            }
            buffer.clear(); // do something with the data and clear/compact it.
            if ((recordCount.get() % 1000) == 0) {
                Thread.sleep(10);
            }
            if (recordCount.get() > maxCount) {
                break;
            }
        }

        System.out.println(PREFIX + Utils.formatTime(System.currentTimeMillis()) + " : produce record end : " +
                recordCount
                        .get());

    }

    public void produceGpxportRecordTest() throws Exception {
        startTime = System.currentTimeMillis();
        System.out.println(PREFIX + Utils.formatTime(System.currentTimeMillis()) + " : produce test11 record for " +
                topic + ", maxCount : ");

        String wrk_dt = "2015-09-25";
        long time = Utils.parseTime(wrk_dt, "yyyy-MM-dd");
        System.out.println("wrk_dt : " + wrk_dt + ", " + Utils.formatTime(time) + " : start : " + Utils
                .formatTime(System.currentTimeMillis()));

        String data = "221.161.133.15gi8,B3871,221.161.133.15,8,./d,a,a/auto,.,15,104000/-,2015-10-25 20:00:09    ";
        GenericRecord record = createGpxPort(data, time);
        System.out.println("byte length : " + data.getBytes().length);
        DipRecordBase<GenericRecord> dipRecordBase = new DipRecordBase<>(topic, record, DipClient.MESSAGE_TYPE.AVRO);
        byte[] payload = dipClient.getByteOf(topic, record);
        if (payload == null) {
            System.out.println("payload is null");
        } else {
            System.out.println("payload : " + payload.length);
        }
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<String, byte[]>(topic, null, payload);


//        while (true) {
//
//            dipClient.send(producerRecord);
//            recordCount.incrementAndGet();
//
//            if ((recordCount.get() % 1000) == 0) {
//                Thread.sleep(10);
//            }
//            if (recordCount.get() > maxCount) {
//                break;
//            }
//        }

        for (int i=0; i<maxCount; i++) {
            dipClient.send(producerRecord);
            recordCount.incrementAndGet();
        }

        System.out.println(PREFIX + Utils.formatTime(System.currentTimeMillis()) + " : produce test record end : " +
                recordCount
                        .get());

    }

    public void testProducerPerformance() throws Exception {
        recordCount.set(0);
        System.err.println("USAGE: java " + ProducerPerformance.class.getName() +
                    " topic_name num_records record_size target_records_sec [prop_name=prop_value]*");
        System.out.println(PREFIX + Utils.getDateString(System.currentTimeMillis()) + " : start performance test");

        /* parse args */
        String topicName = topic;
        long numRecords = maxCount;
        int recordSize = 100;
        int throughput = -1;

        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);

        /* setup perf test */
        byte[] payload = new byte[recordSize];
        Arrays.fill(payload, (byte) 1);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topicName, payload);
        //long sleepTime = NS_PER_SEC / throughput;
        //long sleepDeficitNs = 0;
        for (int i = 0; i < numRecords; i++) {
            long sendStart = System.currentTimeMillis();
            //Callback cb = stats.nextCompletion(sendStart, payload.length, stats);
            producer.send(record, null);
            recordCount.incrementAndGet();

            /*
             * Maybe sleep a little to control throughput. Sleep time can be a bit inaccurate for times < 1 ms so
             * instead of sleeping each time instead wait until a minimum sleep time accumulates (the "sleep deficit")
             * and then make up the whole deficit in one longer sleep.
             */
//            if (throughput > 0) {
//                sleepDeficitNs += sleepTime;
//                if (sleepDeficitNs >= MIN_SLEEP_NS) {
//                    long sleepMs = sleepDeficitNs / 1000000;
//                    long sleepNs = sleepDeficitNs - sleepMs * 1000000;
//                    Thread.sleep(sleepMs, (int) sleepNs);
//                    sleepDeficitNs = 0;
//                }
//            }
        }

        /* print final results */
        producer.close();
        //stats.printTotal();

        System.out.println(PREFIX + Utils.getDateString(System.currentTimeMillis()) + " : end performance test : " +
                recordCount.get());
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
