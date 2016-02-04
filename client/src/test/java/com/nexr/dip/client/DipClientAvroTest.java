package com.nexr.dip.client;

import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageEncoder;
import com.nexr.dip.DipException;
import com.nexr.dip.conf.Configurable;
import com.nexr.dip.conf.Context;
import com.nexr.dip.producer.Producer;
import com.nexr.dip.record.DipRecordBase;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;

public class DipClientAvroTest {

    private static DipClient dipClient;

    @BeforeClass
    public static void setupClass() {

        String baseUrl = "localhost:9092";
        String topic = "employee";
        Properties properties = getProperteis();
        try {
            dipClient = new DipClient(baseUrl, topic, DipClient.MESSAGE_TYPE.AVRO, properties);
            //DummySchemaRegistry schemaRegistry = new DummySchemaRegistry();
            //schemaRegistry.init(properties);
            Context context = new Context();
            context.putAll(properties);
            //dipClient.start(schemaRegistry);
            //dipClient.start(Producer.PRODUCER_TYPE.simple.toString(), schemaRegistry);
            dipClient.start();
        } catch (DipException e) {
            e.printStackTrace();
        }
    }

    private static Properties getProperteis() {
        String schemaRegistryClass = "com.nexr.dip.client.DummySchemaRegistry";
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "false");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //properties.put(Configurable.SCHEMAREGISTRY_CLASS, schemaRegistryClass); //com.nexr.schemaregistry.AvroSchemaRegistry
        properties.put(Configurable.SCHEMAREGIDTRY_URL, "http://localhost:18181/repo");
        properties.put(KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS, schemaRegistryClass);
        return properties;
    }

    @Test
    public void testSchemFields() {
        String origin = "employee";
        Schema schema = dipClient.getSchema(origin);
        System.out.println("---- schema : " + schema);
        System.out.println("--- fieldNames : " + dipClient.getSchemaFields(origin).toString());

    }

    @Test
    public void sendRecordTest() {
        long time = getTime(2015, 10, 15, 20, 30);
        //long time = System.currentTimeMillis();

        String topic = "employee";
        Schema schema = dipClient.getSchema(topic);
        System.out.println(schema);

        long start = System.currentTimeMillis();
        for (int i = 0; i < 3000; i++) {
            GenericRecord record = new GenericData.Record(schema);
            record.put("name", i + "::seoeun-33-hello-azrael");
            record.put("favorite_number", String.valueOf(i));
            record.put("wrk_dt", time);
            record.put("srcinfo", topic);

            DipRecordBase<GenericRecord> dipRecordBase = new DipRecordBase(topic, record, DipClient.MESSAGE_TYPE.AVRO);

            try {
                dipClient.send(dipRecordBase);
                //Thread.sleep(100);
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
        }
        System.out.println("period : " + String.valueOf(System.currentTimeMillis() - start));
    }

    @Test
    public void testKey() throws Exception{
        Thread.sleep(1000);
        for (int i = 0; i < 20; i++) {
            long current = System.currentTimeMillis();
            String key = dipClient.getKey(current);
            System.out.println("current : " + current + " , key : " + key);

            int partition = dipClient.getPartition(current);
            System.out.println("current : " + current + " , partition : " + partition);

            Thread.sleep(1000);
        }
    }

    private long getTime(int year, int month, int day, int hour, int minute) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, month);
        calendar.set(Calendar.DAY_OF_MONTH, day);
        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.set(Calendar.MINUTE, minute);
        return calendar.getTimeInMillis();
    }

    @Test
    public void dateTest() {
        System.out.println(getDateString(1439083382197l));
    }

    public String getDateString(long time) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        String stime = format.format(calendar.getTime());
        return stime;
    }

    @Test
    public void testReadFile() throws IOException {
        System.out.println("testReadFile");
        RandomAccessFile aFile = new RandomAccessFile("portinfo.dat", "r");
        FileChannel inChannel = aFile.getChannel();
        ByteBuffer buffer = ByteBuffer.allocate(512);

        Charset charset = Charset.forName("UTF-8");
        int readByte = 0;
        int lineByte = 0;
        while((readByte = inChannel.read(buffer)) > 0) {
            //System.out.println("\nreadByte : " + readByte);
            buffer.flip();
            for (int i = 0; i < buffer.limit(); i++) {
                byte b = buffer.get();
                System.out.print((char) b );
                lineByte++;
                if (b == '\n') {
                    System.out.println("lineByte : " + lineByte);
                    lineByte = 0;
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
