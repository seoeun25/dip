package com.nexr.dip.client;

import com.nexr.dip.DipException;
import com.nexr.dip.common.Utils;
import com.nexr.dip.conf.Context;
import com.nexr.dip.producer.Producer;
import com.nexr.dip.record.DipRecordBase;
import junit.framework.Assert;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;

public class DipClientTextTest {

    //private static final byte DELIMITER_BYTE = 0x0;
    private static final String DELIMITER = "&";
    private static DipClient<String> dipClient;
    private static String topic = "hello";

    @BeforeClass
    public static void setupClass() {

        String baseUrl = "localhost:9092";
        Properties properties = getProperteis();
        try {
            dipClient = new DipClient(baseUrl, topic, DipClient.MESSAGE_TYPE.TEXT, properties);
            DummySchemaRegistry schemaRegistry = new DummySchemaRegistry();
            schemaRegistry.init(properties);
            Context context = new Context();
            context.putAll(getProperteis());
            dipClient.start(Producer.createMessageProducer(context, Producer.PRODUCER_TYPE.simple), schemaRegistry);
        } catch (DipException e) {
            e.printStackTrace();
        }
    }

    private static Properties getProperteis() {
        String schemaRegistryClass = "com.nexr.dip.client.DummySchemaRegistry";
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "false");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //properties.put(Configurable.SCHEMAREGISTRY_CLASS, schemaRegistryClass); //com.nexr.schemaregistry.AvroSchemaRegistry
        //properties.put(Configurable.SCHEMAREGIDTRY_URL, "http://localhost:18181/avro-repo");
        //properties.put(KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS, schemaRegistryClass);
        return properties;
    }

    @Test
    public void sendTextFormatTest() {
        long time = getTime(2015, 10, 17, 20, 30);
        //long time = System.currentTimeMillis();

        String srcInfo = "hello";

        for (int i = 0; i < 10; i++) {
            String msg = "=55=" + i + "==hi|" + "bbb|" + "ccc|" + i;

            GenericRecord record = new GenericData.Record(DipClient.TEXT_FORMAT_SCHEMA);
            record.put(DipRecordBase.MESSAGE_FIELD, msg);
            record.put(DipRecordBase.EVENTTIME_FIELD, String.valueOf(time));

            DipRecordBase<String> dipRecordBase = new DipRecordBase(srcInfo, record, DipClient.MESSAGE_TYPE.TEXT);
            try {
                dipClient.send(dipRecordBase);
                Thread.sleep(100);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Send the string as is, not added timestamp.
     */
    @Test
    public void sendStringAsisTest() {

        String srcInfo = "sip";

        for (int i = 0; i < 10; i++) {
            long time = System.currentTimeMillis();
            String timeLable = Utils.formatTime(time, "yyyy-MM-dd", "UTC");
            timeLable = timeLable + "T" + Utils.formatTime(time, "hh:mm:ss", "UTC") + "Z";
            String timeIp = Utils.formatTime(time, "hh.mm", "UTC");

            timeIp = String.valueOf((i + 1) % 10) + "." + timeIp;
            int packet = 14 + (i % 10) ;
            String msg = "{\"timestamp\": \"" + timeLable + "\", \"sip\": \"a" + timeIp + "\", \"packet_total\": \"" + packet +
                    "\"}";

//            if ((i + 1) % 2 == 0) {
//                msg = "{\"timestamp\": \"" + timeLable + "\", \"sip\": \"a." + timeIp + "\", \"packet_total\": \"14\"}";
//            } else {
//                msg = "{\"timestamp\": \"" + timeLable + "\", \"sip\": \"b." + timeIp + "\", \"packet_total\": \"10\"}";
//            }

            GenericRecord record = new GenericData.Record(DipClient.TEXT_FORMAT_SCHEMA);
            record.put(DipRecordBase.MESSAGE_FIELD, msg);

            DipRecordBase<String> dipRecordBase = new DipRecordBase(srcInfo, record, DipClient.MESSAGE_TYPE.TEXT);
            try {
                dipClient.send(dipRecordBase);
                Thread.sleep(100);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testSip() {

        try {
            for (int i = 0; i < 100; i++) {
                sendStringAsisTest();
                Thread.sleep(1000 * 60);
                System.out.println("---- send test sip data : " + i);
            }
        } catch (Exception e) {
            e.printStackTrace();
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
    public void testByte() {

        String timeStr = String.valueOf(System.currentTimeMillis());
        System.out.println("--- timeStr : " + timeStr);
        String msg = "seoeun&abc";
        String message = timeStr + DELIMITER + msg;
        System.out.println("message  : " + message);
        long timestamp = 0;
        String payloadString;

        try {
            payloadString = new String(message.getBytes(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            System.out.println("Unable to load UTF-8 encoding, falling back to system default");
            payloadString = new String(message.getBytes());
        }

        if (payloadString.indexOf(DELIMITER, 0) != -1) {
            int index = payloadString.indexOf(DELIMITER, 0);
            String timeStrResult = payloadString.substring(0, index);
            System.out.println("--- timeStr : " + timeStrResult);
            String m = payloadString.substring(index + 1, payloadString.length());
            System.out.println("---- m : " + m);

            Assert.assertEquals(timeStr, timeStrResult);
            Assert.assertEquals(msg, m);

        }
        timestamp = System.currentTimeMillis();

    }

}
