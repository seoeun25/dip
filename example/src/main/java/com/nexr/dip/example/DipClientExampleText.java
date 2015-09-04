package com.nexr.dip.example;

import com.nexr.dip.DipException;
import com.nexr.dip.client.DipClient;
import com.nexr.dip.conf.Configurable;
import com.nexr.dip.record.DipRecordBase;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.Properties;

/**
 * Example of the DipClient usage that send the message of which format is text.
 */
public class DipClientExampleText {

    private DipClient client;

    private String topic;

    public DipClientExampleText(String baseUrl, String topic, Properties conf) throws DipException {

        this.topic = topic;
        client = new DipClient(baseUrl, topic, DipClient.MESSAGE_TYPE.TEXT, conf);
        client.start();
    }

    public static void main(String... args) {

        String baseUrl = "localhost:9092";
        String topic = "hello";
        String schemaRegistryUrl = "http://localhost:18181/schemarepo";
        if (args.length != 0) {
            baseUrl = args[0];
        }
        Properties conf = new Properties();
        conf.put(Configurable.SCHEMAREGIDTRY_URL, schemaRegistryUrl);

        DipClientExampleText example = null;
        try {
            example = new DipClientExampleText(baseUrl, topic, conf);

            String srcInfo = "hello";
            example.sendTextFormatData(srcInfo);

        } catch (Exception e) {
            e.printStackTrace();
        }

        if (example != null) {
            example.close();
        }
    }

    public void sendTextFormatData(String srcInfo) {

        long time = System.currentTimeMillis();

        for (int i = 0; i < 10; i++) {
            String msg = "==00==" + i + "==hi|" + "bbb|" + "ccc|" + i;

            GenericRecord record = new GenericData.Record(DipClient.TEXT_FORMAT_SCHEMA);
            record.put(DipRecordBase.MESSAGE_FIELD, msg);
            record.put(DipRecordBase.EVENTTIME_FIELD, String.valueOf(time));

            try {
                DipRecordBase<String> dipRecordBase = new DipRecordBase(srcInfo, record, DipClient.MESSAGE_TYPE.TEXT);
                client.send(dipRecordBase);
            } catch (DipException e) {
                e.printStackTrace();
            }
        }
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
