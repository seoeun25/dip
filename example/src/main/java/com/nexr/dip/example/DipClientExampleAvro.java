package com.nexr.dip.example;

import com.nexr.dip.DipException;
import com.nexr.dip.client.DipClient;
import com.nexr.dip.conf.Configurable;
import com.nexr.dip.record.DipRecordBase;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.Properties;

/**
 * Example of the DipClient usage that send the message of which format is avro.
 */
public class DipClientExampleAvro {

    private DipClient client;
    private String topic;

    public DipClientExampleAvro(String baseUrl, String topic, Properties conf) throws DipException {

        this.topic = topic;
        client = new DipClient(baseUrl, topic, DipClient.MESSAGE_TYPE.AVRO, conf);
        client.start();
    }

    public static void main(String... args) {

        String baseUrl = "localhost:9092";
        String topic = "employee";
        String schemaRegistryUrl = "http://localhost:18181/repo";
        if (args.length != 0) {
            baseUrl = args[0];
        }
        Properties conf = new Properties();
        conf.put(Configurable.SCHEMAREGIDTRY_URL, schemaRegistryUrl);

        DipClientExampleAvro example = null;
        try {
            example = new DipClientExampleAvro(baseUrl, topic, conf);

            String srcInfo = "employee";
            Thread.sleep(100);
            example.sendAvroFormatData(srcInfo);

        } catch (Exception e) {
            e.printStackTrace();
        }

        if (example != null) {
            example.close();
        }
    }

    public void printSchema(String srcInfo) {
        System.out.println(client.getSchema(srcInfo));
        System.out.println("------------------");
        System.out.println(client.getSchemaFields(srcInfo));
    }

    public void sendAvroFormatData(String srcInfo) {

        Schema schema = client.getSchema(srcInfo);

        for (int i = 0; i < 10; i++) {
            GenericRecord record = new GenericData.Record(schema);
            record.put("name", i + "::seoeun-33-hello-azrael");
            record.put("favorite_number", String.valueOf(i));
            record.put("wrk_dt", System.currentTimeMillis());
            record.put("src_info", "employee");

            try {
                DipRecordBase<GenericRecord> dipRecordBase = new DipRecordBase(srcInfo, record, DipClient.MESSAGE_TYPE.AVRO);
                client.send(dipRecordBase);
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace(System.out);
            }
        }

    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
