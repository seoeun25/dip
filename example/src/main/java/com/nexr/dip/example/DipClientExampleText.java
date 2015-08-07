package com.nexr.dip.example;

import com.nexr.dip.DipException;
import com.nexr.dip.client.DipClient;
import com.nexr.dip.conf.Configurable;
import com.nexr.dip.record.DipRecordBase;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DipClientExampleText {

    private DipClient client;

    public DipClientExampleText(String baseUrl, Properties conf) throws DipException {

        client = new DipClient(baseUrl, DipClient.MESSAGE_TYPE.TEXT, conf);
        client.start();
    }

    public static void main(String... args) {

        String baseUrl = "localhost:9092";
        String schemaRegistryUrl = "http://localhost:18181/repo";
        if (args.length != 0) {
            baseUrl = args[0];
        }
        Properties conf = new Properties();
        conf.put(Configurable.SCHEMAREGIDTRY_URL, schemaRegistryUrl);

        DipClientExampleText example = null;
        try {
            example = new DipClientExampleText(baseUrl, conf);

            String srcInfo = "hello";
            example.sendTextFormatTest(srcInfo);

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

    public void sendTextFormatTest(String srcInfo) {

        DipRecordBase dipRecordBase = new DipRecordBase(srcInfo);
        Map<String, Object> fieldData = new HashMap<>();

        String msg = "az|" + "bbb|" + "ccc|" + System.currentTimeMillis();

        dipRecordBase.put(DipRecordBase.MESSAGE_FIELD, msg);
        dipRecordBase.put(DipRecordBase.EVENTTIME_FIELD, System.currentTimeMillis());

        try {
            client.send(dipRecordBase);
        } catch (DipException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
