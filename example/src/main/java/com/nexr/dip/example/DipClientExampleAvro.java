package com.nexr.dip.example;

import com.nexr.dip.DipException;
import com.nexr.dip.client.DipClient;
import com.nexr.dip.conf.Configurable;
import com.nexr.dip.record.DipRecordBase;
import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DipClientExampleAvro {

    private DipClient client;

    public DipClientExampleAvro(String baseUrl, Properties conf) throws DipException {

        client = new DipClient(baseUrl, DipClient.MESSAGE_TYPE.AVRO, conf);
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

        DipClientExampleAvro example = null;
        try {
            example = new DipClientExampleAvro(baseUrl, conf);

            String srcInfo = "employee";
            example.printSchema(srcInfo);
            example.createTestData(srcInfo);
            Thread.sleep(100);
            example.createTestData2(srcInfo);
            Thread.sleep(100);
            example.createTestData3(srcInfo);
            Thread.sleep(100);
            example.createTestData4(srcInfo);

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

    public void createTestData(String srcInfo) {
        Map<String, Object> fieldData = new HashMap<String, Object>();

        List<Schema.Field> fieldList = client.getSchemaFields(srcInfo);

        for (Schema.Field field : fieldList) {
            if (field.name().equals("wrk_dt")) {
                fieldData.put("wrk_dt", Long.valueOf(System.currentTimeMillis()));
            } else if (field.name().equals("src_info")) {
                fieldData.put("src_info", "gpx_port");
            } else if (field.name().equals("nescode")) {
                fieldData.put("nescode", "nescode-1-" + System.currentTimeMillis());
            } else if (field.name().equals("port")) {
                fieldData.put("port", "1010");
            } else if (field.name().equals("equip_ip")) {
                fieldData.put("equip_ip", "ip:211.211.211.211");
            } else {
                fieldData.put(field.name(), null);
            }
        }
        try {
            client.send(srcInfo, fieldData);
        } catch (DipException e) {
            e.printStackTrace();
        }
    }

    public void createTestData2(String srcInfo) {
        Map<String, Object> fieldData = new HashMap<String, Object>();
        fieldData.put("nescode", "nescode-2-" + System.currentTimeMillis());
        fieldData.put("port", "1010");
        fieldData.put("setup_val", "hahahoho");
        fieldData.put("nego", "hello");
        fieldData.put("setup_speed", "speed-2");
        fieldData.put("curnt_speed", "speed-current");
        fieldData.put("mac_cnt", null);
        fieldData.put("downl_speed_val", null);
        fieldData.put("etc_extrt_info", null);
        fieldData.put("wrk_dt", Long.valueOf(System.currentTimeMillis()));
        fieldData.put("src_info", srcInfo);
        fieldData.put("equip_ip", "dip-ip");


        try {
            client.send(srcInfo, fieldData);
        } catch (DipException e) {
            e.printStackTrace();
        }
    }

    public void createTestData3(String srcInfo) {

        Map<String, Object> fieldData = new HashMap<String, Object>();
        fieldData.put("nescode", "nescode-3-" + System.currentTimeMillis());
        fieldData.put("port", "1010");
        fieldData.put("setup_val", "hahahoho-333");
        fieldData.put("nego", "hello");
        fieldData.put("setup_speed", "speed-3");
        fieldData.put("curnt_speed", "speed-current");
        fieldData.put("mac_cnt", null);
        fieldData.put("downl_speed_val", null);
        fieldData.put("etc_extrt_info", null);
        fieldData.put("wrk_dt", Long.valueOf(System.currentTimeMillis()));
        fieldData.put("src_info", srcInfo);
        fieldData.put("equip_ip", "dip-ip");

        DipRecordBase dipRecordBase = new DipRecordBase(client.getSchema(srcInfo), fieldData);
        try {
            client.send(dipRecordBase);
        } catch (DipException e) {
            e.printStackTrace();
        }
    }

    public void createTestData4(String srcInfo) {

        DipRecordBase dipRecordBase = new DipRecordBase(client.getSchema(srcInfo));

        Map<String, Object> fieldData = new HashMap<String, Object>();

        for (int i = 0; i < 10; i++) {
            fieldData = new HashMap<String, Object>();
            fieldData.put("name", "seoeun 01-" + System.currentTimeMillis());
            fieldData.put("favorite_number", 11);
            fieldData.put("favorite_color", "ggggg-44");
            fieldData.put("event_time", Long.valueOf(System.currentTimeMillis()));

            dipRecordBase.putFields(fieldData);
            try {
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
