package com.nexr.dip.client;

import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageEncoder;
import com.nexr.dip.DipException;
import com.nexr.dip.conf.Context;
import com.nexr.dip.producer.Producer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class DummyMessageProducer extends Producer.MessageProducer<String, byte[]> {
    private Map<String, KafkaAvroMessageEncoder> encoderMap;

    public DummyMessageProducer(Context context) throws DipException {
        this(context, true);
    }

    public DummyMessageProducer(Context context, boolean sync) throws DipException {
        super(context, sync);
        encoderMap = new HashMap<String, KafkaAvroMessageEncoder>();
    }

    private KafkaAvroMessageEncoder getAvroCamusEncoder(String topic) {
        KafkaAvroMessageEncoder encoder = encoderMap.get(topic);
        if (encoder == null) {
            encoder = new KafkaAvroMessageEncoder(topic, null);
            encoder.init(properties, topic);
            encoderMap.put(topic, encoder);
        }
        return encoder;
    }

    public byte[] convertData(String topic, Object data) throws DipException {
        if (data instanceof GenericRecord) {
            return datumToByteArray(topic, (GenericRecord) data);
        }
        return data.toString().getBytes();
    }

    public byte[] datumToByteArray(String topic, GenericRecord datum) throws DipException {
        return getAvroCamusEncoder(topic).toBytes(datum);
    }

    @Override
    public ProducerRecord<String, byte[]> createRecord(String topic, Integer partition, String key, Object data) throws
            DipException {
        byte[] dataBytes = convertData(topic, data);
        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, partition, key, dataBytes);
        return record;
    }

    @Override
    public void send(ProducerRecord<String, byte[]> record) throws DipException{
        record.value();
        //sender.send(producer, record, failedCallback);
        decode(record.topic(), record.value());
    }

    public void decode(String topic, byte[] bytes) {
        System.out.println("---- bytes: " + new String(bytes));
//        KafkaAvroMessageDecoder decoder = new KafkaAvroMessageDecoder();
//        decoder.init(properties, topic);
//
//
//        SchemaRegistry<Schema> schemaRegistry = new DummySchemaRegistry();
//        schemaRegistry.init(new Properties());
//
//        KafkaAvroMessageDecoder kafkaAvroMessageDecoder = new KafkaAvroMessageDecoder();
//        //final byte[] bytes = "whatever".getBytes();
//        bytes[0] = 0x0; // Magic byte
//        KafkaAvroMessageDecoder.MessageDecoderHelper messageDecoderHelper =
//                kafkaAvroMessageDecoder.new MessageDecoderHelper(schemaRegistry, topic, bytes);
//        KafkaAvroMessageDecoder.MessageDecoderHelper actualResult = messageDecoderHelper.invoke();
//
//        assertEquals("com.nexr.dip.avro.schema", actualResult.getSchema().getNamespace());
//        //assertEquals(5, actualResult.getStart());
//        assertEquals(bytes, actualResult.getBuffer().array());
//        //assertEquals(3, actualResult.getLength());

    }
}
