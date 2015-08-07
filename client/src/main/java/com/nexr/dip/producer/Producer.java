package com.nexr.dip.producer;

import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageEncoder;
import com.nexr.dip.DipException;
import com.nexr.dip.conf.Configurable;
import com.nexr.dip.conf.Context;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * DipClient that publishes to the kafka cluster.
 */
public class Producer implements Configurable {

    private MessageProducer messageProducer;

    private PRODUCER_TYPE type = PRODUCER_TYPE.simple;
    private Context context;

    private Integer partition = new Integer(1);
    private String key = "key";

    public static MessageProducer createMessageProducer(Context context, PRODUCER_TYPE type) throws DipException {
        MessageProducer messageProducer = null;
        if (type.equals(PRODUCER_TYPE.avro)) {
            messageProducer = new AvroProducer(context);
        } else if (type.equals(PRODUCER_TYPE.avrocamus)) {
            messageProducer = new AvroCamusProducer(context);
        } else {
            messageProducer = new SimpleProducer(context);
        }
        return messageProducer;
    }

    @Override
    public void configure(Context context) throws DipException {
        this.context = context;
        messageProducer = createMessageProducer(context, type);
    }

    /**
     * Send a data to a topic.
     *
     * @param topic The topic the data will be appended to.
     * @param data  The data will be sent
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws DipException
     */
    public void send(String topic, Object data) throws ExecutionException, InterruptedException, DipException {
        messageProducer.send(messageProducer.createRecord(topic, partition, key, data));
    }

    public void close() {
        messageProducer.close();
    }


    public static enum PRODUCER_TYPE {
        simple,
        avro,
        avrocamus;
    }

    public static abstract class MessageProducer<K, V> {

        protected KafkaProducer producer;
        protected Properties properties;
        protected boolean sync;
        protected Senders.Sender sender;
        protected Senders.SenderCallback failedCallback = new Senders.SenderCallback();

        public MessageProducer(Context context) throws DipException {
            this(context, false);
        }

        public MessageProducer(Context context, boolean sync) throws DipException {
            try {
                properties = context.getAll();
                producer = createKafkaProduer(properties);
                this.sync = sync;
                if (sync) {
                    sender = Senders.Sender.BlockingSender;
                } else {
                    sender = Senders.Sender.AsyncSender;
                }
            } catch (Exception e) {
                throw new DipException("Fail to create Producer : " + e.getMessage(), e);
            }
        }

        public KafkaProducer createKafkaProduer(Properties properties) {
            return new KafkaProducer<K, V>(properties);
        }

        abstract public ProducerRecord<K, V> createRecord(String topic, Integer partition, K key, Object data) throws DipException;

        public void send(ProducerRecord<K, V> record) throws DipException {
            sender.send(producer, record, failedCallback);
        }

        public List<PartitionInfo> partionsFor(String topic) {
            return producer.partitionsFor(topic);
        }

        public void close() {
            producer.close();
        }
    }

    public static class SimpleProducer extends MessageProducer<String, String> {

        public SimpleProducer(Context context) throws DipException {
            this(context, false);
        }

        public SimpleProducer(Context context, boolean sync) throws DipException {
            super(context, sync);
        }

        public String convertData(Object data) throws DipException {
            return data.toString();
        }

        @Override
        public ProducerRecord<String, String> createRecord(String topic, Integer partition, String key, Object data) throws
                DipException {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, partition, key, convertData(data));
            return record;
        }

    }

    public static class AvroProducer extends MessageProducer<String, byte[]> {

        public AvroProducer(Context context) throws DipException {
            this(context, false);
        }

        public AvroProducer(Context context, boolean sync) throws DipException {
            super(context, sync);
        }

        public byte[] convertData(Object data) throws DipException {
            if (data instanceof GenericRecord) {
                return datumToByteArray((GenericRecord) data);
            }
            return data.toString().getBytes();
        }

        public byte[] datumToByteArray(GenericRecord datum) throws DipException {
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(datum.getSchema());
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            try {
                Encoder e = EncoderFactory.get().binaryEncoder(os, null);
                writer.write(datum, e);
                e.flush();
                byte[] byteData = os.toByteArray();
                return byteData;
            } catch (IOException e) {
                throw new DipException("Fail to convert datum ", e);
            } finally {
                try {
                    os.close();
                } catch (IOException e) {

                }
            }
        }

        @Override
        public ProducerRecord<String, byte[]> createRecord(String topic, Integer partition, String key, Object data) throws
                DipException {
            byte[] dataBytes = convertData(data);
            ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, partition, key, dataBytes);
            return record;
        }

    }

    public static class AvroCamusProducer extends MessageProducer<String, byte[]> {
        private Map<String, KafkaAvroMessageEncoder> encoderMap;

        public AvroCamusProducer(Context context) throws DipException {
            this(context, false);
        }

        public AvroCamusProducer(Context context, boolean sync) throws DipException {
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

    }


}
