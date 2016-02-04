package com.nexr.dip.client;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageEncoder;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import com.nexr.dip.DipException;
import com.nexr.dip.conf.Configurable;
import com.nexr.dip.conf.Context;
import com.nexr.dip.producer.Producer;
import com.nexr.dip.record.DipRecordBase;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.nexr.dip.producer.Producer.MessageProducer;
import static com.nexr.dip.producer.Producer.PRODUCER_TYPE;
import static com.nexr.dip.producer.Producer.createMessageProducer;

/**
 * Client API to send message.
 * @param <D> the type of message. <code>String</code> for the text format, <code>GenericRecord</code> for the avro format.
 */
public class DipClient<D> implements Configurable {

    private static Logger LOG = LoggerFactory.getLogger(DipClient.class);

    private MESSAGE_TYPE messageType;
    private String baseUrl;
    private String srcInfo;
    private Context context;
    private int partitionCount;

    private MessageProducer messageProducer;
    private SchemaRegistry<Schema> schemaRegistry;

    private Integer partition = null;
    private String key = null;

    public static Schema TEXT_FORMAT_SCHEMA;

    static {
        String text_format_schema =  "{\"namespace\": \"com.nexr.dip.avro.schema\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"text\",\n" +
                "  \"fields\": [\n" +
                "      {\"name\": \"message\", \"type\": \"string\"},\n" +
                "      {\"name\": \"wrk_dt\", \"type\": \"long\"}\n" +
                "  ]\n" +
                " }";
        TEXT_FORMAT_SCHEMA = new Schema.Parser().parse(text_format_schema);
    }
    /**
     * A DipClient is instantiated by providing baseUrl and conf as configuration.
     *
     * @param baseUrl     a DipServer url, broker url
     * @param srcInfo     a topic
     * @param messageType a message type.
     *                    <ul>
     *                    <li>DipClient.MESSAGE_TYPE.TEXT - plain text</li>
     *                    <li>DipClient.MESSAGE_TYPE.AVRO - avro format</li>
     *                    </ul>
     * @param conf        a configuration
     * @throws DipException
     */
    public DipClient(String baseUrl, String srcInfo, DipClient.MESSAGE_TYPE messageType, Properties conf) throws DipException {
        this.baseUrl = baseUrl;
        this.srcInfo = srcInfo;
        this.messageType = messageType;
        Properties defaultProps = messageType.getDefaultConfig();
        for (String key : conf.stringPropertyNames()) {
            defaultProps.put(key, conf.getProperty(key));
        }
        Context context = new Context(defaultProps);
        context.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, baseUrl);
        configure(context);
    }

    /**
     * Start the client with avro format producer.
     *
     * @throws DipException
     */
    public void start() throws DipException {
        // TODO injection
        PRODUCER_TYPE producerType = messageType.getProducerType();
        System.out.println("[-0- Dip Client -0-] " + baseUrl + ", " + producerType);
        LOG.info("[--DipClient--] start with " + producerType);
        System.out.println("size : " + context.getString("batch.size") + " / " + context.getString("buffer.memory"));

        this.messageProducer = Producer.createMessageProducer(context, producerType);
        if (messageType == MESSAGE_TYPE.AVRO) {
            this.schemaRegistry = loadSchemaRegistry(context.getString(Configurable.SCHEMAREGISTRY_CLASS));
            this.schemaRegistry.init(context.getAll());
        }
        this.partitionCount = messageProducer.partionsFor(srcInfo).size();
        System.out.println("[Dip Client] started ......");
    }

    @VisibleForTesting
    void start(MessageProducer messageProducer, SchemaRegistry<Schema> schemaRegistry) throws DipException {
        this.messageProducer = messageProducer;
        this.schemaRegistry = schemaRegistry;
        this.partitionCount = messageProducer.partionsFor(srcInfo).size();
    }

    @VisibleForTesting
    void start(SchemaRegistry<Schema> schemaRegistry) throws DipException {
        // TODO injection
        this.messageProducer = createMessageProducer(context, PRODUCER_TYPE.avrocamus);
        this.schemaRegistry = schemaRegistry;
        this.partitionCount = messageProducer.partionsFor(srcInfo).size();
    }

    private SchemaRegistry<Schema> loadSchemaRegistry(String claz) throws DipException {
        try {
            Class<? extends SchemaRegistry<Schema>> klass = (Class<? extends SchemaRegistry<Schema>>) Class.forName(claz);
            return klass.newInstance();
        } catch (Exception e) {
            throw new DipException("Can not find shcemaRegistry instance : " + claz, e);
        }
    }

    /**
     * Send a dipRecord to a DipServer.
     *
     * @param dipRecordBase
     * @throws DipException
     */
    public void send(DipRecordBase<D> dipRecordBase) throws DipException {
        key = getKey(System.currentTimeMillis());
        send(dipRecordBase.getSrcInfo(), partition, key, dipRecordBase.getData());
    }

    /**
     * Send a data to a topic.
     *
     * @param srcInfo The topic the data will be appended to.
     * @param data    The data will be sent. The data should be <code>GenericRecord</code> for Avor format,
     *                otherwise <code>String</code>.
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws DipException
     */
    private void send(String srcInfo, Integer partition, String key, D data) throws DipException {
        // TODO error handle : SenderCallback
        messageProducer.send(messageProducer.createRecord(srcInfo, partition, key, data));
    }

    public void send(ProducerRecord producerRecord) throws DipException{
        messageProducer.send(producerRecord);
    }

    /**
     * Gets the schema that was written for a specific topic.
     *
     * @param srcInfo a topic name
     * @return a schema. If not schema exists, an unchecked SchemaNotFoundException will be thrown.
     */
    public Schema getSchema(String srcInfo) {
        return schemaRegistry.getLatestSchemaByTopic(srcInfo).getSchema();
    }

    /**
     * Gets the fields of the schema for a specific topic.
     *
     * @param srcInfo a topic name
     * @return schema fields. If not schema exists, an unchecked SchemaNotFoundException will be thrown.
     */
    public List<Schema.Field> getSchemaFields(String srcInfo) {
        return getSchema(srcInfo).getFields();
    }

    public byte[] getByteOf(String topic, GenericRecord record) throws DipException{
        ProducerRecord producerRecord = messageProducer.createRecord(topic, partition, key, record);
        if (producerRecord.value() instanceof byte[]) {
            return (byte[])producerRecord.value();
        }
        return null;
    }

    /**
     * Close the client adn disconnect to dip server.
     */
    public void close() {
        messageProducer.close();
    }

    @Override
    public void configure(Context context) {
        this.context = context;
    }

    public String getKey(Object timestamp) {
        String key = null;
//        int iKey = getPartition((Long)timestamp);
//        key = String.valueOf(iKey);
        return key;
    }

    public int getPartition(long timestamp) {
        int iKey = 0;
        String key = null;
        try {
            key = formatTime(timestamp, "ss");
        } catch (Exception e) {
            key = formatTime(System.currentTimeMillis(), "ss");
            LOG.warn("Fail to get key from timestamp : " + timestamp + " , use currentTime");
        }
        try {
            iKey = getKafkaBytePartition(key, partitionCount);
        } catch (Exception e) {

        }
        return iKey;
    }

    private int getKafkaBytePartition(String key, int numPartitions) {
        //Utils.abs(java.util.Arrays.hashCode(key.asInstanceOf[Array[Byte]])) % numPartitions
        int abs = Utils.abs(java.util.Arrays.hashCode(key.getBytes()));
        int partition = abs % numPartitions;
        return partition;
    }

    public static String formatTime(long timestamp, String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        return format.format(calendar.getTime());
    }

    public static enum MESSAGE_TYPE {
        TEXT {
            @Override
            public PRODUCER_TYPE getProducerType() {
                return PRODUCER_TYPE.simple;
            }

            public Properties getDefaultConfig() {
                Properties map = new Properties();
                map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                map.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "false");
                map.put(Configurable.SCHEMAREGISTRY_CLASS, "com.nexr.schemaregistry.AvroSchemaRegistry");
                map.put(KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS, "com.nexr.schemaregistry.AvroSchemaRegistry");
                map.put("partitioner.class", "kafka.producer.ByteArrayPartitioner");
                return map;
            }
        },
        AVRO {
            @Override
            public PRODUCER_TYPE getProducerType() {
                return PRODUCER_TYPE.avrocamus;
            }
        };

        public PRODUCER_TYPE getProducerType() {
            return PRODUCER_TYPE.avrocamus;
        }

        public Properties getDefaultConfig() {
            Properties map = new Properties();
            map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            map.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "false");
            map.put(ProducerConfig.ACKS_CONFIG, "all");
            map.put(Configurable.SCHEMAREGISTRY_CLASS, "com.nexr.schemaregistry.AvroSchemaRegistry");
            map.put(KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS, "com.nexr.schemaregistry.AvroSchemaRegistry");
            map.put("partitioner.class", "kafka.producer.ByteArrayPartitioner");
            return map;
        }
    }

}
