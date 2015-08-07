package com.nexr.dip.record;

import com.nexr.dip.client.DipClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.List;
import java.util.Map;

/**
 * Record base for the DIP.
 * @param <D> the type of message. <code>String</code> for the text format, <code>GenericRecord</code>
 */
public class DipRecordBase<D> {

    public static final String DELIMITER = "&";
    public static final String EVENTTIME_FIELD = "wrk_dt";
    public static final String MESSAGE_FIELD = "message";
    public static final String HEADER_FIELD = "header";

    private DipClient.MESSAGE_TYPE messageType;
    private String srcInfo;
    private Schema schema;
    private GenericRecord record;
    private Schema headerSchema;

    /**
     * Construct the DipRecordBase.
     * @param srcInfo a srcinfo
     * @param record a record to be sent
     * @param messageType a message type. One of the following
     *                    <ul>
     *                    <li>DipClient.MESSAGE_TYPE.AVRO</li>
     *                    <li>DipClient.MESSAGE_TYPE.TEXT</li>
     *                    </ul>
     */
    public DipRecordBase(String srcInfo, GenericRecord record, DipClient.MESSAGE_TYPE messageType) {
        this.schema = record.getSchema();
        this.srcInfo = srcInfo;
        this.messageType = messageType;
        if (messageType == DipClient.MESSAGE_TYPE.AVRO) {
            headerSchema = schema.getField(HEADER_FIELD).schema();
        }
        setRecord(record);
    }

    private void setRecord(GenericRecord record) {
        if (headerSchema != null) {
            GenericRecord header = new GenericData.Record(headerSchema);
            header.put("time", record.get(EVENTTIME_FIELD));
            record.put("header", header);
        }
        this.record = record;
    }

    private GenericRecord getRecord() {
        return record;
    }

    /**
     * Gets the data of this record.
     * @return
     */
    public D getData() {
        if (messageType == DipClient.MESSAGE_TYPE.AVRO) {
            return (D) record;
        } else {
            String msg = "";
            if (getRecord().get(DipRecordBase.EVENTTIME_FIELD) != null) {
                msg = getRecord().get(DipRecordBase.EVENTTIME_FIELD).toString() + DipRecordBase.DELIMITER;
            }
            msg = msg + getRecord().get(DipRecordBase.MESSAGE_FIELD).toString();
            return (D) msg;
        }
    }

    @Deprecated
    public static GenericRecord build(Schema schema, Map<String, Object> fields) {
        long eventTime = Long.valueOf(fields.get(EVENTTIME_FIELD).toString());
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        List<Schema.Field> fieldList = schema.getFields();
        for (Schema.Field field : fieldList) {
            if (field.name().equals("header")) {
                GenericRecord headerRecord = getHeader(schema, eventTime);
                builder.set(field, headerRecord);
            } else {
                builder.set(field, fields.get(field.name()));
            }
        }
        return builder.build();
    }

    @Deprecated
    private static GenericRecord getHeader(Schema schema, long eventTime) {
        Schema headerSchema = schema.getField("header").schema();
        GenericRecord header = new GenericData.Record(headerSchema);
        header.put("time", Long.valueOf(eventTime));
        return header;
    }

    public Schema getSchema() {
        return schema;
    }

    public String getSrcInfo() {
        return srcInfo;
    }

    public DipClient.MESSAGE_TYPE getMessageType() {
        return messageType;
    }

    /**
     * Gets the fields of the schema for a specific topic.
     *
     * @return schema fields. If not schema exists, an unchecked SchemaNotFoundException will be thrown.
     */
    public List<Schema.Field> getSchemaFields() {
        return schema.getFields();
    }

}
