package com.nexr.camus.etl.kafka.coders;

import com.linkedin.camus.coders.MessageEncoder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.log4j.Logger;

public class StringMessageEncoder extends MessageEncoder<IndexedRecord, byte[]>{

    private static final Logger logger = Logger.getLogger(StringMessageEncoder.class);

    public static final String count_schema = "{\"namespace\": \"com.nexr.dip.avro.schema\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"count\",\n" +
            "  \"fields\": [\n" +
            "      {\"name\": \"name\", \"type\": \"string\"},\n" +
            "      {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
            "      {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]},\n" +
            "      {\"name\": \"wrk_dt\", \"type\": \"long\"}\n" +
            "  ]\n" +
            " }";

    @Override
    public byte[] toBytes(IndexedRecord record) {
        return record.toString().getBytes();
    }
}
