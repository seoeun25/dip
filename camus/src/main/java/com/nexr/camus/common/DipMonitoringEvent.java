package com.nexr.camus.common;

import com.linkedin.camus.etl.kafka.common.AbstractMonitoringEvent;
import com.linkedin.camus.etl.kafka.common.Source;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class DipMonitoringEvent extends AbstractMonitoringEvent{

    private static Logger log = Logger.getLogger(DipMonitoringEvent.class);

    private String jobId = "";

    public static final String count_schema = "{\"namespace\": \"com.nexr.dip.avro.schema\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"count\",\n" +
            "  \"fields\": [\n" +
            "      {\"name\": \"topic\", \"type\": \"string\"},\n" +
            "      {\"name\": \"jobid\",  \"type\": [\"string\", \"null\"]},\n" +
            "      {\"name\": \"count\", \"type\": \"long\"},\n" +
            "      {\"name\": \"server\",  \"type\": [\"string\", \"null\"]},\n" +
            "      {\"name\": \"service\", \"type\": [\"string\", \"null\"]},\n" +
            "      {\"name\": \"start\", \"type\": \"long\"}\n" +
            "  ]\n" +
            " }";


    public DipMonitoringEvent(Configuration config) {
        super(config);

        // mapreduce.job.dir=/tmp/hadoop-yarn/staging/seoeun/.staging/job_1441688832620_0089
        String jobDir = config.get("mapreduce.job.dir");
        if (jobDir != null) {
            String[] dirs = jobDir.split("/");
            jobId = dirs[dirs.length -1];
        }

    }

    @Override
    public GenericRecord createMonitoringEventRecord(Source countEntry, String topic, long granularity, String tier) {
        long count = countEntry.getCount();
        String server = countEntry.getServer();
        String service = countEntry.getService();
        long start = countEntry.getStart();

        Schema schema = Schema.parse(count_schema);
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set("topic", topic);
        builder.set("jobid", jobId);
        builder.set("count", count);
        builder.set("server", server);
        builder.set("service", service);
        builder.set("start", start);
        GenericRecord record = builder.build();

        return record;
    }
}
