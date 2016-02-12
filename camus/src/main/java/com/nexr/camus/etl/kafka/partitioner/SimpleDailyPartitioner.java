package com.nexr.camus.etl.kafka.partitioner;

import com.linkedin.camus.etl.kafka.partitioner.BaseTimeBasedPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTimeZone;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat.ETL_DEFAULT_TIMEZONE;
import static com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat.ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY;

public class SimpleDailyPartitioner extends BaseTimeBasedPartitioner {

    private static final String DEFAULT_TOPIC_SUB_DIR = "daily";

    public static final String SEOUL_TIME_ZONE = "Asia/Seoul";

    @Override
    public void setConf(Configuration conf) {
        if (conf != null) {
            String destPathTopicSubDir = conf.get(ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY, DEFAULT_TOPIC_SUB_DIR);
            DateTimeZone outputTimeZone = DateTimeZone.forID(conf.get(ETL_DEFAULT_TIMEZONE, SEOUL_TIME_ZONE));

            long outfilePartitionMs = TimeUnit.HOURS.toMillis(24);
            String destSubTopicPathFormat = "'" + destPathTopicSubDir + "'/YYYYMMdd";
            init(outfilePartitionMs, destSubTopicPathFormat, Locale.KOREA, outputTimeZone);
        }

        super.setConf(conf);
    }
}
