package com.nexr.dip.server;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.nexr.dip.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DipContext extends Context{

    public static final String AVRO_TOPICS = "dip.avro.topics";
    public static final String TEXT_TOPICS = "dip.text.topics";
    public static final String DIP_HADOOP_CONF = "dip.hadoop.conf.dir";
    public static final String DIP_NAMENODE = "dip.namenode";
    public static final String DIP_JOBTRACKER = "dip.jobtracker";
    public static final String DIP_HIVESERVER = "dip.hiveserver";
    public static final String DIP_HIVESERVER_USER = "dip.hiveserver.user";
    public static final String DIP_HIVESERVER_PASSWD = "dip.hiveserver.passwd";
    public static final String DIP_OOZIE = "dip.oozie";
    public static final String DIP_KAFKA_BROKER = "dip.kafka.broker";
    public static final String DIP_USER_NAME = "dip.user.name";


    // etl (camus)
    public static final String DIP_ETL_COUNT_DIR = "dip.etl.count.dir";
    public static final String DIP_ETL_DESTINATION_PATH = "dip.etl.destination.path";
    public static final String DIP_ETL_EXECUTION_BASE_PATH = "dip.etl.execution.base.path";
    public static final String DIP_ETL_EXECUTION_HISTORY_PATH = "dip.etl.execution.history.path";
    public static final String DIP_KAFKA_PULL_SIZE = "dip.kafka.max.pull.size";

    public static final String DIP_WF_MONITOR_INTERVAL = "dip.wf.monitor.interval";
    public static final String DIP_PARTITION_CREATE_DDL = "dip.hive.partition.createddl";
    public static final String DIP_SCHEDULE_RETRY_MAX = "dip.schedule.retry.max";

    private static Logger LOG = LoggerFactory.getLogger(DipContext.class);
    private static DipContext dipContext;

    private Context context;

    @Inject
    public DipContext(@Named("siteConfig") String siteConfig, @Named("defaultConfig") String defaultConfig) {
        super(siteConfig, defaultConfig);
    }

}
