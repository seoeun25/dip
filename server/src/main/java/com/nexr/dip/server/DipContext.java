package com.nexr.dip.server;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class DipContext {

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

    // etl (camus)
    public static final String DIP_ETL_COUNT_DIR = "dip.etl.count.dir";
    public static final String DIP_ETL_DESTINATION_PATH = "dip.etl.destination.path";
    public static final String DIP_ETL_EXECUTION_BASE_PATH = "dip.etl.execution.base.path";
    public static final String DIP_ETL_EXECUTION_HISTORY_PATH = "dip.etl.execution.history.path";
    public static final String DIP_KAFKA_PULL_SIZE = "dip.kafka.max.pull.size";

    public static final String DIP_WF_MONITOR_INTERVAL = "dip.wf.monitor.interval";
    public static final String DIP_PARTITION_CREATE_DDL = "dip.hive.partition.createddl";
    public static final String DIP_SCHEDULE_RETRY_MAX = "dip.schedule.retry.max";

    private static final String SITE_XML = "dip.conf";
    private static Logger LOG = LoggerFactory.getLogger(DipContext.class);
    private static DipContext context;
    private Properties properties;

    private DipContext() {
        initConfig();
    }

    public static DipContext getContext() {
        if (context == null) {
            context = new DipContext();
        }
        return context;
    }

    private void initConfig() {

        properties = new Properties();
        try {
            properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("dip-default.conf"));
        } catch (Exception e) {
            LOG.info("Fail to load dip-default.conf");
        }

        try {
            Properties siteProperties = new Properties(properties);
            siteProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(SITE_XML));
            for (String key : siteProperties.stringPropertyNames()) {
                properties.put(key, siteProperties.getProperty(key));
            }
        } catch (Exception e) {
            LOG.info("Can not find config file {0}, Using default-config", SITE_XML);
        }

        for (String key : properties.stringPropertyNames()) {
            LOG.debug("[dip.conf] " + key + " = " + properties.getProperty(key));
        }

    }

    public String getConfig(String name) {
        return properties.getProperty(name);
    }

    public long getLong(String name, long defaultValue) {
        try {
            return Long.parseLong(getConfig(name));
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public int getInt(String name, int defaultValue) {
        try {
            return Integer.parseInt(getConfig(name));
        } catch (Exception e) {
            return defaultValue;
        }
    }

    @VisibleForTesting
    public void setConfig(String name, String value) {
        properties.put(name, value);
    }

    public Properties getProperties() {
        return properties;
    }


}
