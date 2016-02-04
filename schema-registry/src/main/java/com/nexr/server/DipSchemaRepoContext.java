package com.nexr.server;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class DipSchemaRepoContext {

    private static final String SITE_XML = "schema-repo.conf";
    private static Logger LOG = LoggerFactory.getLogger(DipSchemaRepoContext.class);
    private static DipSchemaRepoContext context;
    private Properties properties;

    private DipSchemaRepoContext() {
        initConfig();
    }

    public static DipSchemaRepoContext getContext() {
        if (context == null) {
            context = new DipSchemaRepoContext();
        }
        return context;
    }

    private void initConfig() {

        properties = new Properties();
        try {
            properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("schema-repo-default.conf"));
        } catch (Exception e) {
            LOG.info("Fail to load schema-repo-default.conf");
        }

        try {
            properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(SITE_XML));
            for (String key: properties.stringPropertyNames()) {
                LOG.info(key + " = " + properties.get(key));
            }
        } catch (Exception e) {
            LOG.info("Can not find config file {0}, Using default-config", SITE_XML);
        }

    }

    public String getConfig(String name) {
        return properties.getProperty(name);
    }

    @VisibleForTesting
    public void setConfig(String name, String value) {
        properties.put(name, value);
    }


}
