package com.nexr.dip;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Context {

    private static Logger LOG = LoggerFactory.getLogger(Context.class);
    private Properties properties;

    protected String SITE_CONFIG = "dip.conf";
    protected String DEFAULT_CONFIG = "dip-default.conf";

    public Context() {

    }

    public void initConfig(String siteConfig, String defaultConfig) {
        SITE_CONFIG = siteConfig;
        DEFAULT_CONFIG = defaultConfig;
        initConfig();
    }

    public void initConfig() {

        properties = new Properties();
        try {
            properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(DEFAULT_CONFIG));
        } catch (Exception e) {
            LOG.info("Fail to load " + DEFAULT_CONFIG);
        }

        try {
            Properties siteProperties = new Properties(properties);
            siteProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(SITE_CONFIG));
            for (String key : siteProperties.stringPropertyNames()) {
                properties.put(key, siteProperties.getProperty(key));
            }
        } catch (Exception e) {
            LOG.info("Can not find config file {0}, Using default-config", SITE_CONFIG);
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

    public void setConfig(String name, String value) {
        properties.put(name, value);
    }

    public Properties getProperties() {
        return properties;
    }


}
