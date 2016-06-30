package com.nexr.dip;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Context {

    private static Logger LOG = LoggerFactory.getLogger(Context.class);
    private Properties properties;

    private final String siteConfig;
    private final String defaultConfig;

    @Inject
    public Context(@Named("siteConfig") String siteConfig, @Named("defaultConfig") String defaultConfig) {
        this.siteConfig = siteConfig;
        this.defaultConfig = defaultConfig;
        LOG.info("siteConfig : " + siteConfig + " , defaultConfig : " + defaultConfig);
        initConfig();
    }

    public void initConfig() {

        properties = new Properties();
        try {
            properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(defaultConfig));
        } catch (Exception e) {
            LOG.info("Fail to load " + defaultConfig);
        }

        try {
            Properties siteProperties = new Properties(properties);
            siteProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(siteConfig));
            for (String key : siteProperties.stringPropertyNames()) {
                properties.put(key, siteProperties.getProperty(key));
            }
        } catch (Exception e) {
            LOG.info("Can not find config file {0}, Using default-config", siteConfig);
        }

        for (String key : properties.stringPropertyNames()) {
            LOG.info("[dip.conf] " + key + " = " + properties.getProperty(key));
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
