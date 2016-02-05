package com.nexr.server;

import com.google.common.annotations.VisibleForTesting;
import com.nexr.dip.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class DipSchemaRepoContext {

    private static Logger LOG = LoggerFactory.getLogger(DipSchemaRepoContext.class);
    private static DipSchemaRepoContext schemaRepoContext;

    private Context context;

    private final String SITE_CONFIG = "schemarepo.conf";
    private final String DEFAULT_CONFIG = "schemarepo-default.conf";


    private DipSchemaRepoContext() {
        context = new Context();
        context.initConfig(SITE_CONFIG, DEFAULT_CONFIG);
    }

    public static DipSchemaRepoContext getContext() {
        if (schemaRepoContext == null) {
            schemaRepoContext = new DipSchemaRepoContext();
        }
        return schemaRepoContext;
    }

    public String getConfig(String name) {
        return context.getConfig(name);
    }

    @VisibleForTesting
    public void setConfig(String name, String value) {
        context.setConfig(name, value);
    }


}
