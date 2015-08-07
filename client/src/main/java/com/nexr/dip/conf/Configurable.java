package com.nexr.dip.conf;

import com.nexr.dip.DipException;

/**
 * Interface to take a configuration.
 */
public interface Configurable {

    public String SCHEMAREGIDTRY_URL = "etl.schema.registry.url";
    public String SCHEMAREGISTRY_CLASS = "dip.scheamregistry.class";


    /**
     * Request the implementing class to (re)configure itself.
     *
     * @param context
     */
    public void configure(Context context) throws DipException;
}
