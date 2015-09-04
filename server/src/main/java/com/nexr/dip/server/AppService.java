package com.nexr.dip.server;


import com.nexr.dip.DipLoaderException;

public interface AppService {
    public void start() throws DipLoaderException;

    public void shutdown() throws DipLoaderException;
}