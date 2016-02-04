package com.nexr.server;

import com.nexr.AvroRepoException;

public interface AppService {
    public void start() throws AvroRepoException;

    public void shutdown() throws AvroRepoException;
}
