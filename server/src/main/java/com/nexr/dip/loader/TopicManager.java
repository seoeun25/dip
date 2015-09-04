package com.nexr.dip.loader;

import com.nexr.dip.DipLoaderException;
import com.nexr.dip.common.Utils;
import com.nexr.dip.jpa.LoadResultQueryExecutor;
import com.nexr.dip.server.DipContext;
import com.nexr.dip.server.JDBCService;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Manage the life cycle of the loader by topic.
 */
public class TopicManager {

    public static final Logger LOG = LoggerFactory.getLogger(TopicManager.class);
    private Loader currentLoader = null;
    private ScheduledService scheduledService;
    private String name;
    private String appPath;
    private Loader.SrcType srcType;
    private long initialExecutionTime;
    private long interval;
    private STATUS status = STATUS.START;
    private LoadResultQueryExecutor loadResultQueryExecutor;

    public TopicManager(ScheduledService scheduledService, String name, String appPath, Loader.SrcType srcType, long
            initialExecutionTime) {
        this.scheduledService = scheduledService;
        this.name = name;
        this.appPath = appPath;
        this.srcType = srcType;
        this.initialExecutionTime = initialExecutionTime;

        long defaultScheduleInterval = DipContext.getContext().getLong("dip.load.schedule.interval", 1000 * 60 * 60); // 1 hour
        interval = DipContext.getContext().getLong("dip.load.schedule." + name + ".interval",
                defaultScheduleInterval);

        loadResultQueryExecutor = new LoadResultQueryExecutor(JDBCService.getInstance());

    }

    public void start() {
        status = STATUS.RUNNING;
        Loader loader = new Loader(name, appPath, srcType, initialExecutionTime, interval);
        scheduleLoader(loader, null);
    }

    private Loader createLoader(long executionTime) {
        Loader loader = new Loader(name, appPath, srcType, executionTime, interval);
        return loader;
    }

    public STATUS restart(long initialExecutionTime) throws DipLoaderException {
        if (status != STATUS.END) {
            throw new DipLoaderException("TopicManager [" + name + "] is not END, but " + status.toString());
        }
        this.initialExecutionTime = initialExecutionTime;
        start();

        try {
            Thread.sleep(10);
        } catch (Exception e) {

        }
        return status;
    }

    public STATUS closing(long timeout) {
        if (status == STATUS.CLOSING) {
            return STATUS.CLOSING;
        }
        LOG.info("TopicManager [{}] is closing, it is trying to shutdown in {} ms", name, timeout);
        status = STATUS.CLOSING;

        final long fTimeout = timeout;
        Runnable cancleRunnable = new Runnable() {
            @Override
            public void run() {
                boolean canceled = false;
                long endTime = System.currentTimeMillis() + fTimeout;
                while (System.currentTimeMillis() < endTime) {
                    try {
                        canceled = cancel();
                        if (canceled) {
                            break;
                        }
                        Thread.sleep(1000 * 60);
                    } catch (Exception e) {
                        LOG.warn("Fail to cancel", e);
                    }
                }
                LOG.info("TopicManager [{}] is closed : {} ", name, canceled);
            }
        };
        scheduledService.submit(cancleRunnable);
        try {
            Thread.sleep(10);
        } catch (Exception e) {

        }
        return status;
    }

    public JSONObject getCurrentLoaderInfo() {
        return currentLoader == null ? null : currentLoader.toJsonObject();
    }

    private boolean cancel() {
        if (currentLoader == null) {
            return true;
        }
        boolean canceled = false;
        if (currentLoader.getExecutionTime() > System.currentTimeMillis()) {
            // not yet started.
            if (currentLoader.getFuture() != null) {
                currentLoader.getFuture().cancel(false);
                canceled = currentLoader.getFuture().isCancelled();
                LOG.info("cancel {} : {} ", name, canceled);
            } else {
                LOG.warn("cancel : Future is null, Nothing to do");
                canceled = true;
            }
        } else {
            LOG.warn("cancel : failed. It is running {}", currentLoader.getHeader() + ", " + currentLoader.getStatus() + ", " +
                    currentLoader.getLoadResult().getExternalId());
        }
        return canceled;
    }

    public void shutdown() {
        LOG.error("TopicManager [{}] shutdown...... \n", name);
        status = STATUS.END;
    }

    public void scheduleLoader(Loader nextLoader, Loader oldLoader) {
        if (status == STATUS.CLOSING) {
            LOG.info("TopicManager [{}] is closing, no more loader scheduling", name);
            clearCurrentLoader(oldLoader);
            shutdown();
            return;
        }
        clearCurrentLoader(oldLoader);
        final Loader fLoader = nextLoader;
        final long delay = nextLoader.getExecutionTime() - System.currentTimeMillis();
        LOG.info("Submit Loader [{}] ", nextLoader.toString());
        Runnable scheduleTask = new Runnable() {
            @Override
            public void run() {
                executeLoader(fLoader, delay);
            }
        };
        scheduledService.submit(scheduleTask);
    }

    public void executeLoader(Loader loader, long delay) {
        LOG.info("ExecuteLoader [{}] ", loader);
        ScheduledFuture<LoadResult> future = scheduledService.schedule(loader, delay, TimeUnit.MILLISECONDS);
        loader.setFuture(future);
        setCurrentLoader(loader);
        insertMeta(loader);

        try {
            LoadResult result = future.get();
            while (result == null) {
                Thread.sleep(1000 * 60);
                result = future.get();
            }

        } catch (CancellationException e) {
            LOG.warn("Cancel ::: " + loader.toString() + " ::: " + e.getMessage());
            loader.getLoadResult().setError(e.getMessage());
            loader.getLoadResult().setStatus(LoadResult.STATUS.FAILED);
        } catch (Exception e) {
            LOG.error("Fail to execute ::: " + e.toString(), e);
            loader.getLoadResult().setError(e.getMessage());
            loader.getLoadResult().setStatus(LoadResult.STATUS.FAILED);
        }
        onFinish(loader);
    }

    private void setCurrentLoader(Loader loader) {
        if (currentLoader != null) {
            LOG.warn("Current loader is not null : " + currentLoader.toString());
        }
        this.currentLoader = loader;
        LOG.info("set Loader : " + name + ", " + Utils.getDateString(loader.getExecutionTime()));
    }

    private void insertMeta(Loader loader) {
        try {
            loadResultQueryExecutor.insert(loader.getLoadResult());
            LOG.debug("Insert loadResult in DB : " + loader.getLoadResult().toString());
        } catch (DipLoaderException e) {
            LOG.warn("Failed to insert loadResult : " + loader.getLoadResult(), e);
        }
    }

    private void updateMeta(Loader loader) {
        try {
            int ret = loadResultQueryExecutor.executeUpdate(LoadResultQueryExecutor.LoadResultQuery.UPDATE_LOADRESULT,
                    loader.getLoadResult());
            if (ret == 0) {
                LOG.warn("Failed to update loadResult in DB");
                return;
            }
            LOG.debug("Update result in DB " + loader.getLoadResult().toString());
        } catch (DipLoaderException e) {
            LOG.warn("Failed to update loadResult : " + loader.getLoadResult(), e);
        }
    }

    private void onFinish(Loader loader) {
        LoadResult result = loader.getLoadResult();
        result.setEndTime(new Timestamp(System.currentTimeMillis()));

        LOG.info("onFinish : " + loader.getLoadResult().toString() + "\n");

        if (result.getStatus() == LoadResult.STATUS.SUCCEEDED) {
            onSucceed(loader);
        } else if (result.getStatus() == LoadResult.STATUS.FAILED) {
            onFail(loader);
        } else if (result.getStatus() == LoadResult.STATUS.WF_DONE_COUNT_FAILED) {
            //TODO what should be done?
            LOG.warn("WF done, Count failed. Check the WF at oozie and application logs at yarn");
            onSucceed(loader);
        } else if (result.getStatus() == LoadResult.STATUS.RETRY) {
            onRetry(loader);
        } else {
            LOG.error("Should not happend, WF finished and LoadResult {}", result.toString());
            onFail(loader);
        }

        updateMeta(loader);
        //clearCurrentLoader(loader);
    }

    private void onSucceed(Loader loader) {
        // TODO ? succeed?
        long nextExecutionTime = loader.getExecutionTime() + loader.getInterval();
        if (nextExecutionTime < System.currentTimeMillis()) {
            nextExecutionTime += loader.getInterval();
            LOG.info("NextExecutionTime is passed, set to next interval");
        }
        Loader nextLoader = new Loader(loader.getName(), loader.getAppPath(), srcType, nextExecutionTime, loader
                .getInterval());
        scheduleLoader(nextLoader, loader);
    }

    private void onRetry(Loader loader) {
        int max = DipContext.getContext().getInt(DipContext.DIP_SCHEDULE_RETRY_MAX, 3);
        int retryCount = loader.getRetryCount();
        LOG.warn("onRetry : [{}] retryCount {} ", loader.getName(), retryCount);
        if (retryCount >= max) {
            onFail(loader);
            return;
        }

        retryCount++;
        long nextExecutionTime = System.currentTimeMillis() + (1000 * 60 * retryCount);
        Loader nextLoader = new Loader(loader.getName(), loader.getAppPath(), srcType, nextExecutionTime, loader
                .getInterval(), retryCount);
        scheduleLoader(nextLoader, loader);
    }

    private void onFail(Loader loader) {
        shutdown();
    }

    private void clearCurrentLoader(Loader loader) {
        if (loader == null) {
            LOG.info("clear current loader : null");
            currentLoader = null;
            return;
        }
        LOG.info("clear current loader : {} >>> load result {}", loader.getHeader() + ", " +loader.getStatus(), loader
                .getLoadResult()
                .getStatus());

        if (!loader.equals(currentLoader)) {
            LOG.warn("CurrentLoader is wrong, current [{}], loader [{}]", currentLoader.toString(), loader.toString());
        }
        if (currentLoader.getStatus() != Loader.STATUS.END ) {
            LOG.warn("Loader is not terminated : {}", currentLoader.toString());
        }
        currentLoader = null;
    }

    public void debugLoaderStatus() {
            LOG.info("LoaderStatus [{}]", currentLoader.toString());
    }

    public JSONObject toJsonObject() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", name);
        jsonObject.put("status", status.toString());
        jsonObject.put("loader", currentLoader == null ? "null" : currentLoader.toJsonObject());
        return jsonObject;
    }

    public boolean prepareShutdown() {
        boolean canceled = false;
        try {
            canceled = cancel();
        } catch (Exception e) {

        }
        return canceled;
    }

    public STATUS getStatus() {
        return status;
    }

    public String getName() {
        return name;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[" + name + "] " + status);
        builder.append(", loader=" + currentLoader == null ? "null" : currentLoader.toJsonObject());
        return builder.toString();
    }

    public enum STATUS {
        START,
        RUNNING,
        CLOSING, // running but no more adding loader
        END;
    }

}
