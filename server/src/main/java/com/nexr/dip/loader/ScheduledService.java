package com.nexr.dip.loader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.nexr.dip.Context;
import com.nexr.dip.DipLoaderException;
import com.nexr.dip.jpa.LoadResultQueryExecutor;
import com.nexr.dip.server.DipContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * SchedulerdService schedule the loader by topic.
 */
public class ScheduledService {

    public static final Logger LOG = LoggerFactory.getLogger(ScheduledService.class);

    private ScheduledExecutorService executorService;

    private List<TopicManager> topicManagerList;

    private boolean shutdown = false;

    private final Context context;

    private final HDFSClient hdfsClient;

    private final LoadResultQueryExecutor loadResultQueryExecutor;

    @Inject
    public ScheduledService(Context context, HDFSClient hdfsClient, LoadResultQueryExecutor loadResultQueryExecutor) {
        LOG.info("---- constructor SchedudledService:  context : " + context);
        this.context = context;
        this.hdfsClient = hdfsClient;
        this.loadResultQueryExecutor = loadResultQueryExecutor;
        String loaderName = "loader-%d";
        executorService = Executors.newScheduledThreadPool(100, new ThreadFactoryBuilder().setNameFormat(loaderName).build());

        topicManagerList = new ArrayList<TopicManager>();
    }

    public Context getContext() {
        return context;
    }

    public HDFSClient getHdfsClient() {
        return hdfsClient;
    }

    public LoadResultQueryExecutor getLoadResultQueryExecutor() {
        return loadResultQueryExecutor;
    }

    public void start() throws DipLoaderException {
        LOG.info("---- start SchedudledService : context" + context);
        try {
            initScheduleTask();
        }catch (Exception e) {
            throw new DipLoaderException(e);
        }
    }

    private void initScheduleTask() throws Exception {
        String appPathTemplate = getAppPathTempalte();
        LOG.info("appPathTemplate : " + appPathTemplate);

        long initialExecutionTime = System.currentTimeMillis();
        try {
            initialExecutionTime = getInitialExecutionTime(Calendar.getInstance());
        }catch (ParseException e) {
            LOG.warn("Fail to parse, Use current time as initialExecutionTime", e);
        }

        String[] avroTopics = context.getConfig(DipContext.AVRO_TOPICS).split(",");
        long lastExecutionTime = createTask(appPathTemplate, avroTopics, Loader.SrcType.avro, initialExecutionTime);
        String[] textTopics = context.getConfig(DipContext.TEXT_TOPICS).split(",");
        lastExecutionTime = createTask(appPathTemplate, textTopics, Loader.SrcType.text,
                lastExecutionTime + context.getLong("dip.execution.topic.interval", 1000 * 60 * 2));
    }

    private long createTask(String appPathTemplate, String[] topics, Loader.SrcType srcType, long initialExecutionTime) {
        long topicInterval = context.getLong("dip.execution.topic.interval", 1000 * 60 * 2); // 2 minutes

        int i = 0;
        long executionTime = 0;
        for (String srcInfo : topics) {
            String topic = srcInfo.trim();
            if (topic.isEmpty()) {
                continue;
            }
            String appPath = appPathTemplate.replace("${appName}", topic);
            executionTime = initialExecutionTime + (i * topicInterval);
            TopicManager topicManager = new TopicManager(new TopicManager.TopicDesc(topic, appPath, srcType), this,executionTime);
            topicManager.start();
            topicManagerList.add(topicManager);
            i++;
        }
        return executionTime;
    }

    private String getAppPathTempalte() throws IOException{
        Properties jobProperties = new Properties();
        jobProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("wf/job.properties"));
        String appPathTemplate = jobProperties.get("oozie.wf.application.path").toString();
        if (appPathTemplate.contains("${nameNode}")) {
            appPathTemplate = appPathTemplate.replace("${nameNode}", jobProperties.getProperty("nameNode"));
        }
        if ( appPathTemplate.contains("${user.name}"))  {
            appPathTemplate = appPathTemplate.replace("${user.name}", context.getConfig(DipContext.DIP_USER_NAME)) ;
        }
        if (appPathTemplate.contains("${root}")) {
            appPathTemplate = appPathTemplate.replace("${root}", jobProperties.getProperty("root"));
        }
        if (appPathTemplate.contains("${dipNameNode}")) {
            appPathTemplate = appPathTemplate.replace("${dipNameNode}", context.getConfig(DipContext.DIP_NAMENODE));
        }

        return appPathTemplate;
    }

    public void submit(Runnable runnable) {
        executorService.submit(runnable);
    }

    public ScheduledFuture<LoadResult> schedule(Callable callable, long delay, TimeUnit unit) {
        return executorService.schedule(callable, delay, unit);
    }

    public void debugLoaderStatus() {
        for (TopicManager topicManager : topicManagerList) {
            topicManager.debugLoaderStatus();
        }
    }

    public void debugTaskMangerStatus(boolean all) {
        for (TopicManager topicManager : topicManagerList) {
            if (topicManager.getStatus() == TopicManager.STATUS.END) {
                LOG.info("topicManager = " + topicManager.toString());
            } else {
                if (all) {
                    LOG.info("topicManager = " + topicManager.toString());
                } else {
                    LOG.debug("topicManager = " + topicManager.toString());
                }
            }
        }
    }

    public JSONObject toJsonObject() {
        JSONObject jsonObject = new JSONObject();
        JSONArray jsonArray = new JSONArray();
        for (TopicManager topicManager : topicManagerList) {
            jsonArray.add(topicManager.toJsonObject());
        }
        jsonObject.put("topic", jsonArray);
        return jsonObject;
    }

    public JSONObject toJsonObject(String topic) {
        for (TopicManager topicManager : topicManagerList) {
            if (topicManager.getName().equals(topic)) {
                return topicManager.toJsonObject();
            }
        }
        return null;
    }

    public boolean prepareShutdown() {
        boolean prepared = true;
        LOG.info("Start prepareShutdown, topic list : " + topicManagerList.size());
        for (TopicManager topicManager : topicManagerList) {
            prepared = prepared && topicManager.prepareShutdown() ;
        }
        return prepared;
    }

    public TopicManager.STATUS restartTopicManager(String topic) throws DipLoaderException{
        TopicManager topicManager = getTopicManager(topic);
        return topicManager.restart(System.currentTimeMillis());
    }

    public TopicManager.STATUS closeTopicManager(String topic) throws DipLoaderException {
        TopicManager topicManager = getTopicManager(topic);
        return topicManager.closing(1000 * 60 * 20);
    }

    public TopicManager getTopicManager(String topic) throws DipLoaderException{
        for (TopicManager topicManager : topicManagerList) {
            if (topicManager.getName().equals(topic)) {
                return topicManager;
            }
        }
        throw new DipLoaderException("Topic not found: " + topic);
    }

    @VisibleForTesting
    long getInitialExecutionTime(Calendar current) throws ParseException {
        current.add(Calendar.MINUTE, 1);
        current.set(Calendar.SECOND, 0);
        return current.getTimeInMillis();
    }



    public void shutdown() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

}
