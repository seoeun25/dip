package com.nexr.dip.loader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.nexr.dip.DipLoaderException;
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

    private static ScheduledService scheduledService;
    private ScheduledExecutorService executorService;

    private List<TopicManager> topicManagerList;

    private boolean shutdown = false;


    private ScheduledService() {
        String loaderName = "loader-%d";
        executorService = Executors.newScheduledThreadPool(100, new ThreadFactoryBuilder().setNameFormat(loaderName).build());

        topicManagerList = new ArrayList<TopicManager>();
    }

    public static ScheduledService getInstance() {
        if (scheduledService == null) {
            scheduledService = new ScheduledService();
        }
        return scheduledService;
    }

    public void start() throws DipLoaderException {

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

        String[] avroTopics = DipContext.getContext().getConfig(DipContext.AVRO_TOPICS).split(",");
        long lastExecutionTime = createTask(appPathTemplate, avroTopics, Loader.SrcType.avro, initialExecutionTime);
        String[] textTopics = DipContext.getContext().getConfig(DipContext.TEXT_TOPICS).split(",");
        lastExecutionTime = createTask(appPathTemplate, textTopics, Loader.SrcType.text,
                lastExecutionTime + DipContext.getContext().getLong("dip.execution.topic.interval", 1000 * 60 * 2));
    }

    private long createTask(String appPathTemplate, String[] topics, Loader.SrcType srcType, long initialExecutionTime) {
        long topicInterval = DipContext.getContext().getLong("dip.execution.topic.interval", 1000 * 60 * 2); // 2 minutes

        int i = 0;
        long executionTime = 0;
        for (String srcInfo : topics) {
            String topic = srcInfo.trim();
            if (topic.isEmpty()) {
                continue;
            }
            String appPath = appPathTemplate.replace("${appName}", topic);
            executionTime = initialExecutionTime + (i * topicInterval);
            TopicManager topicManager = new TopicManager(this, topic, appPath, srcType, executionTime);
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
            appPathTemplate = appPathTemplate.replace("${user.name}", DipContext.getContext().getConfig(DipContext.DIP_USER_NAME)) ;
        }
        if (appPathTemplate.contains("${root}")) {
            appPathTemplate = appPathTemplate.replace("${root}", jobProperties.getProperty("root"));
        }
        if (appPathTemplate.contains("${dipNameNode}")) {
            appPathTemplate = appPathTemplate.replace("${dipNameNode}", DipContext.getContext().getConfig(DipContext.DIP_NAMENODE));
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
//        int minute = current.get(Calendar.MINUTE);
//        int diff = (int) (Math.ceil((double)minute /10) * 10) - minute;
//        current.add(Calendar.MINUTE, diff);
//        current.set(Calendar.SECOND, 0);
//        current.set(Calendar.MILLISECOND, 0);
//        return current.getTimeInMillis();

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
