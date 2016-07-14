package com.nexr.dip.loader;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.linkedin.camus.etl.kafka.common.EtlCounts;
import com.linkedin.camus.etl.kafka.common.Source;
import com.nexr.dip.Context;
import com.nexr.dip.DipLoaderException;
import com.nexr.dip.common.Utils;
import com.nexr.dip.server.DipContext;
import com.sun.org.apache.bcel.internal.generic.DCONST;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ScheduledFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Load the messages from kafka to HDFS.
 */
public class Loader implements Callable<LoadResult> {
    public static final String COUNT_FILE_KEY = "ExecutionCountFile";
    public static String KFKKA_LEADER_ERROR = "Some topics skipped due to failure in getting latest offset from Kafka leaders";
    public static String OOZIE_USER_RETRY_ERROR = "JA008";
    public static String IO_ERROR = "java.io.IOException";
    private static Logger LOG = LoggerFactory.getLogger(Loader.class);
    private String name;
    private TopicManager.TopicDesc topicDesc;
    private LoadResult loadResult;
    private long executionTime;
    private long interval;
    private ScheduledFuture<LoadResult> future;
    private STATUS status = STATUS.PREP;
    private int retryCount;

    @Inject
    private Context context;

    @Inject
    private HDFSClient hdfsClient;

    public Loader(TopicManager.TopicDesc topicDesc, long executionTime, long interval) {
        this(topicDesc, executionTime, interval, 0);
    }

    public Loader(TopicManager.TopicDesc topicDesc, long executionTime, long interval, int retryCount) {
        this.topicDesc = topicDesc;
        this.name = topicDesc.getName();
        this.interval = interval;
        this.retryCount = retryCount;
        Timestamp timestamp = new Timestamp(executionTime);
        timestamp.setNanos(0);
        this.loadResult = new LoadResult(name, null, timestamp);
        this.executionTime = timestamp.getTime();
    }

    public TopicManager.TopicDesc getTopicDesc() {
        return this.topicDesc;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public LoadResult getLoadResult() {
        return loadResult;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }

    public long getInterval() {
        return interval;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public ScheduledFuture<LoadResult> getFuture() {
        return future;
    }

    public void setFuture(ScheduledFuture future) {
        this.future = future;
    }

    public STATUS getStatus() {
        return this.status;
    }

    public WorkflowJob.Status monitorJob(WorkflowClient workflowClient, String jobID) throws Exception {
        OozieClient client = workflowClient.getClient();
        WorkflowJob.Status status = client.getJobInfo(jobID).getStatus();

        while (!workflowClient.isTerminated(status)) {
            Thread.sleep(WorkflowClient.POLLING);
            WorkflowJob workflowJob = client.getJobInfo(jobID);
            status = workflowJob.getStatus();
            if (getLoadAction(workflowJob) != null) {
                String externalId = getLoadAction(workflowJob).getExternalId();
                loadResult.setExternalId(externalId);
                String childIds = getLoadAction(workflowJob).getExternalChildIDs();
                LOG.info("[{}] {}", jobID, name + ", " + status + ", " + externalId);
            } else {
                LOG.info("[{}] {}", jobID, name + ", " + status);
            }
        }
        LOG.info("[{}] finished = {}", jobID, status);
        return status;
    }

    private WorkflowAction getLoadAction(WorkflowJob workflowJob) {
        WorkflowAction loadAction = null;
        List<WorkflowAction> actions = workflowJob.getActions();
        for (WorkflowAction action : actions) {
            if (action.getName().startsWith("load")) {
                loadAction = action;
                break;
            }
        }
        return loadAction;
    }

    @Override
    public LoadResult call() throws Exception {
        status = STATUS.START;
        loadResult.setStatusStr(LoadResult.STATUS.RUNNING.toString());
        LOG.info("Loader " + status + " [{}], real execute time [{}] ", name,
                Utils.getDateString(System.currentTimeMillis()));
        try {
            //FIXME
            LOG.info("------ context : " + context);

            WorkflowClient client = new WorkflowClient(context);
            Properties properties = new Properties();
            properties.put(OozieClient.APP_PATH, topicDesc.getAppPath());
            String jobId = client.run(properties);
            loadResult.setJobId(jobId);
            status = STATUS.WFRUNNING;
            LOG.info("[{}], jobId = {} ", name, jobId);
            WorkflowJob.Status status = monitorJob(client, jobId);
            loadResult.setWfStatus(status);
            WorkflowJob job = client.getClient().getJobInfo(jobId);
            WorkflowAction loadAction = getLoadAction(job);
            if (loadAction.getData() != null) {
                Properties properties1 = new Properties();
                properties1.load(new StringReader(loadAction.getData()));
                loadResult.setCountFile(properties1.get(COUNT_FILE_KEY).toString());
            }
            if (loadAction != null && loadAction.getErrorMessage() != null) {
                loadResult.setError(loadAction.getErrorMessage());
            }

            onWFFinish();

            LOG.debug("call end [{}] ", loadResult.toString());
        } catch (CancellationException e) {
            LOG.info("Cancel : " + this.toString());
        } catch (OozieClientException e) {
            loadResult.setError(e.getMessage());
            loadResult.setStatus(LoadResult.STATUS.RETRY);
            LOG.error("run to [{}] ", context.getConfig(DipContext.DIP_OOZIE));
            LOG.error("Fail to load, need to retry : " + name, e);
        } catch (Exception e) {
            loadResult.setError(e.getMessage());
            loadResult.setStatus(LoadResult.STATUS.FAILED);
            LOG.error("run to [{}] ", context.getConfig(DipContext.DIP_OOZIE));
            LOG.error("Fail to load : " + name, e);
        }
        status = STATUS.END;
        return loadResult;
    }

    private void onWFFinish() {
        LOG.debug("onWFFinish : " + getLoadResult().toString());
        LoadResult result = getLoadResult();
        if (result.getWfStatus() == null) {
            result.setStatus(LoadResult.STATUS.FAILED);
        } else {
            try {
                status = STATUS.COUNTING;
                count();
            } catch (Exception e) {
                if (getLoadResult().getWfStatus() != WorkflowJob.Status.SUCCEEDED) {
                    LOG.warn("WF {}, No counting file", getLoadResult().getWfStatus());
                } else {
                    LOG.warn("Fail to read count file : " + getLoadResult().getCountFile());
                }
                LOG.warn("Need to check the load result later, manually");
                //TODO no count file, but upload succeed?
                //TODO search mr log?
            }

            if (getLoadResult().getExternalId() != null) {
                try {
                    String camusApplicationId = null;
                    for (int i = 0; i < 3; i++) {
                        camusApplicationId = readLauncherLogs(getLoadResult().getExternalId());
                        if (camusApplicationId != null) {
                            break;
                        }
                        try {
                            LOG.info("Retry read yarn launcher logs in 2000 ms, [{}]", String.valueOf(i));
                            Thread.sleep(2000);
                        } catch (Exception e) {

                        }
                    }
                    String movedFiles = readCamusApplicationLogs(camusApplicationId);
                    getLoadResult().setResultFiles(movedFiles);
                    addPartitionToHive(movedFiles);
                } catch (DipLoaderException e) {
                    LOG.warn("Failed to get upload file", e);
                }
            } else {
                LOG.warn("No externalId for loader : " + this);
            }

            if (result.getWfStatus() == WorkflowJob.Status.SUCCEEDED) {
                if (getLoadResult().getErrorCount() == 0) {
                    result.setStatus(LoadResult.STATUS.SUCCEEDED);
                } else if (getLoadResult().getErrorCount() > 0) {
                    getLoadResult()
                            .setError("Error on read message from kafka, errorCount=" + getLoadResult().getErrorCount());
                    result.setStatus(LoadResult.STATUS.FAILED);
                } else {
                    result.setStatus(LoadResult.STATUS.WF_DONE_COUNT_FAILED);
                }
            } else if (result.getWfStatus() == WorkflowJob.Status.FAILED || result.getWfStatus() == WorkflowJob.Status.KILLED) {
                // TODO mr succeed but JavaMain left the error > JA018 > error > Kill WF. or RuntimeException by Camus.
                if (isTransientError(getLoadResult().getError())) {
                    cleanupResult();
                    result.setStatus(LoadResult.STATUS.RETRY);
                } else {
                    result.setStatus(LoadResult.STATUS.FAILED);
                }
            } else {
                LOG.error("It should not happen : " + result.toString());
                result.setStatus(LoadResult.STATUS.FAILED);
            }
        }
    }

    private boolean isTransientError(String errorMsg) {
        if (errorMsg == null || errorMsg.isEmpty()) {
            return false;
        }
        boolean result = false;
        if (errorMsg.contains(KFKKA_LEADER_ERROR) || errorMsg.contains(OOZIE_USER_RETRY_ERROR) || errorMsg.contains(IO_ERROR)) {
            result = true;
        }

        return result;
    }

    @VisibleForTesting
    public void cleanupResult() {
        //FIXME hdfsclient init
        LOG.warn("cleanupResult start");
        boolean removed = true;
        if (getLoadResult().getResultFiles() != null && !getLoadResult().getResultFiles().equals("")) {
            String[] files = getLoadResult().getResultFiles().split(",");
            for (String file : files) {
                try {
                    removed = removed && hdfsClient.delete(new Path(context.getConfig
                            (DipContext.DIP_NAMENODE) + file));
                } catch (Exception e) {
                    LOG.error("Failed to delete result : " + file, e);
                }
            }
        } else {
            LOG.warn("No uploaded file, No need to delete.");
        }

        if (getLoadResult().getEtlExecutionPath() != null) {
            String camusExectutionPath = getLoadResult().getEtlExecutionPath();
            String hisotryPath = context.getConfig(DipContext.DIP_ETL_EXECUTION_HISTORY_PATH);
            if (hisotryPath.contains("${topic}")) {
                hisotryPath = hisotryPath.replace("${topic}", name);
            }
            try {
                removed = removed && hdfsClient.delete(new Path(context.getConfig(DipContext
                        .DIP_NAMENODE) + hisotryPath + "/" + camusExectutionPath));
            } catch (Exception e) {
                LOG.error("Failed to delete camus history : " + hisotryPath + "/" + camusExectutionPath, e);
            }
        }
        LOG.warn("cleanupResult end");
    }

    private EtlCounts count() throws Exception {
        if (getLoadResult().getCountFile() == null) {
            // TODO : error 발생해도 wf SUCCEEDED. search log?
            throw new FileNotFoundException("EtlCounts file is null. Check etl.keep.count.files configuration.");
        }
        Path path = new Path(context.getConfig(DipContext.DIP_NAMENODE) + getLoadResult()
                .getCountFile());
        LOG.debug("count from : " + path.toString());
        EtlCounts etlCounts = new EtlCounts(name, 10);

        String user = System.getProperty("user.name");
        FileSystem fs = hdfsClient.createFileSystem(user, path.toUri());
        if (fs.isDirectory(path)) {
            FileStatus[] gstatuses = fs.listStatus(path, new PrefixFilter("counts"));
            for (FileStatus gfileStatus : gstatuses) {
                LOG.debug("gfilestatus : " + gfileStatus.toString());
                FSDataInputStream fdsis = fs.open(gfileStatus.getPath());
                BufferedReader br = new BufferedReader(new InputStreamReader(fdsis), 1048576);
                StringBuffer buffer = new StringBuffer();
                String temp = "";
                while ((temp = br.readLine()) != null) {
                    buffer.append(temp);
                }
                countResult(etlCounts, buffer.toString());
                fdsis.close();
            }
        }

        long totalCount = 0;
        HashMap<String, Source> sourceMap = etlCounts.getCounts();
        for (Map.Entry<String, Source> entry : sourceMap.entrySet()) {
            LOG.debug("counts : " + entry.getKey() + "=" + entry.getValue().getCount());
            totalCount += entry.getValue().getCount();
        }
        LOG.info("counts [{}], totalCount [{}]", getName(), totalCount);
        LOG.info("counts [{}], errorCount [{}]", getName(), etlCounts.getErrorCount());
        LOG.debug("counts [{}], eventCount [{}]", getName(), etlCounts.getEventCount());

        getLoadResult().setResultCount(totalCount);
        getLoadResult().setErrorCount(etlCounts.getErrorCount());

        return etlCounts;
    }

    private void countResult(EtlCounts existingCount, String buffer) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        ArrayList<EtlCounts> countsObjects =
                mapper.readValue(buffer, new TypeReference<ArrayList<EtlCounts>>() {
                });
        LOG.debug("  buffer : " + buffer);
        LOG.debug("  EtlCounts size : " + countsObjects.size());
        for (EtlCounts count : countsObjects) {
            existingCount.setEndTime(Math.max(existingCount.getEndTime(), count.getEndTime()));
            existingCount.setLastTimestamp(Math.max(existingCount.getLastTimestamp(), count.getLastTimestamp()));
            existingCount.setStartTime(Math.min(existingCount.getStartTime(), count.getStartTime()));
            existingCount.setFirstTimestamp(Math.min(existingCount.getFirstTimestamp(), count.getFirstTimestamp()));
            existingCount.setErrorCount(existingCount.getErrorCount() + count.getErrorCount());
            existingCount.setGranularity(count.getGranularity());
            existingCount.setTopic(count.getTopic());
            for (Map.Entry<String, Source> entry : count.getCounts().entrySet()) {
                Source source = entry.getValue();
                if (existingCount.getCounts().containsKey(source.toString())) {
                    Source old = existingCount.getCounts().get(source.toString());
                    old.setCount(old.getCount() + source.getCount());
                    existingCount.getCounts().put(old.toString(), old);
                } else {
                    existingCount.getCounts().put(source.toString(), source);
                }
            }
        }
    }

    private String readLauncherLogs(String applicationId) throws DipLoaderException {
        if (applicationId == null) {
            LOG.warn("Launcher job ({}) applicationId can not be null", name);
            return null;
        }
        // TODO
        // java.lang.RuntimeException: Some topics skipped due to offsets from Kafka metadata out of
        String camusApplication = null;
        String appId = applicationId.replace("job", "application");
        String command = "yarn logs -applicationId " + appId;
        Process process = null;
        try {
            Thread.sleep(2000);
            process = Runtime.getRuntime().exec(command);
            List<String> lines = IOUtils.readLines(process.getInputStream());
            LOG.info("---- launcher logs ({}) [" + command + "] ---- ", name);
            StringBuilder builder = new StringBuilder();
            StringBuilder errorBuilder = new StringBuilder();
            int lineCount = 0;
            for (String line : lines) {
                builder.append(line + "\n");
                if ((line.contains("Exception") && !line.contains("Can't read Kafka version from MANIFEST.MF")) || lineCount > 0) {
                    errorBuilder.append(line + "\n");
                    lineCount++;
                    if (lineCount == 20) {
                        LOG.warn(command + " exception 11 : \n" + errorBuilder.toString() + "\n");
                        lineCount = 0;
                        errorBuilder.delete(0, errorBuilder.length());
                    }
                }
                if (line.contains("Submitted application")) {
                    camusApplication = getCamusApplication(line);
                }
                if (line.contains("New execution temp location")) {
                    String etlExecutionPath = extractExecutionPath(line);
                    etlExecutionPath = etlExecutionPath.substring(etlExecutionPath.lastIndexOf("/"), etlExecutionPath.length());
                    getLoadResult().setEtlExecutionPath(etlExecutionPath);
                }
            }
            if (lineCount > 0) {
                LOG.warn(command + " exception 22 : \n" + errorBuilder.toString() + "\n");
                lineCount = 0;
            }
            LOG.debug(builder.toString());
            LOG.debug("yarn logs end -----------------");
        } catch (Exception e) {
            throw new DipLoaderException("Can not read wf launcher job log from yarn : [" + command + "]", e);
        } finally {
            if (process != null) {
                try {
                    if (process.getInputStream() != null) {
                        process.getInputStream().close();
                    }
                } catch (Exception e) {

                }
            }
        }
        return camusApplication;
    }

    private String readCamusApplicationLogs(String applicationId) throws DipLoaderException {
        Set<String> moveFileSet = new java.util.HashSet<>();
        if (applicationId == null) {
            LOG.warn("Camus job ({}) applicationId can not be null", name);
            return "";
        }
        // TODO
        // java.lang.RuntimeException: Some topics skipped due to offsets from Kafka metadata out of
        String camusApplication = null;
        String appId = applicationId.replace("job", "application");
        String command = "yarn logs -applicationId " + appId;
        Process process = null;
        try {
            Thread.sleep(1000);
            process = Runtime.getRuntime().exec(command);
            List<String> lines = IOUtils.readLines(process.getInputStream());
            LOG.info("---- camus application logs ({}) [" + command + "] ---- ", name);
            StringBuilder builder = new StringBuilder();
            StringBuilder errorBuilder = new StringBuilder();
            for (String line : lines) {
                builder.append(line + "\n");
                if (line.contains("Moved file from:")) {
                    String to = extractMovedFiles(line);
                    if (to != null) {
                        moveFileSet.add(to);
                    }
                }
                if (line.contains("-- checksum-error,")) {
                    errorBuilder.append(line + "\n");
                }
                if (line.contains("Failed to move ")) {
                    errorBuilder.append(line + "\n");
                }
            }
            LOG.debug(builder.toString());
            if (errorBuilder.length() > 0) {
                LOG.warn("mr error : \n" + errorBuilder.toString() + "\n");
            }
            LOG.debug("yarn logs end -----------------");
        } catch (Exception e) {
            throw new DipLoaderException("Can not read camus application log : [" + command + "]", e);
        } finally {
            if (process != null && process.getInputStream() != null) {
                try {
                    process.getInputStream().close();
                } catch (Exception e) {

                }
            }
        }
        StringBuilder moveFiles = new StringBuilder();
        for (String file : moveFileSet) {
            moveFiles.append(file + ",");
        }
        return moveFiles.toString();
    }

    @VisibleForTesting
    public String getCamusApplication(String submitLog) {
        String patternString1 = "(.*)(Submitted application )(.*)";
        Pattern pattern = Pattern.compile(patternString1);
        Matcher matcher = pattern.matcher(submitLog);
        if (matcher.find()) {
            String applicationId = matcher.group(3);
            return applicationId;
        }
        return null;
    }

    @VisibleForTesting
    public String extractMovedFiles(String movedLog) {
        String patternString1 = "(Moved file from: )(.*)( to: )(.*)";
        Pattern pattern = Pattern.compile(patternString1);
        Matcher matcher = pattern.matcher(movedLog);
        if (matcher.find()) {
            String fileName = matcher.group(4);
            return fileName;
        }
        return null;
    }

    @VisibleForTesting
    public String extractExecutionPath(String executionPath) {
        //New execution temp location: /user/ndap/camus/ont_info/exec/2015-10-03-10-57-15
        String patternString = "(New execution temp location: )(.*)";
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(executionPath);
        if (matcher.find()) {
            String fileName = matcher.group(2);
            return fileName;
        }
        return null;
    }

    @VisibleForTesting
    public void addPartitionToHive(String files) {
        String[] fileNames = files.split(",");

        String tableName = "";
        String partition = "";
        String oldLocation = "";
        String patternString1 = "(srcinfos)(.*)(daily)(.*)(/)(.*)";
        Pattern pattern = Pattern.compile(patternString1);
        for (String fileName : fileNames) {
            Matcher matcher = pattern.matcher(fileName);
            if (matcher.find()) {
                tableName = matcher.group(2);
                tableName = "aihsrc." + tableName.substring(1, tableName.length() - 1);
                partition = matcher.group(4);
                partition = partition.substring(1, partition.length());
                String location = fileName.replace(matcher.group(6), "");
                if (!oldLocation.equals(location)) {
                    if (!tableName.equals("") && !partition.equals("") && !location.equals("")) {
                        // ALTER TABLE ${topic} ADD PARTITION (ins_date = '${partition}') location '${location}'
                        String createDML = context.getConfig(DipContext.DIP_PARTITION_CREATE_DDL);
                        System.out.println("org : " + createDML);
                        createDML = createDML.replace("${table_name}", tableName);
                        createDML = createDML.replace("${partition}", partition);
                        createDML = createDML.replace("${location}", location);

                        String[] cmdarray = {"beeline",
                                "-u", "jdbc:hive2://" + context.getConfig(DipContext.DIP_HIVESERVER),
                                "-n", context.getConfig(DipContext.DIP_HIVESERVER_USER),
                                "-p", context.getConfig(DipContext.DIP_HIVESERVER_PASSWD),
                                "-e", "\"" + createDML + "\""
                        };
                        Process process = null;
                        StringBuilder command = new StringBuilder();
                        try {
                            process = Runtime.getRuntime().exec(cmdarray);
                            List<String> lines = IOUtils.readLines(process.getInputStream());
                            StringBuilder builder = new StringBuilder();
                            for (String line : lines) {
                                builder.append(line + "\n");
                            }

                            for (String cmd : cmdarray) {
                                command.append(cmd + " ");
                            }
                            LOG.info(command.toString() + "\n  >>>  \n" + builder.toString());

                        } catch (Exception e) {
                            LOG.warn("Failed to create partition [{}]", command);
                        } finally {
                            if (process != null && process.getInputStream() != null) {
                                try {
                                    process.getInputStream().close();
                                } catch (Exception e) {

                                }
                            }
                        }
                    }
                }
                oldLocation = location;
            }
        }
    }

    public String getHeader() {
        return "[" + name + "] [" + Utils.getDateString(executionTime) + "]";
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(name + ", ");
        builder.append(Utils.getDateString(executionTime) + ", ");
        builder.append(status + ", ");
        builder.append("externalId=" + loadResult.getExternalId() + ", ");
        builder.append(topicDesc.getAppPath() + ", ");
        builder.append(interval);
        return builder.toString();
    }

    public JSONObject toJsonObject() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", name);
        jsonObject.put("executionTime", Utils.getDateString(executionTime));
        jsonObject.put("status", status.toString());
        jsonObject.put("externalId", loadResult.getExternalId());
        return jsonObject;
    }

    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (getClass() != o.getClass()) {
            return false;
        }

        Loader e = (Loader) o;

        return (this.getName() == e.getName() && this.getExecutionTime() == e.getExecutionTime());
    }

    public int hashCode() {
        final int PRIME = 31;
        int initialNonZeroOddNumber = getName().hashCode() + Utils.formatTime(getExecutionTime()).hashCode();
        return new HashCodeBuilder(initialNonZeroOddNumber % 2 == 0 ? initialNonZeroOddNumber + 1 : initialNonZeroOddNumber, PRIME).toHashCode();
    }

    public enum SrcType {
        avro,
        text;
    }

    public enum STATUS {
        PREP, // submit, not called
        START, // called,
        WFRUNNING, // has jobId from oozie, wf running
        COUNTING, // count the upload result
        END; //
    }

    /**
     * Path filter that filters based on prefix
     */
    private class PrefixFilter implements PathFilter {
        private final String prefix;

        public PrefixFilter(String prefix) {
            this.prefix = prefix;
        }

        public boolean accept(Path path) {
            // TODO Auto-generated method stub
            return path.getName().startsWith(prefix);
        }
    }

}
