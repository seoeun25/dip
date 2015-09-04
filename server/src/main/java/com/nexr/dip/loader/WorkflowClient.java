package com.nexr.dip.loader;

import com.nexr.dip.server.DipContext;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class WorkflowClient {

    public static final Logger LOG = LoggerFactory.getLogger(WorkflowClient.class);
    public static long POLLING = 5000; //ms
    private DipContext dipContext;
    private String oozieUrl;
    private String user;
    private String nameNode;
    private String jobTracker;
    private String hiveServer;

    public WorkflowClient(DipContext dipContext) {
        this.dipContext = dipContext;
        this.oozieUrl = dipContext.getConfig(DipContext.DIP_OOZIE);
        this.nameNode = dipContext.getConfig(DipContext.DIP_NAMENODE);
        this.jobTracker = dipContext.getConfig(DipContext.DIP_JOBTRACKER);
        this.hiveServer = dipContext.getConfig(DipContext.DIP_HIVESERVER);
        this.POLLING = dipContext.getLong(DipContext.DIP_WF_MONITOR_INTERVAL, 10000);
        this.user = System.getProperty("user.name");

        String localUser = System.getProperty("user.name");
        user = localUser;
        //baseAppPath = nameNode + "/user/" + user + "/" + examplesRoot + "/apps";
        //definitionDir = OozieClientIT.class.getClassLoader().getResource("definitions").getPath();

    }

    public String run(Properties props) throws Exception {
        Properties defaultProps = getDefaultProperties();
        for (String key: props.stringPropertyNames()) {
            defaultProps.put(key, props.getProperty(key));
        }

        if (defaultProps.get(OozieClient.APP_PATH) != null) {
            LOG.debug("[subimt app] " + defaultProps.getProperty(OozieClient.APP_PATH));
        } else if (defaultProps.get(OozieClient.COORDINATOR_APP_PATH) != null) {
            LOG.debug("[subimt coord] " + defaultProps.getProperty(OozieClient.COORDINATOR_APP_PATH));
        } else {
            LOG.debug("[subimt ] nothing");
        }
        LOG.debug("--- submit job properites start ---");
        for (Object key : defaultProps.keySet()) {
            LOG.debug(key + " = " + defaultProps.getProperty((String) key));
        }
        LOG.debug("--- submit job properites end ---");

        String id = getClient().run(defaultProps);
        LOG.debug(">>>> run id >>> " + id);
        return id;
    }

    public WorkflowJob.Status monitorJob(String jobID) throws Exception {
        OozieClient client = getClient();
        WorkflowJob.Status status = client.getJobInfo(jobID).getStatus();

        while (!isTerminated(status)) {
            status = client.getJobInfo(jobID).getStatus();
            LOG.info("[{}] status = [{}]", jobID, status);
            System.out.println(" ---- status : " + jobID + " , " + status);
            Thread.sleep(POLLING);
        }
        LOG.info("[{}] finished, {}", jobID, status);
        return status;
    }

//    public WorkflowJob.Status monitorJob(Loader loader) throws Exception {
//        OozieClient client = getClient();
//        WorkflowJob.Status status = client.getJobInfo(jobID).getWfStatus();
//
//        while (!isTerminated(status)) {
//            status = client.getJobInfo(jobID).getWfStatus();
//            LOG.info("[{}] status = [{}]", jobID, status);
//            Thread.sleep(POLLING);
//        }
//        LOG.info("[{}] finished, {}", jobID, status);
//        return status;
//    }

    public static boolean isTerminated(WorkflowJob.Status status) {
        if (status == WorkflowJob.Status.SUCCEEDED || status == WorkflowJob.Status.KILLED
                || status == WorkflowJob.Status.FAILED) {
            return true;
        }
        return false;
    }


    public Properties getDefaultProperties() {
        Properties configs = new Properties();
        configs.put(OozieClient.USER_NAME, user);

        configs.put("nameNode", nameNode);
        configs.put("jobTracker", jobTracker);
        configs.put("hiveServer", hiveServer);

        return configs;
    }

    protected OozieClient getClient() {
        OozieClient client = new OozieClient(oozieUrl);
        client.setDebugMode(1);
        return client;
    }


}
