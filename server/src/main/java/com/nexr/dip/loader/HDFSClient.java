package com.nexr.dip.loader;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.nexr.dip.Context;
import com.nexr.dip.server.DipContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

/**
 * Client to access HDFS.
 */
public class HDFSClient {

    private static Logger LOG = LoggerFactory.getLogger(HDFSClient.class);

    private static HDFSClient instance;

    private Configuration configuration;
    private String user;

    private final Context context;

    @Inject
    public HDFSClient(Context context) {
        this.context = context;
        try {
            this.configuration = loadHadoopConf();
        } catch (IOException e) {
            LOG.warn("Failed to load Hadoop config files", e);
            this.configuration = new Configuration();
        }
        LOG.debug("---- configuration : fs.defaultFS : " + configuration.get("fs.defaultFS"));
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

        user = System.getProperty("user.name");
    }

    public FileSystem createFileSystem(String user, final URI uri) throws Exception{
        return createFileSystem(user, uri, configuration);
    }

    public FileSystem createFileSystem(String user, final URI uri, final Configuration conf)
            throws Exception {
        String nameNode = uri.getAuthority();
        LOG.debug("nameNdoe : " + nameNode);
        LOG.debug("user : " + user);
        LOG.debug("uri : " + uri);

        UserGroupInformation ugi = getProxyUser(user);
        return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
            public FileSystem run() throws Exception {
                return FileSystem.get(uri, conf);
            }
        });
    }

    @VisibleForTesting
    public boolean delete(Path path) throws Exception {
        FileSystem fs = createFileSystem(user, path.toUri());
        boolean result =  fs.delete(path, true);
        LOG.info("deleted {}, [{}]", result, path.toString());
        System.out.println(result + " : " + path.toString());
        return result;
    }

    private UserGroupInformation getProxyUser(String user) throws IOException {
        return UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
    }

    public Configuration loadHadoopConf() throws IOException {
        Configuration hadoopConf = new Configuration();
        if (context.getConfig(DipContext.DIP_HADOOP_CONF) != null) {
            File dir = new File(context.getConfig(DipContext.DIP_HADOOP_CONF));
            String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml", "hadoop-site.xml"};
            for (String file : HADOOP_CONF_FILES) {
                File f = new File(dir, file);
                if (f.exists()) {
                    Configuration conf = new Configuration();
                    conf.addResource(f.toURL());
                    hadoopConf.addResource(conf);
                }
            }
        }
        LOG.debug("---- hadoopConf : " + hadoopConf.get("fs.defaultFS"));
        LOG.debug("---- hadoopConf : " + hadoopConf.get("yarn.resourcemanager.resource-tracker.address"));

        return hadoopConf;
    }

}
