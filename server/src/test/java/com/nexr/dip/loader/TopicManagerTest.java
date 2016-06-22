package com.nexr.dip.loader;

import com.nexr.dip.server.DipContext;
import junit.framework.Assert;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Integration Test. Run with other echo systems: oozie, hadoop, hive, kafka.
 */
public class TopicManagerTest {

    private static ScheduledService scheduledService;
    private static TopicManager topicManager;
    private static Loader loader;

    @BeforeClass
    public static void setupClass() {
        DipContext.getContext();
        scheduledService = ScheduledService.getInstance();
        topicManager = new TopicManager(scheduledService, "employee", "", Loader.SrcType.avro, System.currentTimeMillis() + (1000 *
                2));

        String name = "employee";
        String appPath = "hdfs://sembp:8020/user/ndap/dip/apps/" + name;
        loader = new Loader(name, appPath, Loader.SrcType.avro, System.currentTimeMillis(), 1000 * 60);

    }

    @Test
    public void testStart() {

        String name = "employee";
        String appPath = "hdfs://sembp:8020/user/ndap/dip/apps/" + name;
        Loader loader = new Loader(name, appPath, Loader.SrcType.avro, System.currentTimeMillis(), 1000 * 2);

        try {
            topicManager.executeLoader(loader, 1000 * 2);
            //LoadResult result = loader.call();
            Thread.sleep(40000);
            //System.out.println("result :: \n" + result.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testHadoopConf() {
        try {
            Configuration configuration = HDFSClient.getInstance().loadHadoopConf();
            Iterator<Map.Entry<String, String>> iterator = configuration.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();
                System.out.println(entry.getKey() + " = " + entry.getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetCamusApplicationId() {
        String log = "[YarnClientImpl] - Submitted application application_1442802049889_0002";
        String appId = "application_1442802049889_0002";
        String applicationId = loader.getCamusApplication(log);
        Assert.assertEquals(appId, applicationId);
    }

    @Test
    public void testGetMovedFile() {
        String log = "2015-09-21 14:41:26,805 INFO [main] com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat: Moved file " +
                "from: hdfs://sembp:8020/user/seoeun/camus/employee/exec/2015-09-19-04-00-06/_temporary/1/_temporary/attempt_1442587971144_0027_m_000002_0/data.employee.0.7.1442588400000-m-00002.avro to: /user/seoeun/dip/srcinfos/employee/daily/20150919/employee.0.7.943.1806.1442588400000.avro\n";
        String to = "/user/seoeun/dip/srcinfos/employee/daily/20150919/employee.0.7.943.1806.1442588400000.avro";
        String patternString1 = "(Moved file from: )(.*)( to: )(.*)";
        Pattern pattern = Pattern.compile(patternString1);
        Matcher matcher = pattern.matcher(log);
        if (matcher.find()) {
            String fileName = matcher.group(4);
            System.out.println("fileName : " + fileName);
//            System.out.println("group(1) : " + matcher.group(1));
//            System.out.println("group(2) : " + matcher.group(2));
//            System.out.println("group(3) : " + matcher.group(3));
//            System.out.println("group(4) : " + matcher.group(4));
        }
        String file = loader.extractMovedFiles(log);
        Assert.assertEquals(to, file);
    }

    @Test
    public void testPartition() {
        String fileName = "/user/ndap/dip/srcinfos/tb_dhcp/daily/20150923/tb_dhcp.0.0.30333.307.1444," +
                "/user/ndap/dip/srcinfos/tb_dhcp/daily/20150923/tb_dhcp.1.1.30333.307.1444,";
        loader.addPartitionToHive(fileName);

        fileName = "/user/ndap/dip/srcinfos/tb_dhcp/daily/20150923/tb_dhcp.0.0.30333.307.1444," +
                "/user/ndap/dip/srcinfos/tb_dhcp/daily/20150924/tb_dhcp.1.1.30333.307.1444,";
        loader.addPartitionToHive(fileName);
    }

    @Test
    public void testExecutionPath() {
        String log = "New execution temp location: /user/ndap/camus/ont_info/exec/2015-10-03-10-57-15";
        String path = "/user/ndap/camus/ont_info/exec/2015-10-03-10-57-15";
        String file = loader.extractExecutionPath(log);
        Assert.assertEquals(path, file);
    }

    @Test
    public void testAddPartitonToHive() {


        String[] cmdarray = {"beeline", "-u", "jdbc:hive2://localhost:10000", "-n", "ndap", "-p", "ndap", "-e",
                "\"ALTER TABLE aihsrc.employee ADD IF NOT EXISTS PARTITION (ins_date='20151002') location " +
                        "'/user/seoeun/dip/srcinfos/employee/daily/20151002/'\""};
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(cmdarray);
            List<String> lines = IOUtils.readLines(process.getInputStream());
            StringBuilder builder = new StringBuilder();
            for (String line : lines) {
                builder.append(line + "\n");
            }
            StringBuilder command = new StringBuilder();
            for (String cmd : cmdarray) {
                command.append(cmd + " ");
            }
            System.out.println(command.toString() + "\n  >>>  \n" + builder.toString());

        } catch (Exception e) {
            System.out.println("Failed to create partition : " + e.getMessage());
        } finally {
            if (process != null && process.getInputStream() != null) {
                try {
                    process.getInputStream().close();
                } catch (Exception e) {

                }
            }
        }
    }

    @Test
    public void testCleanup() {
        String name = "employee";
        String appPath = "hdfs://sembp:8020/user/ndap/dip/apps/" + name;
        Loader loader = new Loader(name, appPath, Loader.SrcType.avro, System.currentTimeMillis(), 1000 * 2);

        try {
            loader.getLoadResult().setError("Some topics skipped due to failure in getting latest offset from Kafka leaders");


            String files = "/user/seoeun/dip/srcinfos/employee/daily/20151003/employee.0.9.260.41607.1443711600000.avro," +
                    "/user/seoeun/dip/srcinfos/employee/daily/20151003/employee.0.9.385.40585.1443711600000.avro";
            loader.getLoadResult().setResultFiles(files);
            String etlPath = "2015-10-02-16-05-37";
            loader.getLoadResult().setEtlExecutionPath(etlPath);

            loader.cleanupResult();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDeleteOnHDFS() {
        String path = DipContext.getContext().getConfig(DipContext.DIP_NAMENODE) +
                "/user/seoeun/dip/srcinfos/employee/daily/20151003/employee.0.9.260" + ".41607" + ".1443711600000.avro";
        try {
            boolean result = HDFSClient.getInstance().delete(new Path(path));
            System.out.println("result : " + result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
