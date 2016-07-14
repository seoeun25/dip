package com.nexr.dip.jpa;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.nexr.dip.common.Utils;
import com.nexr.dip.loader.LoadResult;
import com.nexr.dip.server.DipServer;
import junit.framework.Assert;
import org.apache.oozie.client.WorkflowJob;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.List;

public class LoadResultQueryExecutorTest {

    private static JDBCService jdbcService;
    private static LoadResultQueryExecutor loadResultQueryExecutor;

    @BeforeClass
    public static void setupClass() {
        try {
            System.setProperty("persistenceUnit", "dip-test-hsql");
            Injector injector = Guice.createInjector(new DipServer());
            DipServer app = injector.getInstance(DipServer.class);
            jdbcService = injector.getInstance(JDBCService.class);

            Thread.sleep(1000);
            loadResultQueryExecutor = injector.getInstance(LoadResultQueryExecutor.class);

            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void tearDown() {
        jdbcService.shutdown();
        jdbcService = null;
    }

    @Test
    public void testInsertLoadResult() {

        try {
            LoadResult loadResult = new LoadResult("employee", null, new Timestamp(Utils.parseTime("2015-11-12",
                    "yyyy-MM-dd")));
            loadResult.setStatus(LoadResult.STATUS.SUCCEEDED);
            loadResult.setJobId("0000003-161007102247722-oozie-seoe-W");
            loadResult.setWfStatus(WorkflowJob.Status.SUCCEEDED);
            loadResult.setExternalId("job_1444180809999_0108");
            loadResult.setEtlExecutionPath("/2015-10-07-11-02-34");
            loadResult.setCountFile("/user/seoeun/dip/result/2015-10-07-11-02-34");
            loadResult.setResultCount(45678);
            loadResult.setErrorCount(0);
            loadResult.setResultFiles("/user/seoeun/dip/srcinfos/employee/daily/20151006/employee.0.7.381270.412742.1444057200000.avro,/user/seoeun/dip/srcinfos/employee/daily/20151006/employee.0.4.30013.35031.1444057200000.avro,/user/seoeun/dip/srcinfos/employee/daily/20151007/employee.0.9.10.791685.1444143600000.avro,");
            loadResult.setEndTime(new Timestamp(Utils.parseTime("2015-11-12 13:10:11", "yyyy-MM-dd HH:mm:ss")));
            loadResultQueryExecutor.insert(loadResult);

            loadResult = new LoadResult("hello", null, new Timestamp(System.currentTimeMillis()));
            loadResult.setStatus(LoadResult.STATUS.RETRY);
            loadResult.setJobId(null);
            loadResult.setWfStatus(null);
            loadResult.setExternalId(null);
            loadResult.setEtlExecutionPath(null);
            loadResult.setCountFile(null);
            loadResult.setResultCount(0);
            loadResult.setErrorCount(-1);
            loadResult.setError("E0504: App directory [hdfs://sembp:8020/user/ndap/dip/apps/hello] does not exist");
            loadResult.setResultFiles(null);
            loadResultQueryExecutor.insert(loadResult);

            // truncate nanotime on executionTime
            long time = System.currentTimeMillis();
            Timestamp timestamp = new Timestamp(time);
            timestamp.setNanos(0);

            LoadResult loadResult1 = new LoadResult("employee", null, new Timestamp(timestamp.getTime()));
            loadResultQueryExecutor.insert(loadResult1);
            Assert.assertEquals(timestamp.getTime(), loadResultQueryExecutor.get(LoadResultQueryExecutor.LoadResultQuery.GET_LOADRESULT,
                    new Object[]{"employee", new Timestamp(timestamp.getTime())}).getExecutionTime().getTime());

            // Update LoadResult
            LoadResult loadResult2 = loadResultQueryExecutor.get(LoadResultQueryExecutor.LoadResultQuery.GET_LOADRESULT,
                    new Object[]{"employee", timestamp});
            System.out.println(loadResult);

            loadResult2.setStatusStr(LoadResult.STATUS.RETRY.toString());
            loadResult2.setEndTime(new Timestamp(Utils.parseTime("2015-11-10 15:21:10", "yyyy-MM-dd HH:mm:ss")));

            loadResultQueryExecutor.executeUpdate(LoadResultQueryExecutor.LoadResultQuery.UPDATE_LOADRESULT, loadResult2);

            loadResult2 = loadResultQueryExecutor.get(LoadResultQueryExecutor.LoadResultQuery.GET_LOADRESULT,
                    new Object[]{"employee", timestamp});
            System.out.println(loadResult2);
            Assert.assertEquals(LoadResult.STATUS.RETRY.toString(), loadResult2.getStatusStr());

            // list by topic
            List<LoadResult> list = loadResultQueryExecutor.getList(LoadResultQueryExecutor.LoadResultQuery
                    .GET_LOADRESULT_BY_TOPIC, new Object[]{"employee", 100});
            Assert.assertEquals(2, list.size());

            // list by time from
            list = loadResultQueryExecutor.getList(LoadResultQueryExecutor.LoadResultQuery
                    .GET_LOADRESULT_FROM_TIME, new Object[]{new Timestamp(Utils.parseTime("2015-11-13 00:00:00,000")), 100});
            Assert.assertEquals(2, list.size());

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

}
