package com.nexr.dip.jpa;

import com.nexr.dip.DipLoaderException;
import com.nexr.dip.common.Utils;
import com.nexr.dip.loader.LoadResult;
import com.nexr.dip.loader.ScheduledService;
import com.nexr.dip.server.JDBCService;
import org.apache.oozie.client.WorkflowJob;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.List;

public class DipPropertyQueryExecutorTest {

    private static JDBCService jdbcService;
    private static DipPropertyQueryExecutor dipPropertyQueryExecutor;
    private static LoadResultQueryExecutor loadResultQueryExecutor;

    @BeforeClass
    public static void setupClass() {
        try {
            jdbcService = JDBCService.getInstance();
            jdbcService.start();

            dipPropertyQueryExecutor = new DipPropertyQueryExecutor(jdbcService);
            loadResultQueryExecutor = new LoadResultQueryExecutor(jdbcService);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInsertDipProperty() {

        //dip.schemaregistry.url=http://localhost:18181/repo
        //dip.load.task.count=10

        try {
            DipProperty dipProperty = new DipProperty();
            dipProperty.setName("dip.schemaregistry.url");
            dipProperty.setValue("http://hello.host.com:18181/repo");
            dipPropertyQueryExecutor.insert(dipProperty);

            dipProperty = new DipProperty();
            dipProperty.setName("dip.load.task.count");
            dipProperty.setValue("15");
            dipPropertyQueryExecutor.insert(dipProperty);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testGetListDipProperty() {
        try {
            List<DipProperty> list = dipPropertyQueryExecutor.getList(DipPropertyQueryExecutor.DipPropertyQuery
                    .GET_DIPPROPERTY_ALL, new Object[]{});
            for (DipProperty dipProperty : list) {
                System.out.println(dipProperty.getName() + "=" + dipProperty.getValue());
            }
        } catch (DipLoaderException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetDipProperty() {
        try {
            DipProperty dipProperty = dipPropertyQueryExecutor.get(DipPropertyQueryExecutor.DipPropertyQuery
                    .GET_DIPPROPERTY_BY_NAME, new Object[]{"dip.load.task.count"});
            System.out.println(dipProperty.getName() + "=" + dipProperty.getValue());
        } catch (DipLoaderException e) {
            e.printStackTrace();
        }
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
            loadResult.setCount(45678);
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
            loadResult.setCount(0);
            loadResult.setErrorCount(-1);
            loadResult.setError("E0504: App directory [hdfs://sembp:8020/user/ndap/dip/apps/hello] does not exist");
            loadResult.setResultFiles(null);
            loadResultQueryExecutor.insert(loadResult);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testInsertAndGetLoadResult() {

        try {
            long time = System.currentTimeMillis();
            System.out.println("time : " + time);
            Timestamp timestamp = new Timestamp(time);
            System.out.println("timestamp : " + timestamp.getTime());
            System.out.println("timestamp nano : " + timestamp.getNanos());
            timestamp.setNanos(0);
            System.out.println("timestamp 2 : " + timestamp.getTime());
            System.out.println("timestamp nano2 : " + timestamp.getNanos());


            LoadResult loadResult = new LoadResult("employee", null, new Timestamp(timestamp.getTime()));
            loadResultQueryExecutor.insert(loadResult);

            System.out.println(time + " ==== " + Utils.getDateString(time));

            Thread.sleep(3000);

            LoadResult loadResult1 = loadResultQueryExecutor.get(LoadResultQueryExecutor.LoadResultQuery.GET_LOADRESULT,
                    new Object[]{loadResult.getName(), new Timestamp(timestamp.getTime())});
            System.out.println("---- loadResult 1 : " + loadResult1);

            LoadResult loadResult2 = loadResultQueryExecutor.get(LoadResultQueryExecutor.LoadResultQuery.GET_LOADRESULT,
                    new Object[]{loadResult.getName(), loadResult.getExecutionTime()});

            System.out.println("---- loadResult 2 : " + loadResult1);


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testGetLoadResult() throws Exception{
        try {
            String name = "employee";
            String timeStr = "2015-10-08 15:21:11";
            long time = Utils.parseTime(timeStr, "yyyy-MM-dd HH:mm:ss");
            System.out.println("time : " + time );  //1444285270627
            Timestamp executionTime = new Timestamp(time);
            LoadResult loadResult = loadResultQueryExecutor.get(LoadResultQueryExecutor.LoadResultQuery.GET_LOADRESULT,
                    new Object[]{name, executionTime});
            System.out.println(loadResult);

            loadResult.setStatusStr(LoadResult.STATUS.RETRY.toString());
            loadResult.setEndTime(new Timestamp(Utils.parseTime("2015-11-10 15:21:10", "yyyy-MM-dd HH:mm:ss")));

            loadResultQueryExecutor.executeUpdate(LoadResultQueryExecutor.LoadResultQuery.UPDATE_LOADRESULT, loadResult);

            loadResult = loadResultQueryExecutor.get(LoadResultQueryExecutor.LoadResultQuery.GET_LOADRESULT,
                    new Object[]{name, executionTime});
            System.out.println(loadResult);
        } catch (DipLoaderException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetListLoadResultByTopic() {
        try {
            int limit = 100;
            List<LoadResult> list = loadResultQueryExecutor.getList(LoadResultQueryExecutor.LoadResultQuery
                    .GET_LOADRESULT_BY_TOPIC, new Object[]{"employee", limit});
            for (LoadResult loadResult : list) {
                System.out.println(loadResult.toString());
            }

            limit = 100;
            list = loadResultQueryExecutor.getList(LoadResultQueryExecutor.LoadResultQuery
                    .GET_LOADRESULT_BY_TOPIC, new Object[]{"hello", limit});
            for (LoadResult loadResult : list) {
                System.out.println(loadResult.toString());
            }
        } catch (DipLoaderException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetListLoadResultFromTime() throws Exception{
        try {
            int limit = 10;
            List<LoadResult> list = loadResultQueryExecutor.getList(LoadResultQueryExecutor.LoadResultQuery
                    .GET_LOADRESULT_FROM_TIME, new Object[]{new Timestamp(Utils.parseTime("2015-10-07 " +
                    "14:00:00,000")), limit});
            for (LoadResult loadResult : list) {
                System.out.println(loadResult.toString());
            }

        } catch (DipLoaderException e) {
            e.printStackTrace();
        }
    }


}
