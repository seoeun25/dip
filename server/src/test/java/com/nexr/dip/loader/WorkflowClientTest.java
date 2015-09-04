package com.nexr.dip.loader;

import com.nexr.dip.server.DipContext;
import org.junit.BeforeClass;
import org.junit.Test;

public class WorkflowClientTest {

    private static WorkflowClient client;

    @BeforeClass
    public static void beforeClass() {
        DipContext dipContext = DipContext.getContext();
        dipContext.setConfig(DipContext.DIP_OOZIE, "http://sembp:11000/oozie");
        dipContext.setConfig(DipContext.DIP_NAMENODE, "hdfs://sembp:8020");
        dipContext.setConfig(DipContext.DIP_JOBTRACKER, "sembp:8032");

        client = new WorkflowClient(dipContext);
    }

    @Test
    public void testLoaderCall() {

        String name = "employee";
        String appPath = "hdfs://sembp:8020/user/ndap/dip/apps/" + name;
        Loader loader = new Loader(name, appPath, Loader.SrcType.avro, System.currentTimeMillis(), 1000);

        try {
            LoadResult result = loader.call();
            Thread.sleep(40000);
            System.out.println("result :: \n" + result.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
