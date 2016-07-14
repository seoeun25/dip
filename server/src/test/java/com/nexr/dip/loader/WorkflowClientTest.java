package com.nexr.dip.loader;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.nexr.dip.Context;
import com.nexr.dip.server.DipContext;
import com.nexr.dip.server.DipServer;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration Test. Run with other echo systems: oozie, hadoop, hive, kafka.
 */
public class WorkflowClientTest {

    private static WorkflowClient client;

    @BeforeClass
    public static void beforeClass() {

        System.setProperty("persistenceUnit", "dip-test-hsql");
        Injector injector = Guice.createInjector(new DipServer());

        Context dipContext = injector.getInstance(Context.class);
        dipContext.setConfig(DipContext.DIP_OOZIE, "http://sembp:11000/oozie");
        dipContext.setConfig(DipContext.DIP_NAMENODE, "hdfs://sembp:8020");
        dipContext.setConfig(DipContext.DIP_JOBTRACKER, "sembp:8032");

        client = new WorkflowClient(dipContext);
    }

    @Test
    public void testLoaderCall() {

        String name = "employee";
        String appPath = "hdfs://sembp:8020/user/ndap/dip/apps/" + name;
        Loader loader = new Loader(new TopicManager.TopicDesc(name, appPath, Loader.SrcType.avro), System.currentTimeMillis(),
                1000);

        try {
            LoadResult result = loader.call();
            Thread.sleep(40000);
            System.out.println("result :: \n" + result.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
